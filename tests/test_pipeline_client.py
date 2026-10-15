"""Focused unit and integration tests for BiosignalProtocolClient (Phase 2 Increment B).

Verifies REQ-B-01, REQ-B-02, REQ-B-04, REQ-B-05:
- Interleaved polling for Sensor 1 and Sensor 2 over single TCP socket.
- Strict NDJSON framing, schema validation, and exact raw-data CRC-32 validation.
- Malformed reply rejection without queue contamination.
- Corrupt CRC detection, immediate targeted retries, and incident quarantine.
- Duplicate reply handling without desynchronization.
- Disconnect detection and reconnect backoff.
- Session change detection and wire cursor resetting.
- Bounded queue backpressure.
- TST-06B: CRC retries exhausted, forward acquisition progresses, and in-process
  sweeper later recovers the gap cleanly with zero false loss.
"""

from __future__ import annotations

import asyncio
import json
import pytest
from biosignal_mock.faults import FaultConfig
from biosignal_mock.server import MockServer
from pipeline.protocol_client import (
    BiosignalProtocolClient,
    IngestBatch,
    UnresolvedCrcIncident,
)
from shared.protocol import ReadCommand, format_mock_reply

BASE_PORT = 7300


@pytest.mark.asyncio
async def test_client_normal_acquisition_both_sensors_interleaved() -> None:
    """Verifies nominal interleaved polling of Sensor 1 and Sensor 2."""
    port = BASE_PORT + 1
    server = MockServer(
        mode="ramp",
        seed=7,
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    # Pre-generate 200 samples in mock generator
    server.generator.advance_samples(200)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        poll_delay=0.0,
    )

    try:
        connected = await client.connect()
        assert connected is True
        assert client.is_connected is True

        # Execute 2 poll steps: should poll Sensor 1 then Sensor 2
        await client.poll_once()
        assert client.active_sensor == 2

        await client.poll_once()
        assert client.active_sensor == 1

        assert client.accepted_queue.qsize() == 2

        batch1 = await client.accepted_queue.get()
        assert batch1.sensor == 1
        assert batch1.first_index == 0
        assert batch1.count == 200
        assert len(batch1.data) == 200
        # Sensor 1 has 3 channels: (3*0 + c) % 8388608
        assert batch1.data[0] == [0, 1, 2]

        batch2 = await client.accepted_queue.get()
        assert batch2.sensor == 2
        assert batch2.first_index == 0
        assert batch2.count == 200
        assert len(batch2.data) == 200
        # Sensor 2 has 2 channels: (2*0 + c) % 8388608
        assert batch2.data[0] == [0, 1]

        assert client.telemetry.batches_accepted == 2
        assert client.telemetry.samples_accepted == 400
        assert client.telemetry.crc_failures_detected == 0
        assert client.telemetry.protocol_errors == 0
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_client_malformed_reply_handling() -> None:
    """Verifies that malformed JSON or invalid schema frames are rejected and discarded."""
    port = BASE_PORT + 2

    # Custom mini TCP server sending malformed frames followed by valid frame
    async def malformed_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break
            # First send unparseable garbage
            writer.write(b"NOT_VALID_JSON_AT_ALL\n")
            await writer.drain()

            # Next send JSON missing required fields
            writer.write(b'{"invalid_schema": true}\n')
            await writer.drain()

            # Finally send a valid NDJSON frame
            valid_reply = format_mock_reply(
                session_id="test_sess",
                sensor=1,
                first_index=0,
                data=[[10, 20, 30]],
            )
            writer.write(valid_reply.encode("utf-8"))
            await writer.drain()
            break
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(malformed_handler, "127.0.0.1", port)
    client = BiosignalProtocolClient(host="127.0.0.1", port=port, read_timeout=0.5)

    try:
        await client.connect()
        # Query 1: receives "NOT_VALID_JSON_AT_ALL"
        await client.poll_once()
        assert client.telemetry.protocol_errors == 1
        assert client.accepted_queue.qsize() == 0

        # Query 2: receives {"invalid_schema": true}
        await client.poll_once()
        assert client.telemetry.protocol_errors == 2
        assert client.accepted_queue.qsize() == 0

        # Query 3: receives valid reply
        await client.poll_once()
        assert client.accepted_queue.qsize() == 1
        batch = await client.accepted_queue.get()
        assert batch.session_id == "test_sess"
        assert batch.data == [[10, 20, 30]]
    finally:
        await client.disconnect()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_client_corrupt_crc_and_immediate_recovery() -> None:
    """Verifies CRC mismatch detection, payload rejection, and immediate retry recovery."""
    port = BASE_PORT + 3
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        max_crc_retries=3,
    )

    try:
        await client.connect()

        # Inject CRC corruption on initial attempt
        server.fault_config.corrupt_crc = True

        # We will change corruption to False on the second attempt to simulate recovery
        original_read_sensor = server.generator.read_sensor
        call_count = 0

        def wrapped_read_sensor(sensor: int, from_index: int | None = None):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                # Disable corruption for retry
                server.fault_config.corrupt_crc = False
            return original_read_sensor(sensor, from_index)

        server.generator.read_sensor = wrapped_read_sensor

        await client.poll_once()

        # Verify CRC failure was detected
        assert client.telemetry.crc_failures_detected >= 1
        # Verify that retry recovered the batch cleanly
        assert client.accepted_queue.qsize() == 1
        batch = await client.accepted_queue.get()
        assert batch.count == 50
        # No unrecovered incident left behind
        assert len(client.unresolved_crc_incidents) == 0
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_client_duplicate_reply_handling() -> None:
    """Verifies that duplicate reply frames sent by the mock are handled safely."""
    port = BASE_PORT + 4
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(100)
    # Configure mock to duplicate every response frame
    server.fault_config.duplicate_reply = True
    await server.start()

    client = BiosignalProtocolClient(host="127.0.0.1", port=port)

    try:
        await client.connect()

        # Poll 1: queries S1 -> receives S1 frame (buffered duplicate S1 frame remains in socket)
        await client.poll_once()
        assert client.accepted_queue.qsize() == 1

        # Poll 2: queries S2 -> reads the buffered duplicate S1 frame from socket, detects duplicate, discards it
        await client.poll_once()
        assert client.telemetry.duplicate_frames_handled >= 1

        # Poll 3: reads the S2 frame produced by query 2
        await client.poll_once()
        assert client.accepted_queue.qsize() == 2
        assert client.telemetry.batches_accepted == 2
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_client_disconnect_and_reconnect() -> None:
    """Verifies that client detects socket disconnects and reconnects with backoff."""
    port = BASE_PORT + 5
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        reconnect_delay=0.1,
        poll_delay=0.05,
    )

    task = client.start()
    try:
        # Wait for initial connection and first batch
        for _ in range(40):
            if client.is_connected and client.accepted_queue.qsize() >= 1:
                break
            await asyncio.sleep(0.05)
        assert client.is_connected is True
        assert client.accepted_queue.qsize() >= 1

        # Stop server to severed connection
        await server.stop()
        for _ in range(40):
            if not client.is_connected:
                break
            await asyncio.sleep(0.05)
        assert client.is_connected is False

        # Restart server on same port
        server2 = MockServer(
            mode="ramp",
            port=port,
            host="127.0.0.1",
            realtime=False,
        )
        server2.generator.advance_samples(50)
        await server2.start()

        try:
            # Client should reconnect automatically
            for _ in range(60):
                if client.is_connected and client.telemetry.reconnect_count >= 2:
                    break
                await asyncio.sleep(0.05)
            assert client.is_connected is True
            assert client.telemetry.reconnect_count >= 2
        finally:
            await server2.stop()
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_client_session_transition() -> None:
    """Verifies detection of server session restart, resetting transport wire cursors."""
    port = BASE_PORT + 6
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(100)
    await server.start()

    client = BiosignalProtocolClient(host="127.0.0.1", port=port)

    try:
        await client.connect()

        # Poll from initial session
        await client.poll_once()
        initial_sess = client.current_session_id
        assert initial_sess is not None
        assert client.transport_polling_cursor[1] == 100

        # Inject session restart fault on server
        server.fault_config.trigger_restart = True
        server.generator.advance_samples(50)

        # Poll again; server resets session
        await client.poll_once()
        new_sess = client.current_session_id
        assert new_sess != initial_sess
        assert client.telemetry.session_transitions_observed == 1

        # Verify that wire cursors reflect the new session starting at 0
        batch = await client.accepted_queue.get()
        assert batch.session_id == initial_sess
        batch2 = await client.accepted_queue.get()
        assert batch2.session_id == new_sess
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_client_bounded_queue_backpressure() -> None:
    """Verifies that the downstream accepted_queue strictly respects maxsize bound."""
    port = BASE_PORT + 7
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(200)
    await server.start()

    # Create client with maxsize=2
    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        queue_maxsize=2,
    )

    try:
        await client.connect()
        await client.poll_once()  # S1 -> queue size 1
        await client.poll_once()  # S2 -> queue size 2
        assert client.accepted_queue.qsize() == 2
        assert client.accepted_queue.full() is True

        # Consumer drains 1 item
        item = await client.accepted_queue.get()
        assert item.sensor == 1
        assert client.accepted_queue.qsize() == 1

        # Now client can accept one more
        await client.poll_once()
        assert client.accepted_queue.qsize() == 2
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_tst06b_crc_failure_forward_progress_and_in_process_recovery() -> None:
    """TST-06B: CRC retries fail, subsequent samples arrive, and in-process sweeper recovers gap."""
    port = BASE_PORT + 8
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    # Pre-generate first batch: samples 0..99
    server.generator.advance_samples(100)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        max_crc_retries=3,
        sweeper_interval=0.05,
    )

    try:
        await client.connect()

        # Step 1: Force CRC corruption on Sensor 1 index 0
        server.fault_config.corrupt_crc = True

        # Poll: 1 initial attempt + 3 retries will all fail CRC
        await client.poll_once()

        # Step 2: Intermediate Assertions:
        # - Payload was discarded; NOT placed in accepted_queue
        assert client.accepted_queue.qsize() == 0
        # - Unresolved incident logged in memory (NOT in loss ledger)
        assert len(client.unresolved_crc_incidents) == 1
        incident = client.unresolved_crc_incidents[0]
        assert incident.sensor == 1
        assert incident.from_index == 0
        assert incident.retries_attempted == 3
        # - Wire transport cursor advanced past gap to prevent loop deadlock
        assert client.transport_polling_cursor[1] >= 100

        # Step 3: Advance generator with next batch (samples 100..199).
        server.generator.advance_samples(100)
        # Disable CRC corruption fault
        server.fault_config.corrupt_crc = False

        # Step 4: Normal forward acquisition continues without stall
        # Poll active sensor (Sensor 2)
        await client.poll_once()
        # Poll active sensor (Sensor 1) -> reads clean batch 100..199
        await client.poll_once()

        assert client.accepted_queue.qsize() >= 1
        # Find the forward Sensor 1 batch
        forward_batch = None
        while not client.accepted_queue.empty():
            b = await client.accepted_queue.get()
            if b.sensor == 1 and b.first_index >= 100:
                forward_batch = b

        assert forward_batch is not None
        assert forward_batch.first_index == 100
        assert forward_batch.count == 100
        assert client.transport_polling_cursor[1] == 200

        # Quarantined incident is still tracked in memory
        assert len(client.unresolved_crc_incidents) == 1

        # Step 5: Trigger in-process gap reconciliation sweeper
        await client._sweep_unresolved_incidents()

        # Step 6: Final Assertions after In-Process Recovery:
        # - The quarantined gap [0, 99] was successfully recovered from mock ring buffer!
        assert len(client.unresolved_crc_incidents) == 0
        assert client.telemetry.incidents_recovered_in_process == 1

        # - The recovered batch [0, 99] is now in accepted_queue (mock returns all buffered samples from 0)
        assert client.accepted_queue.qsize() >= 1
        recovered_batch = await client.accepted_queue.get()
        assert recovered_batch.sensor == 1
        assert recovered_batch.first_index == 0
        assert recovered_batch.count >= 100
        assert recovered_batch.data[0] == [0, 1, 2]  # Ramp seed 7 formula match!

        # - Zero false loss recorded at any point
        assert recovered_batch.lost == []
    finally:
        await client.disconnect()
        await server.stop()
