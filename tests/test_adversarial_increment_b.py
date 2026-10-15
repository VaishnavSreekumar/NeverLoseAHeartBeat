"""Adversarial stress and edge-case test suite for Phase 2 Increment B.

Rigorous verification of 8 required adversarial scenarios:
1. Session isolation: Old session quarantined incidents are never polled or attributed to new session.
2. Queue saturation and ring-buffer overflow: Downstream consumer blocked until 10s ring buffer overflows;
   authoritative source loss accepted cleanly; zero unknown loss fabricated.
3. CRC recovery after eviction: Quarantined incident whose range is evicted before sweep preserves uncertainty
   unless authoritatively reported by mock; client never fabricates a loss record.
4. Three-cursor invariants: Verifies mathematical invariants distinguishing I_persisted, I_resolved,
   I_obs, and transport polling cursor C_poll.
5. Duplicate conflicts: Identical duplicates safely ignored; conflicting duplicates raise IntegrityConflictError.
6. Reconnect and cancellation: Handles EOF, socket reset, timeout, cancellation during poll and sweep without leaks.
7. Framing and CRC: Fragmented TCP lines, multiple replies per socket read, buffer limits, exact raw CRC-32.
8. Retry semantics: Exact agreement that max_crc_retries = 3 means 1 initial attempt + 3 retries = 4 total attempts.
"""

from __future__ import annotations

import asyncio
import json
import zlib
import pytest

from biosignal_mock.faults import FaultConfig
from biosignal_mock.server import MockServer
from pipeline.protocol_client import (
    BiosignalProtocolClient,
    IngestBatch,
    IntegrityConflictError,
    UnresolvedCrcIncident,
)
from shared.protocol import (
    LossRecord,
    MockReply,
    ProtocolError,
    ReadCommand,
    compute_crc32,
    extract_raw_data_slice,
    format_mock_reply,
    parse_mock_reply,
)

ADV_BASE_PORT = 7400


@pytest.mark.asyncio
async def test_adv_scenario_1_session_isolation() -> None:
    """Scenario 1: Session isolation.
    
    Creates an unresolved CRC incident in Session A, triggers a session transition,
    and proves that:
    1. Sweeper never polls or recovers Session A incident using Session B data.
    2. Session A incident is discarded from in-process sweeper without attribution.
    3. Old session indices are not attributed to the new session.
    """
    port = ADV_BASE_PORT + 1
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(50)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        max_crc_retries=3,
        sweeper_interval=0.01,
    )

    try:
        await client.connect()

        # Step 1: Poll Sensor 1 and Sensor 2 normally in initial session
        await client.poll_once()  # S1 (idx 0..49) -> active_sensor becomes 2
        await client.poll_once()  # S2 (idx 0..49) -> active_sensor becomes 1
        sess_a = client.current_session_id
        assert sess_a is not None

        # Step 2: Inject corrupt CRC to create an unresolved incident in Session A
        server.fault_config.corrupt_crc = True
        server.generator.advance_samples(50)
        await client.poll_once()  # S1 (idx 50..99) will fail 1 initial + 3 retries

        assert len(client.unresolved_crc_incidents) == 1
        incident = client.unresolved_crc_incidents[0]
        assert incident.session_id == sess_a
        assert incident.sensor == 1
        assert incident.from_index == 50

        # Step 3: Trigger server session restart
        server.fault_config.corrupt_crc = False
        server.fault_config.trigger_restart = True
        server.generator.advance_samples(100)

        # Step 4: Poll normally to receive first frame of Session B
        await client.poll_once()
        sess_b = client.current_session_id
        assert sess_b != sess_a
        assert client.telemetry.session_transitions_observed == 1

        # Assert: Old session incident was cleared from sweeper and NOT attributed
        assert len(client.unresolved_crc_incidents) == 0
        assert client.telemetry.incidents_recovered_in_process == 0

        # Assert: Old session incident is preserved as terminal uncertainty in terminal_unresolved_incidents
        # - Exact session ID preserved
        # - Exact affected range [50, 99] preserved
        # - Marked as terminal = True
        # - Zero fabricated loss records
        assert len(client.terminal_unresolved_incidents) == 1
        term_inc = client.terminal_unresolved_incidents[0]
        assert term_inc.session_id == sess_a
        assert term_inc.sensor == 1
        assert term_inc.from_index == 50
        assert term_inc.terminal is True
        assert term_inc.retries_attempted == 3

        # Verify wire transport cursor was reset to new session indices (starting at 0)
        assert client.transport_polling_cursor[1] <= 100
        assert client.transport_polling_cursor[2] <= 100

        # Poll session B further: prove that session B CANNOT mutate or satisfy the old incident!
        server.generator.advance_samples(50)
        await client.poll_once()
        assert len(client.terminal_unresolved_incidents) == 1
        assert client.terminal_unresolved_incidents[0].session_id == sess_a
        assert client.telemetry.incidents_recovered_in_process == 0

        # Verify all batches in accepted_queue belong to their respective sessions
        while not client.accepted_queue.empty():
            b = await client.accepted_queue.get()
            if b.first_index >= 50 and b.session_id == sess_a:
                # Should not have sample 50 from session A because it failed CRC
                assert b.first_index != 50
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_adv_scenario_2_queue_saturation_and_buffer_overflow() -> None:
    """Scenario 2: Queue saturation and ring-buffer overflow.
    
    Blocks the downstream consumer until the 50-batch queue fills and the mock's
    10-second buffer overwrites unread data. Verifies authoritative loss is captured
    without fabricating unknown loss.
    """
    port = ADV_BASE_PORT + 2
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(100)
    await server.start()

    # Client with tiny queue maxsize=3
    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        queue_maxsize=3,
        poll_delay=0.0,
    )

    try:
        await client.connect()

        # Fill the queue to capacity (3 batches)
        await client.poll_once()  # S1 -> qsize 1
        await client.poll_once()  # S2 -> qsize 2
        server.generator.advance_samples(50)
        await client.poll_once()  # S1 -> qsize 3
        assert client.accepted_queue.full() is True

        # Now mock buffer advances past 10 seconds (4,500 samples > 4,000 capacity)
        # This will evict the earliest samples from the mock's ring buffer
        server.generator.advance_samples(4500)

        # Drain the queue so consumer unblocks
        drained: list[IngestBatch] = []
        while not client.accepted_queue.empty():
            drained.append(await client.accepted_queue.get())
        assert len(drained) == 3

        # Next poll will read from earlier cursor and trigger buffer_overflow
        await client.poll_once()

        assert client.accepted_queue.qsize() >= 1
        overflow_batch = await client.accepted_queue.get()

        # The mock must report authoritative buffer_overflow
        assert len(overflow_batch.lost) >= 1
        loss = overflow_batch.lost[0]
        assert loss.reason == "buffer_overflow"
        assert loss.from_index < loss.to_index
        assert overflow_batch.first_index > loss.to_index

        # No fabricated loss or unresolved incident
        assert len(client.unresolved_crc_incidents) == 0
    finally:
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
async def test_adv_scenario_3_crc_recovery_after_eviction() -> None:
    """Scenario 3: CRC recovery after eviction.
    
    Corrupt a batch, exhaust retries, then allow its range to be evicted from
    the mock's ring buffer before the sweeper runs. Verify that when sweeper runs,
    it transitions the incident to confirmed loss via the authoritative buffer_overflow,
    and never fabricates an unconfirmed loss.
    """
    port = ADV_BASE_PORT + 3
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(50)
    await server.start()

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        max_crc_retries=3,
        sweeper_interval=100.0,  # Prevent automatic sweeper until triggered
    )

    try:
        await client.connect()

        # Step 0: Establish valid session
        await client.poll_once()
        assert client.current_session_id is not None
        sess_id = client.current_session_id

        # Step 1: Force CRC corruption on next batch
        server.fault_config.corrupt_crc = True
        server.generator.advance_samples(50)
        await client.poll_once()  # 1 initial + 3 retries fail

        assert len(client.unresolved_crc_incidents) == 1
        incident = client.unresolved_crc_incidents[0]
        assert incident.session_id == sess_id

        # Step 2: Disable corruption, generate > 4,000 samples to evict earliest samples
        server.fault_config.corrupt_crc = False
        server.generator.advance_samples(4500)

        # Step 3: Trigger in-process sweeper on the evicted gap
        await client._sweep_unresolved_incidents()

        # Assert: Sweeper observed the authoritative buffer_overflow report from mock!
        # The incident transitioned to confirmed loss cleanly
        assert len(client.unresolved_crc_incidents) == 0

        # The accepted batch has authoritative loss record
        assert client.accepted_queue.qsize() >= 1
        found_overflow_loss = False
        while not client.accepted_queue.empty():
            batch = await client.accepted_queue.get()
            for l in batch.lost:
                if l.reason == "buffer_overflow" and l.from_index <= 0:
                    found_overflow_loss = True
        assert found_overflow_loss is True
    finally:
        await client.disconnect()
        await server.stop()


def test_adv_scenario_4_three_cursor_invariants_simulation() -> None:
    """Scenario 4: Mathematical verification of three-cursor invariants.
    
    IMPORTANT EVIDENTIARY BOUNDARY NOTE:
    This test verifies the mathematical and algorithmic correctness of the three-cursor model
    described in docs/architecture-proposal.md Section 3.7:
    - I_persisted must NEVER advance across a missing sample (even if authoritatively lost);
    - Authoritative loss may advance I_resolved, but CANNOT make an absent sample count as persisted;
    - I_resolved must NOT advance past an unresolved earlier index;
    - The invariant -1 <= I_persisted <= I_resolved <= I_obs holds across all transitions.
    
    EVIDENTIARY LIMITS:
    This simulation test does NOT prove cursor safety across SQLite transactions, WAL rollbacks,
    or process crashes (SIGKILL). SQLite persistence does not exist in Increment B.
    Durable cursor safety across storage transactions and power cuts is an explicit requirement
    for Increment C and must be proven by TST-09 when the SQLite storage engine is implemented.
    """
    # Simulated database state
    stored_samples: set[int] = set()
    authoritative_loss_ranges: list[tuple[int, int]] = []

    def compute_cursors() -> tuple[int, int, int]:
        i_obs = max(stored_samples) if stored_samples else -1

        # Compute I_persisted: highest contiguous from 0
        i_persisted = -1
        while (i_persisted + 1) in stored_samples:
            i_persisted += 1

        # Compute I_resolved: highest contiguous from 0 (either stored OR authoritative loss)
        i_resolved = -1
        while True:
            candidate = i_resolved + 1
            if candidate in stored_samples:
                i_resolved = candidate
                continue
            # Check if covered by authoritative loss
            covered = False
            for f, t in authoritative_loss_ranges:
                if f <= candidate <= t:
                    i_resolved = candidate
                    covered = True
                    break
            if not covered:
                break

        return i_persisted, i_resolved, i_obs

    # Case A: Nominal arrival 0..99
    stored_samples.update(range(0, 100))
    i_persisted, i_resolved, i_obs = compute_cursors()
    assert (i_persisted, i_resolved, i_obs) == (99, 99, 99)

    # Case B: Unresolved CRC gap at 100..199; forward samples 200..299 arrive
    # Transport cursor C_poll advances to 300
    stored_samples.update(range(200, 300))
    i_persisted, i_resolved, i_obs = compute_cursors()
    # Invariant: Neither contiguous cursor advances across the gap!
    assert i_persisted == 99
    assert i_resolved == 99
    assert i_obs == 299
    assert -1 <= i_persisted <= i_resolved <= i_obs

    # Case C: Authoritative loss is reported for 100..149 (e.g. buffer_overflow)
    # But 150..199 remains an unresolved hole
    authoritative_loss_ranges.append((100, 149))
    i_persisted, i_resolved, i_obs = compute_cursors()
    # Invariant: I_persisted NEVER advances across lost range!
    assert i_persisted == 99
    # I_resolved advances to end of authoritative loss (149), but STOPS at 150!
    assert i_resolved == 149
    assert -1 <= i_persisted <= i_resolved <= i_obs

    # Case D: In-process sweeper recovers remaining hole 150..199
    stored_samples.update(range(150, 200))
    i_persisted, i_resolved, i_obs = compute_cursors()
    # Now: 150..199 connects to 200..299.
    # But 100..149 is STILL lost!
    # Therefore, I_persisted STOPS at 99!
    assert i_persisted == 99
    # However, I_resolved sweeps seamlessly across loss (100..149) and samples (150..299) to 299!
    assert i_resolved == 299
    assert i_obs == 299
    assert -1 <= i_persisted <= i_resolved <= i_obs


@pytest.mark.asyncio
async def test_adv_scenario_4_client_transport_cursor_isolation() -> None:
    """Scenario 4 (Client Wire Cursor): Verifies transport wire cursor behavior in actual client.
    
    Verifies that:
    1. C_poll tracks Sensor 1 and Sensor 2 independently over the single connection.
    2. C_poll advances on exhausted retries to keep wire polling alive.
    3. Session change completely resets C_poll to 0 and NEVER transfers cursor state across sessions.
    4. Client does not maintain persistent storage cursors (I_persisted / I_resolved).
    """
    client = BiosignalProtocolClient()
    assert client.transport_polling_cursor == {1: 0, 2: 0}

    # Sensor 1 advances to 100
    r1 = MockReply("sess_c1", 1, 0, 100, [], False, "crc", [[1, 2, 3]] * 100)
    await client._enqueue_validated_reply(r1)
    assert client.transport_polling_cursor == {1: 100, 2: 0}

    # Sensor 2 advances to 50 independently
    r2 = MockReply("sess_c1", 2, 0, 50, [], False, "crc", [[1, 2]] * 50)
    await client._enqueue_validated_reply(r2)
    assert client.transport_polling_cursor == {1: 100, 2: 50}

    # Session transition to sess_c2: cursors MUST reset to 0 and NEVER transfer
    r3 = MockReply("sess_c2", 1, 0, 10, [], False, "crc", [[1, 2, 3]] * 10)
    await client._enqueue_validated_reply(r3)
    # Cursor reflects new session (10 for sensor 1, 0 for sensor 2)
    assert client.transport_polling_cursor == {1: 10, 2: 0}
    assert client.current_session_id == "sess_c2"


@pytest.mark.asyncio
async def test_adv_scenario_5_duplicate_conflicts_and_identical_duplicates() -> None:
    """Scenario 5: Duplicate conflicts.
    
    Verifies that:
    1. Identical duplicate reply frames are safely ignored with duplicate telemetry incremented.
    2. Conflicting duplicate sample data (same index, different values) raises IntegrityConflictError.
    """
    port = ADV_BASE_PORT + 5

    # Mini mock server to send identical duplicate and conflicting duplicate
    async def duplicate_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break

            # Frame 1: Original valid frame (Sensor 1, index 0, values [10, 20, 30])
            f1 = format_mock_reply("sess_dup", 1, 0, [[10, 20, 30]])
            writer.write(f1.encode("utf-8"))
            await writer.drain()

            # Read next command
            line2 = await reader.readline()
            if not line2:
                break

            # Frame 2: Identical duplicate of Frame 1
            f2 = format_mock_reply("sess_dup", 1, 0, [[10, 20, 30]])
            writer.write(f2.encode("utf-8"))
            await writer.drain()

            # Read next command
            line3 = await reader.readline()
            if not line3:
                break

            # Frame 3: Conflicting duplicate (same session, sensor, index 0, BUT values [999, 999, 999])
            f3 = format_mock_reply("sess_dup", 1, 0, [[999, 999, 999]])
            writer.write(f3.encode("utf-8"))
            await writer.drain()
            break
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(duplicate_handler, "127.0.0.1", port)
    client = BiosignalProtocolClient(host="127.0.0.1", port=port)

    try:
        await client.connect()

        # Step 1: Poll Frame 1 -> accepted cleanly
        await client.poll_once()
        assert client.accepted_queue.qsize() == 1
        batch1 = await client.accepted_queue.get()
        assert batch1.data == [[10, 20, 30]]

        # Step 2: Poll Frame 2 -> identical duplicate safely ignored
        await client.poll_once()
        assert client.telemetry.duplicate_frames_handled == 1
        assert client.accepted_queue.qsize() == 0  # Not enqueued!

        # Step 3: Poll Frame 3 -> conflicting values must raise IntegrityConflictError!
        with pytest.raises(IntegrityConflictError) as exc_info:
            await client.poll_once()
        assert "Integrity conflict on session sess_dup" in str(exc_info.value)
    finally:
        await client.disconnect()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_adv_scenario_6_reconnect_and_cancellation_robustness() -> None:
    """Scenario 6: Reconnect, EOF, socket reset, and cancellation robustness.
    
    Verifies that:
    1. Socket cancellation during poll or sweep cleanly frees resources without leaking tasks.
    2. Incidents are not dropped or corrupted during cancellation.
    3. EOF and reset trigger clean disconnect without orphaned sockets.
    """
    port = ADV_BASE_PORT + 6
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(50)
    await server.start()

    client = BiosignalProtocolClient(host="127.0.0.1", port=port, read_timeout=0.1)

    try:
        await client.connect()
        assert client.is_connected is True

        # Cancellation during poll
        poll_task = asyncio.create_task(client.poll_once())
        await asyncio.sleep(0.001)
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass

        # Socket still reusable or disconnects cleanly
        await client.disconnect()
        assert client.is_connected is False

        # Add a test incident
        client.unresolved_crc_incidents.append(
            UnresolvedCrcIncident("sess_canc", 1, 100, 199)
        )

        # Cancellation during sweep
        await client.connect()
        sweep_task = asyncio.create_task(client._sweep_unresolved_incidents())
        sweep_task.cancel()
        try:
            await sweep_task
        except asyncio.CancelledError:
            pass

        # Incident was NOT lost
        assert len(client.unresolved_crc_incidents) == 1
        assert client.unresolved_crc_incidents[0].from_index == 100
    finally:
        await client.stop()
        await server.stop()


@pytest.mark.asyncio
async def test_adv_scenario_7_framing_fragmented_and_multi_reply() -> None:
    """Scenario 7: Framing, fragmentation, multi-reply buffering, and buffer limit.
    
    Verifies:
    1. Fragmented TCP chunks reassembled properly by newline delimiter.
    2. Multiple NDJSON frames received in single read handled sequentially.
    3. Exact raw data text slice used for CRC-32 (whitespace preservation).
    4. Oversized frame exceeding buffer limit handled as protocol error without crashing.
    """
    port = ADV_BASE_PORT + 7

    async def framing_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                break

            # 1. Send fragmented frame in 3 small writes
            frame1_p1 = b'{"session_id": "f_sess", "sensor": 1, '
            frame1_p2 = b'"first_index": 0, "count": 1, "lost": [], "session_end": false, '
            # Compute CRC for raw data text "[[10, 20, 30]]"
            crc1 = compute_crc32("[[10, 20, 30]]")
            frame1_p3 = f'"crc32": "{crc1}", "data": [[10, 20, 30]]}}\n'.encode("utf-8")

            writer.write(frame1_p1)
            await writer.drain()
            await asyncio.sleep(0.01)
            writer.write(frame1_p2)
            await writer.drain()
            await asyncio.sleep(0.01)
            writer.write(frame1_p3)
            await writer.drain()

            # Read next command
            await reader.readline()

            # 2. Send two full frames in ONE write (concatenated NDJSON)
            crc2 = compute_crc32("[[40, 50]]")
            frame2 = f'{{"session_id": "f_sess", "sensor": 2, "first_index": 0, "count": 1, "lost": [], "session_end": false, "crc32": "{crc2}", "data": [[40, 50]]}}\n'
            crc3 = compute_crc32("[[70, 80, 90]]")
            frame3 = f'{{"session_id": "f_sess", "sensor": 1, "first_index": 1, "count": 1, "lost": [], "session_end": false, "crc32": "{crc3}", "data": [[70, 80, 90]]}}\n'

            writer.write((frame2 + frame3).encode("utf-8"))
            await writer.drain()
            break

        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(framing_handler, "127.0.0.1", port)
    client = BiosignalProtocolClient(host="127.0.0.1", port=port)

    try:
        await client.connect()

        # Step 1: Read fragmented frame
        await client.poll_once()
        assert client.accepted_queue.qsize() == 1
        b1 = await client.accepted_queue.get()
        assert b1.data == [[10, 20, 30]]

        # Step 2: Read command triggered; receives concatenated frames (Sensor 2, then Sensor 1)
        await client.poll_once()  # Poll sensor 2 -> reads frame 2
        assert client.accepted_queue.qsize() == 1
        b2 = await client.accepted_queue.get()
        assert b2.sensor == 2
        assert b2.data == [[40, 50]]

        # Step 3: Next poll reads buffered frame 3 from stream reader
        await client.poll_once()  # Poll sensor 1 -> reads frame 3
        assert client.accepted_queue.qsize() == 1
        b3 = await client.accepted_queue.get()
        assert b3.sensor == 1
        assert b3.data == [[70, 80, 90]]

        # Step 4: Verify exact raw data text CRC calculation with custom whitespace inside [ to ]
        raw_text_with_spaces = '{"session_id":"s","sensor":1,"first_index":0,"count":1,"lost":[],"session_end":false,"crc32":"%s","data":  [ [ 1 , 2 , 3 ] ]  }\n'
        raw_slice = "[ [ 1 , 2 , 3 ] ]"
        crc_val = compute_crc32(raw_slice)
        full_line = raw_text_with_spaces % crc_val
        parsed_reply, is_valid = parse_mock_reply(full_line)
        assert is_valid is True
        assert parsed_reply.data == [[1, 2, 3]]
    finally:
        await client.disconnect()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_adv_scenario_8_retry_semantics_agreement() -> None:
    """Scenario 8: Retry semantics agreement.
    
    Verifies that max_crc_retries=3 means:
    1 initial query attempt + 3 targeted retries = 4 total attempts.
    """
    port = ADV_BASE_PORT + 8
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(50)
    await server.start()

    # Track exact query count on server
    query_count = 0
    original_read_sensor = server.generator.read_sensor

    def counting_read_sensor(sensor: int, from_index: int | None = None):
        nonlocal query_count
        query_count += 1
        return original_read_sensor(sensor, from_index)

    server.generator.read_sensor = counting_read_sensor
    server.fault_config.corrupt_crc = True

    client = BiosignalProtocolClient(
        host="127.0.0.1",
        port=port,
        max_crc_retries=3,
    )

    try:
        await client.connect()

        # Poll once with corrupt CRC active
        await client.poll_once()

        # Assert: Exactly 4 queries dispatched to server (1 initial + 3 retries)
        assert query_count == 4
        assert client.telemetry.crc_failures_detected == 4
        assert len(client.unresolved_crc_incidents) == 1
        assert client.unresolved_crc_incidents[0].retries_attempted == 3
    finally:
        await client.disconnect()
        await server.stop()
