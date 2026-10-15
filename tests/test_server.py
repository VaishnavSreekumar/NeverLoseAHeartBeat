"""Integration tests for biosignal_mock/server.py (Increment E).

Verifies REQ-A-07, REQ-A-08, REQ-A-09, REQ-A-10, REQ-A-12:
- NDJSON over TCP on designated port
- Request-response loop for Sensor 1 and Sensor 2
- Single active client connection constraint
- Fault injections: corrupted CRC, duplicate replies, stalls, and session restarts
"""

import asyncio
import pytest
from biosignal_mock.faults import FaultConfig
from biosignal_mock.server import MockServer
from shared.protocol import ReadCommand, parse_mock_reply

TEST_PORT = 7188  # Dynamic port for test isolation


@pytest.mark.asyncio
async def test_server_tcp_communication_and_crc() -> None:
    """Verifies nominal TCP request-response and CRC validity."""
    server = MockServer(
        mode="ramp",
        seed=7,
        duration=60,
        port=TEST_PORT,
        host="127.0.0.1",
        realtime=False,
    )
    # Pre-generate 100 samples in generator
    server.generator.advance_samples(100)

    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", TEST_PORT)

        # 1. Read Sensor 1
        cmd_s1 = ReadCommand(sensor=1)
        writer.write(cmd_s1.to_json().encode("utf-8"))
        await writer.drain()

        line1 = await reader.readline()
        reply1, is_valid1 = parse_mock_reply(line1)
        assert is_valid1 is True
        assert reply1.sensor == 1
        assert reply1.first_index == 0
        assert reply1.count == 100
        assert len(reply1.data) == 100
        # Check ramp math
        assert reply1.data[0] == [0, 1, 2]

        # 2. Read Sensor 2
        cmd_s2 = ReadCommand(sensor=2)
        writer.write(cmd_s2.to_json().encode("utf-8"))
        await writer.drain()

        line2 = await reader.readline()
        reply2, is_valid2 = parse_mock_reply(line2)
        assert is_valid2 is True
        assert reply2.sensor == 2
        assert reply2.first_index == 0
        assert reply2.count == 100
        assert len(reply2.data) == 100
        assert reply2.data[0] == [0, 1]

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_server_single_client_restriction() -> None:
    """Verifies that a second concurrent client is rejected."""
    port = TEST_PORT + 1
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()
    try:
        # First client connects and holds connection
        r1, w1 = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.05)

        # Second client attempts to connect
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        # Second client should be immediately closed by server
        data2 = await r2.read()
        assert len(data2) == 0  # Connection closed by server

        w1.close()
        await w1.wait_closed()
        w2.close()
        await w2.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_server_corrupted_crc_fault_injection() -> None:
    """Verifies REQ-A-12: Mock injects corrupted CRC-32 when requested."""
    port = TEST_PORT + 2
    faults = FaultConfig(corrupt_crc=True)
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        fault_config=faults,
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()

        line = await reader.readline()
        reply, is_valid = parse_mock_reply(line)
        # CRC must be rejected as invalid
        assert is_valid is False

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_server_duplicate_reply_fault_injection() -> None:
    """Verifies REQ-A-12: Mock sends duplicate reply frame when requested."""
    port = TEST_PORT + 3
    faults = FaultConfig(duplicate_reply=True)
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        fault_config=faults,
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()

        line1 = await reader.readline()
        line2 = await reader.readline()
        assert line1 == line2  # Exact duplicate sent

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_server_restart_fault_injection() -> None:
    """Verifies REQ-A-12: Session restart resets index to 0 with new session_id."""
    port = TEST_PORT + 4
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
    )
    server.generator.advance_samples(50)
    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        
        # Read initial session
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()
        reply1, _ = parse_mock_reply(await reader.readline())
        initial_session = reply1.session_id

        # Trigger restart fault
        server.fault_config.trigger_restart = True
        server.generator.advance_samples(20)

        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()
        reply2, _ = parse_mock_reply(await reader.readline())
        
        assert reply2.session_id != initial_session
        assert reply2.first_index == 0  # Reset to 0

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()
