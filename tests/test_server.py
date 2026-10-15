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
from biosignal_mock.server import MockServer, compute_deadline_delay
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


@pytest.mark.asyncio
async def test_server_selective_sensor_stall_fault_injection() -> None:
    """Verifies AUDIT-03 / REQ-A-12: selective sensor stall suppresses one sensor while other continues."""
    port = 7199
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

        # 1. Initial reads for both sensors
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()
        reply1, _ = parse_mock_reply(await reader.readline())
        assert reply1.first_index == 0
        assert reply1.count == 50
        assert len(reply1.lost) == 0

        writer.write(ReadCommand(sensor=2).to_json().encode("utf-8"))
        await writer.drain()
        reply2, _ = parse_mock_reply(await reader.readline())
        assert reply2.first_index == 0
        assert reply2.count == 50
        assert len(reply2.lost) == 0

        # 2. Trigger selective stall on Sensor 1 for 100 samples (indices 50 to 149)
        server.fault_config.sensor_stall = 1
        server.fault_config.sensor_stall_samples = 100

        # Trigger stall processing on next command
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()
        reply_stall_immediate, _ = parse_mock_reply(await reader.readline())
        assert len(reply_stall_immediate.lost) == 1
        assert reply_stall_immediate.lost[0].from_index == 50
        assert reply_stall_immediate.lost[0].to_index == 149
        assert reply_stall_immediate.lost[0].reason == "sensor_stall"

        # 3. Advance generator by 100 samples (while S1 is stalled, S2 generates normally)
        server.generator.advance_samples(100)

        # 4. Advance generator by 50 more samples (S1 resumes at 150, S2 continues at 150)
        server.generator.advance_samples(50)

        # 5. Sensor 2 must have all samples 50..199 (150 samples) with ZERO loss
        writer.write(ReadCommand(sensor=2).to_json().encode("utf-8"))
        await writer.drain()
        reply_s2_after, _ = parse_mock_reply(await reader.readline())
        assert reply_s2_after.first_index == 50
        assert reply_s2_after.count == 150
        assert len(reply_s2_after.lost) == 0

        # 6. Sensor 1 resumes from index 150 with 50 samples
        writer.write(ReadCommand(sensor=1).to_json().encode("utf-8"))
        await writer.drain()
        reply_s1_after, _ = parse_mock_reply(await reader.readline())
        assert reply_s1_after.first_index == 150
        assert reply_s1_after.count == 50
        assert len(reply_s1_after.lost) == 0

        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


def test_deterministic_deadline_scheduler_anti_drift() -> None:
    """Verifies AUDIT-06: absolute deadline scheduler compensates for sleep jitter and prevents drift."""
    batch_interval = 0.025  # 25 ms per batch
    start_time = 1000.0

    # Iteration 1: started at 1000.0, evaluated at 1000.001
    d1 = compute_deadline_delay(start_time, batches_generated=1, batch_interval=batch_interval, now=1000.001)
    # Expected target is 1000.025 -> delay is 0.024
    assert pytest.approx(d1, abs=1e-6) == 0.024

    # Suppose sleep overshot by 5 ms, so now is 1000.030
    # Iteration 2 target is 1000.050.
    d2 = compute_deadline_delay(start_time, batches_generated=2, batch_interval=batch_interval, now=1000.030)
    # Delay must automatically decrease to 0.020 to recover the 5 ms overshoot!
    assert pytest.approx(d2, abs=1e-6) == 0.020

    # Suppose iteration 2 also overshot slightly, waking at 1000.052 (2 ms lag)
    # Iteration 3 target is 1000.075.
    d3 = compute_deadline_delay(start_time, batches_generated=3, batch_interval=batch_interval, now=1000.052)
    # Delay must adjust to 0.023 to recover the 2 ms overshoot
    assert pytest.approx(d3, abs=1e-6) == 0.023


@pytest.mark.asyncio
async def test_server_realtime_sampling_rate_integration() -> None:
    """Verifies AUDIT-06: real-time generation loop runs with deadline compensation without major drift.
    
    Note: Tests sustained rate over wall-clock window without asserting hard real-time guarantees,
    as standard desktop OS schedulers have 1-15 ms timer resolution.
    """
    port = 7198
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=True,
    )
    t0 = asyncio.get_running_loop().time()
    await server.start()
    try:
        # Run for 0.4 seconds (scheduled for 16 batches = 160 samples at 400 Hz)
        await asyncio.sleep(0.40)
        elapsed = asyncio.get_running_loop().time() - t0
        samples_produced = server.generator.current_index
        
        # Calculate effective rate
        rate = samples_produced / elapsed
        # On a desktop OS, rate should be within practical bound (300 Hz to 500 Hz)
        # and definitely should have generated significant samples (>120 samples)
        assert samples_produced >= 120
        assert 300.0 <= rate <= 500.0
    finally:
        await server.stop()

