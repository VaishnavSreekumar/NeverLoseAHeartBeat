"""Unit tests for biosignal_mock/buffer.py (Increment D).

Verifies REQ-A-11:
- 10-second (4000 sample) capacity limit
- Retention of the most recent 4000 samples
- Accurate loss range reporting: [{"from_index": X, "to_index": Y, "reason": "buffer_overflow"}]
- 1-second (400 samples) maximum delivery chunk
- from_index re-read behavior within and outside buffer bounds
"""

import pytest
from biosignal_mock.buffer import SensorRingBuffer
from shared.protocol import MAX_WINDOW_SAMPLES


def test_buffer_nominal_sequential_reading() -> None:
    """Verifies standard sequential reads deliver up to 400 samples without loss."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)

    # Push 800 samples (2 seconds of data)
    for i in range(800):
        buf.push(i, [100, 200, 300])

    # First read fetches 400 samples [0, 399]
    first_idx, data, lost = buf.read()
    assert first_idx == 0
    assert len(data) == 400
    assert len(lost) == 0

    # Second read fetches next 400 samples [400, 799]
    first_idx, data, lost = buf.read()
    assert first_idx == 400
    assert len(data) == 400
    assert len(lost) == 0

    # Third read finds buffer drained up to latest index
    first_idx, data, lost = buf.read()
    assert len(data) == 0
    assert len(lost) == 0


def test_buffer_overflow_and_loss_reporting() -> None:
    """Verifies REQ-A-11: pushing beyond 4000 samples drops older data and logs exact loss."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)

    # Push 5000 samples (indices 0 to 4999)
    # The first 1000 samples (0 to 999) must be dropped
    for i in range(5000):
        buf.push(i, [i, i * 2, i * 3])

    assert buf.earliest_index == 1000
    assert buf.latest_index == 4999

    # Client reads from uninitialized pointer (starts at 0)
    first_idx, data, lost = buf.read()
    assert first_idx == 1000
    assert len(data) == MAX_WINDOW_SAMPLES  # 400 samples: [1000, 1399]
    # EXACTLY ONE loss record covering [0, 999]
    assert len(lost) == 1
    assert lost[0].from_index == 0
    assert lost[0].to_index == 999
    assert lost[0].reason == "buffer_overflow"


def test_sequential_read_then_overflow_loss_accounting() -> None:
    """Verifies AUDIT-02: samples already delivered are NOT marked lost, and no duplicate records."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)

    # 1. Push 400 samples and client reads them
    for i in range(400):
        buf.push(i, [i, i, i])
    first_idx, data, lost = buf.read()
    assert first_idx == 0
    assert len(data) == 400
    assert len(lost) == 0
    assert buf.client_next_index == 400

    # 2. Push 4100 more samples (400 to 4499)
    # Total samples pushed: 4500. Earliest retained is 500 (since capacity is 4000).
    for i in range(400, 4500):
        buf.push(i, [i, i, i])
    assert buf.earliest_index == 500
    assert buf.latest_index == 4499

    # 3. Client calls read() next.
    # Client expected sample 400, but earliest is 500.
    # Lost range MUST be [400, 499] ONLY (NOT [0, 499], since [0, 399] was already read!)
    first_idx2, data2, lost2 = buf.read()
    assert first_idx2 == 500
    assert len(data2) == 400
    assert len(lost2) == 1
    assert lost2[0].from_index == 400
    assert lost2[0].to_index == 499
    assert lost2[0].reason == "buffer_overflow"

    # 4. Immediate subsequent read should have zero loss
    first_idx3, data3, lost3 = buf.read()
    assert first_idx3 == 900
    assert len(data3) == 400
    assert len(lost3) == 0


def test_from_index_within_buffer() -> None:
    """Verifies re-reading from an index that is still within the 10-second buffer."""
    buf = SensorRingBuffer(sensor=2, capacity=4000)
    for i in range(2000):
        buf.push(i, [400_000, 150_000])

    # Re-read from index 500
    first_idx, data, lost = buf.read(from_index=500, max_count=100)
    assert first_idx == 500
    assert len(data) == 100
    assert len(lost) == 0
    assert buf.client_next_index == 600


def test_from_index_expired_reports_loss() -> None:
    """Verifies re-reading an index that has already fallen off the buffer reports exact loss."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)
    # Push 6000 samples (0 to 5999). Earliest is 2000.
    for i in range(6000):
        buf.push(i, [1, 2, 3])

    # Ask for index 500 (which was dropped long ago)
    first_idx, data, lost = buf.read(from_index=500, max_count=400)
    assert first_idx == 2000
    assert len(data) == 400
    # Must report EXACTLY ONE loss record from 500 to 1999 (NOT multiple overlapping records)
    assert len(lost) == 1
    assert lost[0].from_index == 500
    assert lost[0].to_index == 1999
    assert lost[0].reason == "buffer_overflow"


def test_future_index_read_returns_empty() -> None:
    """Verifies asking for a sample index that has not been generated yet returns 0 count."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)
    for i in range(100):
        buf.push(i, [1, 2, 3])

    first_idx, data, lost = buf.read(from_index=500)
    assert first_idx == 500
    assert len(data) == 0
    assert len(lost) == 0
