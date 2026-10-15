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
    """Verifies REQ-A-11: pushing beyond 4000 samples drops older data and logs loss."""
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
    assert len(lost) >= 1
    # Check that dropped range [0, 999] was reported
    assert any(
        lr.from_index == 0 and lr.to_index == 999 and lr.reason == "buffer_overflow"
        for lr in lost
    )


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


def test_from_index_expired_reports_loss() -> None:
    """Verifies re-reading an index that has already fallen off the buffer reports loss."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)
    # Push 6000 samples (0 to 5999). Earliest is 2000.
    for i in range(6000):
        buf.push(i, [1, 2, 3])

    # Ask for index 500 (which was dropped long ago)
    first_idx, data, lost = buf.read(from_index=500, max_count=400)
    assert first_idx == 2000
    assert len(data) == 400
    assert len(lost) >= 1
    # Must report loss from requested index 500 to earliest available - 1 (1999)
    assert any(
        lr.from_index == 500 and lr.to_index == 1999 and lr.reason == "buffer_overflow"
        for lr in lost
    )


def test_future_index_read_returns_empty() -> None:
    """Verifies asking for a sample index that has not been generated yet returns 0 count."""
    buf = SensorRingBuffer(sensor=1, capacity=4000)
    for i in range(100):
        buf.push(i, [1, 2, 3])

    first_idx, data, lost = buf.read(from_index=500)
    assert first_idx == 500
    assert len(data) == 0
    assert len(lost) == 0
