"""10-second ring buffer and explicit loss tracking for MayaCardio biosignal-mock.

Strictly follows Section 6.2:
- 10-second capacity per sensor (4000 samples at 400 Hz).
- Data not read in time is dropped and reported in the 'lost' field:
  {"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}.
- Sequential read advances automatically; re-reads with 'from_index' are supported.
- Slices deliver at most 1 second of data (400 samples).
"""

from __future__ import annotations

from collections import deque
from typing import Any, List, Optional, Tuple
from shared.protocol import LossRecord, MAX_WINDOW_SAMPLES


class SensorRingBuffer:
    """Bounded ring buffer holding the last 10 seconds of data for one sensor."""

    def __init__(self, sensor: int, capacity: int = 4000) -> None:
        if sensor not in (1, 2):
            raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")
        self.sensor = sensor
        self.capacity = capacity
        self._buffer: deque[tuple[int, list[int]]] = deque(maxlen=capacity)
        
        self.earliest_index: int = 0
        self.latest_index: int = -1
        self.client_next_index: int = 0
        
        # Accumulates overflow drop ranges that have occurred since last client read
        self._unreported_drops: list[LossRecord] = []
        self._drop_range_start: Optional[int] = None
        self._drop_range_end: Optional[int] = None

    def push(self, sample_index: int, values: list[int]) -> None:
        """Push a single sample into the ring buffer, tracking overflow drops."""
        if len(self._buffer) == self.capacity:
            # The oldest sample in the buffer is about to be overwritten
            dropped_idx, _ = self._buffer[0]
            if self._drop_range_start is None:
                self._drop_range_start = dropped_idx
                self._drop_range_end = dropped_idx
            else:
                self._drop_range_end = dropped_idx
                
        self._buffer.append((sample_index, values))
        self.latest_index = sample_index
        self.earliest_index = self._buffer[0][0]

    def push_block(self, start_index: int, samples: list[list[int]]) -> None:
        """Push a block of contiguous samples."""
        for offset, values in enumerate(samples):
            self.push(start_index + offset, values)

    def _flush_accumulated_drop(self) -> Optional[LossRecord]:
        """Convert pending drop range into a LossRecord and reset."""
        if self._drop_range_start is not None and self._drop_range_end is not None:
            rec = LossRecord(
                from_index=self._drop_range_start,
                to_index=self._drop_range_end,
                reason="buffer_overflow",
            )
            self._drop_range_start = None
            self._drop_range_end = None
            return rec
        return None

    def read(
        self,
        from_index: Optional[int] = None,
        max_count: int = MAX_WINDOW_SAMPLES,
    ) -> tuple[int, list[list[int]], list[LossRecord]]:
        """Read up to max_count (<=400) samples.
        
        Returns:
            (first_index, data_samples, lost_records)
        """
        lost_to_report: list[LossRecord] = []
        
        # Check any un-flushed drops from background buffer pushes
        drop_rec = self._flush_accumulated_drop()
        if drop_rec:
            lost_to_report.append(drop_rec)

        if not self._buffer:
            # Buffer empty (e.g. at start of session before generation)
            return self.client_next_index, [], lost_to_report

        if from_index is not None:
            req_index = from_index
        else:
            req_index = self.client_next_index

        # Case 1: Client is asking for data older than what is currently in buffer
        if req_index < self.earliest_index:
            # That entire span was dropped!
            loss_span = LossRecord(
                from_index=req_index,
                to_index=self.earliest_index - 1,
                reason="buffer_overflow",
            )
            # Avoid duplicate reporting if already covered in lost_to_report
            if not any(lr.from_index == loss_span.from_index and lr.to_index == loss_span.to_index for lr in lost_to_report):
                lost_to_report.append(loss_span)
            actual_start = self.earliest_index
        # Case 2: Client is asking for future data not yet generated
        elif req_index > self.latest_index:
            return req_index, [], lost_to_report
        else:
            actual_start = req_index

        # Extract contiguous samples starting from actual_start
        # Compute offset in deque
        offset = actual_start - self.earliest_index
        count_to_fetch = min(max_count, MAX_WINDOW_SAMPLES, len(self._buffer) - offset)
        
        data_out: list[list[int]] = []
        for i in range(count_to_fetch):
            _, vals = self._buffer[offset + i]
            data_out.append(vals)

        # Advance sequential client pointer
        self.client_next_index = actual_start + count_to_fetch

        return actual_start, data_out, lost_to_report
