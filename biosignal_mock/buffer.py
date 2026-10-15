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

    def __init__(
        self,
        sensor: int,
        capacity: int = 4000,
        on_drop: Optional[Any] = None,
    ) -> None:
        if sensor not in (1, 2):
            raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")
        self.sensor = sensor
        self.capacity = capacity
        self.on_drop = on_drop
        self._buffer: deque[tuple[int, list[int]]] = deque(maxlen=capacity)
        
        self.earliest_index: int = 0
        self.latest_index: int = -1
        self.client_next_index: int = 0

    def push(self, sample_index: int, values: list[int]) -> None:
        """Push a single sample into the ring buffer, notifying on_drop if capacity exceeded."""
        if len(self._buffer) == self.capacity:
            dropped_idx, _ = self._buffer[0]
            if self.on_drop:
                self.on_drop(self.sensor, dropped_idx)
                
        self._buffer.append((sample_index, values))
        self.latest_index = sample_index
        self.earliest_index = self._buffer[0][0]

    def push_block(self, start_index: int, samples: list[list[int]]) -> None:
        """Push a block of contiguous samples."""
        for offset, values in enumerate(samples):
            self.push(start_index + offset, values)

    def read(
        self,
        from_index: Optional[int] = None,
        max_count: int = MAX_WINDOW_SAMPLES,
    ) -> tuple[int, list[list[int]], list[LossRecord]]:
        """Read up to max_count (<=400) samples.
        
        Returns:
            (first_index, data_samples, lost_records)
        """
        if not self._buffer:
            req_index = from_index if from_index is not None else self.client_next_index
            return req_index, [], []

        req_index = from_index if from_index is not None else self.client_next_index

        lost_to_report: list[LossRecord] = []

        # Case 1: Client is asking for data older than retained buffer window
        if req_index < self.earliest_index:
            lost_to_report.append(
                LossRecord(
                    from_index=req_index,
                    to_index=self.earliest_index - 1,
                    reason="buffer_overflow",
                )
            )
            actual_start = self.earliest_index
        # Case 2: Client is asking for future data not yet generated
        elif req_index > self.latest_index:
            return req_index, [], []
        else:
            actual_start = req_index

        # Extract contiguous samples starting from actual_start
        offset = actual_start - self.earliest_index
        count_to_fetch = min(max_count, MAX_WINDOW_SAMPLES, len(self._buffer) - offset)
        
        data_out: list[list[int]] = []
        for i in range(count_to_fetch):
            _, vals = self._buffer[offset + i]
            data_out.append(vals)

        # Advance sequential client pointer
        self.client_next_index = actual_start + count_to_fetch

        return actual_start, data_out, lost_to_report
