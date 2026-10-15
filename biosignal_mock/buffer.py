"""10-second ring buffer and explicit loss tracking for MayaCardio biosignal-mock.

Strictly follows Section 6.2:
- 10-second capacity per sensor (4000 samples at 400 Hz).
- Data not read in time is dropped and reported in the 'lost' field:
  {"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}.
- Sequential read advances automatically; re-reads with 'from_index' are supported.
- Slices deliver at most 1 second of data (400 samples).
"""

from __future__ import annotations

import bisect
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
        self._injected_losses: list[LossRecord] = []

    def inject_loss(self, from_index: int, to_index: int, reason: str = "sensor_stall") -> None:
        """Inject an explicit loss record (e.g. from sensor stall)."""
        self._injected_losses.append(
            LossRecord(from_index=from_index, to_index=to_index, reason=reason)
        )
        if self.client_next_index <= to_index:
            self.client_next_index = max(self.client_next_index, to_index + 1)

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
        req_index = from_index if from_index is not None else self.client_next_index
        lost_to_report: list[LossRecord] = []

        if self._injected_losses:
            lost_to_report.extend(self._injected_losses)
            self._injected_losses.clear()

        if not self._buffer:
            return req_index, [], lost_to_report

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
            return req_index, [], lost_to_report
        else:
            actual_start = req_index

        # Locate the first sample with index >= actual_start using binary search
        offset = bisect.bisect_left(self._buffer, actual_start, key=lambda item: item[0])
        if offset >= len(self._buffer):
            return req_index, [], lost_to_report

        actual_start = self._buffer[offset][0]
        max_possible = min(max_count, MAX_WINDOW_SAMPLES, len(self._buffer) - offset)
        
        data_out: list[list[int]] = []
        for i in range(max_possible):
            s_idx, vals = self._buffer[offset + i]
            if i > 0 and s_idx != self._buffer[offset + i - 1][0] + 1:
                # Contiguous block ended at a gap
                break
            data_out.append(vals)

        # Advance sequential client pointer
        self.client_next_index = actual_start + len(data_out)

        return actual_start, data_out, lost_to_report
