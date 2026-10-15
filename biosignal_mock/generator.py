"""Dual-sensor sample orchestrator and buffer manager for biosignal-mock.

Strictly follows Section 6.1 and 6.2:
- Synchronous start: Sample i of Sensor 1 and Sensor 2 are taken at the exact same moment.
- Generation rate: 400 samples/sec per sensor.
- Modes: ramp or signal.
- Session management: session_id resets and restarts with sample index 0.
- Integrated independent ground-truth tap.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable, List, Optional, Tuple

from biosignal_mock.buffer import SensorRingBuffer
from biosignal_mock.ramp import generate_ramp_sample_sensor1, generate_ramp_sample_sensor2
from biosignal_mock.signal import SignalGenerator
from shared.protocol import LossRecord, MAX_WINDOW_SAMPLES

SAMPLING_RATE = 400  # 400 Hz


class DualSensorGenerator:
    """Coordinates synchronous 400 Hz sample generation for both sensors."""

    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        buffer_capacity: int = 4000,
        ground_truth_tap: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> None:
        if mode not in ("ramp", "signal"):
            raise ValueError(f"Mode must be 'ramp' or 'signal', got '{mode}'.")
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.max_total_samples = duration * SAMPLING_RATE
        self.buffer_capacity = buffer_capacity
        self.ground_truth_tap = ground_truth_tap

        self.signal_gen = SignalGenerator(seed=seed) if mode == "signal" else None

        # Session state
        self.session_id: str = self._generate_session_id()
        self.current_index: int = 0
        self.is_session_ended: bool = False

        # Independent ring buffers for Sensor 1 and Sensor 2
        self.buffer_s1 = SensorRingBuffer(sensor=1, capacity=buffer_capacity, on_drop=self._on_buffer_drop)
        self.buffer_s2 = SensorRingBuffer(sensor=2, capacity=buffer_capacity, on_drop=self._on_buffer_drop)

        # Active sensor stalls: sensor_id -> (start_idx, end_idx)
        self._stalls: dict[int, tuple[int, int]] = {}

    def _on_buffer_drop(self, sensor: int, dropped_idx: int) -> None:
        """Invoked when ring buffer drops a sample due to overflow."""
        if self.ground_truth_tap:
            self.ground_truth_tap({
                "session_id": self.session_id,
                "event": "buffer_drop",
                "sensor": sensor,
                "from_index": dropped_idx,
                "to_index": dropped_idx,
                "reason": "buffer_overflow",
                "timestamp": time.time(),
            })

    @staticmethod
    def _generate_session_id() -> str:
        """Generate a random 8-character lowercase hex session ID."""
        return uuid.uuid4().hex[:8]

    def restart_session(self) -> str:
        """Simulate a mock restart (Section 6.3 fault): reset indices, generate new session_id."""
        old_session = self.session_id
        self.session_id = self._generate_session_id()
        self.current_index = 0
        self.is_session_ended = False
        self._stalls.clear()
        self.buffer_s1 = SensorRingBuffer(sensor=1, capacity=self.buffer_capacity, on_drop=self._on_buffer_drop)
        self.buffer_s2 = SensorRingBuffer(sensor=2, capacity=self.buffer_capacity, on_drop=self._on_buffer_drop)

        if self.ground_truth_tap:
            self.ground_truth_tap({
                "event": "session_restart",
                "old_session_id": old_session,
                "new_session_id": self.session_id,
                "timestamp": time.time(),
            })
        return self.session_id

    def trigger_sensor_stall(
        self,
        sensor: int,
        duration_sec: float = 0.0,
        samples: int = 0,
    ) -> tuple[int, int]:
        """Trigger selective sensor stall (Section 6.3).
        
        The stalled sensor stops delivering data across [start_idx, end_idx],
        reporting the span as lost, while the other sensor continues undisturbed.
        """
        if sensor not in (1, 2):
            raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")
        stall_count = samples if samples > 0 else int(duration_sec * SAMPLING_RATE)
        if stall_count <= 0:
            return -1, -1

        start_idx = self.current_index
        end_idx = start_idx + stall_count - 1
        self._stalls[sensor] = (start_idx, end_idx)

        buf = self.buffer_s1 if sensor == 1 else self.buffer_s2
        buf.inject_loss(from_index=start_idx, to_index=end_idx, reason="sensor_stall")

        if self.ground_truth_tap:
            self.ground_truth_tap({
                "session_id": self.session_id,
                "event": "sensor_stall",
                "sensor": sensor,
                "from_index": start_idx,
                "to_index": end_idx,
                "timestamp": time.time(),
            })
        return start_idx, end_idx

    def generate_single_step(self) -> tuple[list[int], list[int]]:
        """Advance generation by exactly one sample for both sensors."""
        if self.current_index >= self.max_total_samples:
            self.is_session_ended = True
            return [], []

        idx = self.current_index
        if self.mode == "ramp":
            s1_vals = generate_ramp_sample_sensor1(idx)
            s2_vals = generate_ramp_sample_sensor2(idx)
        else:
            assert self.signal_gen is not None
            s1_vals = self.signal_gen.generate_sensor1_sample(idx)
            s2_vals = self.signal_gen.generate_sensor2_sample(idx)

        # Check selective sensor stall state
        s1_stalled = False
        if 1 in self._stalls:
            s_from, s_to = self._stalls[1]
            if s_from <= idx <= s_to:
                s1_stalled = True
            elif idx > s_to:
                del self._stalls[1]

        s2_stalled = False
        if 2 in self._stalls:
            s_from, s_to = self._stalls[2]
            if s_from <= idx <= s_to:
                s2_stalled = True
            elif idx > s_to:
                del self._stalls[2]

        # Push into respective ring buffers only if not stalled
        if not s1_stalled:
            self.buffer_s1.push(idx, s1_vals)
        if not s2_stalled:
            self.buffer_s2.push(idx, s2_vals)

        # Emit to independent candidate ground-truth tap if configured
        if self.ground_truth_tap:
            now_ts = time.time()
            if not s1_stalled:
                self.ground_truth_tap({
                    "session_id": self.session_id,
                    "sensor": 1,
                    "index": idx,
                    "values": s1_vals,
                    "timestamp": now_ts,
                })
            if not s2_stalled:
                self.ground_truth_tap({
                    "session_id": self.session_id,
                    "sensor": 2,
                    "index": idx,
                    "values": s2_vals,
                    "timestamp": now_ts,
                })

        self.current_index += 1
        if self.current_index >= self.max_total_samples:
            self.is_session_ended = True

        return s1_vals, s2_vals

    def advance_samples(self, count: int) -> int:
        """Advance generation by count samples."""
        actual_generated = 0
        for _ in range(count):
            if self.is_session_ended:
                break
            self.generate_single_step()
            actual_generated += 1
        return actual_generated

    def read_sensor(
        self,
        sensor: int,
        from_index: Optional[int] = None,
        max_count: int = MAX_WINDOW_SAMPLES,
    ) -> tuple[int, list[list[int]], list[LossRecord], bool]:
        """Read data for requested sensor.
        
        Returns:
            (first_index, data, lost, session_end)
        """
        buf = self.buffer_s1 if sensor == 1 else self.buffer_s2
        first_index, data, lost = buf.read(from_index=from_index, max_count=max_count)
        
        # session_end is true if the session is finished and this read reaches the end
        session_end = self.is_session_ended and (buf.client_next_index >= self.max_total_samples)
        return first_index, data, lost, session_end
