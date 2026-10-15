"""MayaCardio Cloud Signal Processing Service (Phase 4A).

Responsible for:
- Executing Section 3 online window processing using the shared deterministic
  SignalProcessingEngine with mode="online".
- Strict prevention of premature or incomplete window evaluations:
  * A window w ([400*w, 400*w + 399]) is ready if and only if BOTH Sensor 1 and Sensor 2
    have contiguous_resolved_index >= 400*w + 399, OR the session has cleanly ended.
  * Ensures no window is evaluated while unreceived samples or in-flight bursts
    could change missing counts, channel statistics, or completeness.
- Exact Section 3 math:
  * Missing sample accounting (zero fabricated samples).
  * 1-decimal-place channel means, mins, maxs, and Sensor 2 magnitude.
  * Exact 5-second heart rate with 800-sample lookback, 100-sample refractory distance,
    and null rules (w < 4, missing samples in 5-second interval, or < 2 peaks).
- Exact parity between cloud online mode and device offline mode.
- Persistent caching of computed window results into cloud storage.
"""

from __future__ import annotations

import logging
from typing import Optional

from pipeline.cloud.storage import CloudStorageEngine
from pipeline.processing import (
    SignalProcessingEngine,
    WindowResult,
    WINDOW_SAMPLES,
)

logger = logging.getLogger("pipeline.cloud.service")


class CloudProcessingService:
    """Orchestrates Section 3 online signal processing over cloud storage."""

    def __init__(self, storage: CloudStorageEngine) -> None:
        """Initialize with an underlying cloud storage engine."""
        self.storage = storage

    def get_max_ready_window(self, session_id: str) -> int:
        """Determine the highest window index w that is safe to evaluate without premature emission.

        Returns:
            Highest window index >= 0 that is ready, or -1 if no window is ready yet.
        """
        ckpt1 = self.storage.get_checkpoint(session_id, sensor=1)
        ckpt2 = self.storage.get_checkpoint(session_id, sensor=2)
        session_ended = self.storage.is_session_ended(session_id)

        r1 = ckpt1.contiguous_resolved_index if ckpt1 else -1
        r2 = ckpt2.contiguous_resolved_index if ckpt2 else -1

        if session_ended:
            # Session is closed: any window with observed data can be finalized
            obs1 = ckpt1.highest_observed_index if ckpt1 else -1
            obs2 = ckpt2.highest_observed_index if ckpt2 else -1
            max_obs = max(obs1, obs2)
            if max_obs < 0:
                return -1
            return max_obs // WINDOW_SAMPLES

        # Active stream: window w is ready ONLY when both sensors have resolved through the end of w
        min_resolved = min(r1, r2)
        if min_resolved < WINDOW_SAMPLES - 1:
            return -1
        return min_resolved // WINDOW_SAMPLES

    def process_ready_windows(self, session_id: str) -> list[WindowResult]:
        """Compute and persist all newly ready windows for a session.

        Returns:
            List of all computed WindowResult objects for the session up to max_ready_window.
        """
        max_ready_w = self.get_max_ready_window(session_id)
        if max_ready_w < 0:
            return []

        # Check existing computed windows in storage
        existing_results = self.storage.get_window_results(session_id)
        existing_windows = {r.window for r in existing_results}

        # Determine if there are uncomputed ready windows
        needed = [w for w in range(max_ready_w + 1) if w not in existing_windows]
        if not needed:
            return [r for r in existing_results if r.window <= max_ready_w]

        # Load all session data and loss records
        s1_samples, s2_samples = self.storage.get_samples_for_session(session_id)
        loss_ranges = self.storage.get_loss_records_for_session(session_id)

        # Process needed windows with 5-second lookback priming
        eval_start_w = max(0, min(needed) - 4)
        windows_to_process = list(range(eval_start_w, max_ready_w + 1))

        engine = SignalProcessingEngine(session_id=session_id, mode="online")
        new_results = engine.process_windows(
            windows=windows_to_process,
            sensor1_samples=s1_samples,
            sensor2_samples=s2_samples,
            loss_ranges=loss_ranges,
            session_id=session_id,
        )

        # Persist evaluated window results in a single atomic transaction
        self.storage.save_window_results(new_results)

        return self.storage.get_window_results(session_id)

    def get_results(self, session_id: str) -> list[WindowResult]:
        """Fetch all verified window results for a session, computing any pending ready windows."""
        return self.process_ready_windows(session_id)
