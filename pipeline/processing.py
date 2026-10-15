"""MayaCardio Pure Deterministic Signal Processing Engine (Section 3).

Conforms strictly to Section 3 of raw_assessment_spec.txt:
- 400 Hz dual-sensor windowing (1-second windows: indices 400*w through 400*w + 399).
- Missing sample counting and loss accounting without synthetic data fabrication.
- Channel summary statistics (mean, min, max) rounded to 1 decimal place.
- Sensor 2 magnitude calculation: mean(sqrt(ch1**2 + ch2**2)) over available samples.
- Exact ECG peak detection on Sensor 1 Channel 2:
  1. 800 preceding samples and 1 following sample available.
  2. Strict local maximum: x[i] > x[i-1] and x[i] > x[i+1].
  3. Dynamic threshold: x[i] > 0.5 * max(x[i-800:i]).
  4. Refractory distance: i - i_prev >= 100 sample indices.
- Exact 5-second heart rate calculation:
  - Interval: 400*(w-4) through 400*(w+1)-1 (2,000 samples).
  - Null if w < 4, if any sample in that 5-second interval is missing, or if < 2 peaks.
  - Heart rate = 60 / mean(peak_gaps_in_seconds) rounded to 1 decimal place.
- Loss ledger consistency: validates that declared loss ranges do not contradict
  available samples, with full support for bounded and open-ended (to_index=None) loss.
- Boundary peak progression: guarantees boundary samples are evaluated exactly once
  when the subsequent window chunk arrives.
- Strict session isolation: prevents state bleed across unrelated sessions.

Completely decoupled from databases, networking, filesystems, cloud services,
wall-clock time, and mutable global state.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

# Mathematical constants defined by Section 3 and Section 6.1
SAMPLING_RATE_HZ: int = 400
WINDOW_SAMPLES: int = 400  # 1 second = 400 samples at 400 Hz
PEAK_LOOKBACK_SAMPLES: int = 800  # 800 preceding samples required for threshold
PEAK_REFRACTORY_SAMPLES: int = 100  # Minimum 100 samples between accepted peaks
HEART_RATE_WINDOW_SPAN_SEC: int = 5  # 5-second interval
HEART_RATE_WINDOW_SPAN_SAMPLES: int = HEART_RATE_WINDOW_SPAN_SEC * SAMPLING_RATE_HZ  # 2000 samples
HEART_RATE_MIN_WINDOW: int = 4  # Window index must be >= 4
HEART_RATE_MIN_PEAKS: int = 2  # At least 2 accepted peaks required for rate calculation
SENSOR_1_CHANNELS: int = 3
SENSOR_2_CHANNELS: int = 2


@dataclass(frozen=True)
class Sample:
    """Explicitly typed sample representation.

    Attributes:
        sensor: Sensor identifier (1 or 2).
        index: Non-negative sample index (at 400 Hz).
        values: Signed 24-bit integer channel values (length 3 for S1, length 2 for S2).
    """

    sensor: int
    index: int
    values: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.sensor not in (1, 2):
            raise ValueError(f"sensor must be 1 or 2, got {self.sensor}")
        if self.index < 0:
            raise ValueError(f"index must be non-negative, got {self.index}")
        expected_ch = SENSOR_1_CHANNELS if self.sensor == 1 else SENSOR_2_CHANNELS
        if len(self.values) != expected_ch:
            raise ValueError(
                f"Sensor {self.sensor} requires {expected_ch} channels, got {len(self.values)}"
            )


@dataclass(frozen=True)
class LossRange:
    """Authoritative loss record range for a sensor.

    Attributes:
        sensor: Sensor identifier (1 or 2).
        from_index: First lost sample index (inclusive).
        to_index: Last lost sample index (inclusive), or None if unbounded.
        reason: Diagnostic reason for loss (e.g. 'buffer_overflow', 'network_outage').
    """

    sensor: int
    from_index: int
    to_index: Optional[int] = None
    reason: str = "unknown"

    def contains(self, index: int) -> bool:
        """Check if a sample index falls within this loss range."""
        if index < self.from_index:
            return False
        if self.to_index is not None and index > self.to_index:
            return False
        return True


@dataclass(frozen=True)
class ChannelStats:
    """Summary statistics for a single sensor channel over available samples."""

    mean: Optional[float]
    min: Optional[int]
    max: Optional[int]


@dataclass(frozen=True)
class WindowResult:
    """Section 3 window processing result.

    Attributes:
        window: Window index w (non-negative integer).
        mode: Processing mode ('online' or 'offline').
        missing_1: Count of missing Sensor 1 samples in [400*w, 400*w + 399].
        missing_2: Count of missing Sensor 2 samples in [400*w, 400*w + 399].
        complete: True if and only if both missing_1 == 0 and missing_2 == 0.
        mean: Channel means per sensor: {1: [ch1, ch2, ch3], 2: [ch1, ch2]}.
        min: Channel minimums per sensor: {1: [ch1, ch2, ch3], 2: [ch1, ch2]}.
        max: Channel maximums per sensor: {1: [ch1, ch2, ch3], 2: [ch1, ch2]}.
        magnitude: Mean of sqrt(ch1**2 + ch2**2) over available Sensor 2 samples, or None.
        heart_rate: ECG heart rate in bpm from Sensor 1 Channel 2, or None.
        session_id: Optional session identifier.
    """

    window: int
    mode: str
    missing_1: int
    missing_2: int
    complete: bool
    mean: dict[int, list[Optional[float]]]
    min: dict[int, list[Optional[int]]]
    max: dict[int, list[Optional[int]]]
    magnitude: Optional[float]
    heart_rate: Optional[float]
    session_id: Optional[str] = None

    @property
    def mean_1(self) -> list[Optional[float]]:
        """Sensor 1 channel means."""
        return self.mean[1]

    @property
    def mean_2(self) -> list[Optional[float]]:
        """Sensor 2 channel means."""
        return self.mean[2]

    @property
    def min_1(self) -> list[Optional[int]]:
        """Sensor 1 channel minimums."""
        return self.min[1]

    @property
    def min_2(self) -> list[Optional[int]]:
        """Sensor 2 channel minimums."""
        return self.min[2]

    @property
    def max_1(self) -> list[Optional[int]]:
        """Sensor 1 channel maximums."""
        return self.max[1]

    @property
    def max_2(self) -> list[Optional[int]]:
        """Sensor 2 channel maximums."""
        return self.max[2]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to Section 3 NDJSON-compatible dictionary.

        Supports Section 4 CLI `pipeline results --session ID` format:
        one JSON object per window.
        """
        d: dict[str, Any] = {
            "window": self.window,
            "mode": self.mode,
            "complete": self.complete,
            "missing_1": self.missing_1,
            "missing_2": self.missing_2,
            "mean": {"1": self.mean[1], "2": self.mean[2]},
            "min": {"1": self.min[1], "2": self.min[2]},
            "max": {"1": self.max[1], "2": self.max[2]},
            "magnitude": self.magnitude,
            "heart_rate": self.heart_rate,
        }
        if self.session_id is not None:
            d["session_id"] = self.session_id
        return d

    def __getitem__(self, item: str) -> Any:
        """Allow dict-style key access for backward/forward compatibility."""
        if item == "window":
            return self.window
        elif item == "mode":
            return self.mode
        elif item == "complete":
            return self.complete
        elif item == "missing_1":
            return self.missing_1
        elif item == "missing_2":
            return self.missing_2
        elif item == "mean":
            return self.mean
        elif item == "min":
            return self.min
        elif item == "max":
            return self.max
        elif item == "magnitude":
            return self.magnitude
        elif item == "heart_rate":
            return self.heart_rate
        elif item == "session_id":
            return self.session_id
        raise KeyError(item)


def get_window_bounds(window: int) -> tuple[int, int]:
    """Calculate the sample index interval [start, end] inclusive for window w.

    Args:
        window: 0-indexed window index w >= 0.

    Returns:
        Tuple of (400*w, 400*w + 399).
    """
    if window < 0:
        raise ValueError(f"window index must be non-negative, got {window}")
    start = WINDOW_SAMPLES * window
    end = start + WINDOW_SAMPLES - 1
    return start, end


def get_heart_rate_interval(window: int) -> tuple[int, int]:
    """Calculate the 5-second sample index interval for window w.

    Per Section 3: samples 400*(w - 4) to 400*(w + 1) - 1.

    Args:
        window: Window index w.

    Returns:
        Tuple of (start_idx, end_idx) inclusive (2,000 samples).
    """
    start = WINDOW_SAMPLES * (window - 4)
    end = WINDOW_SAMPLES * (window + 1) - 1
    return start, end


def validate_loss_consistency(
    sensor: int,
    available_indices: Set[int],
    loss_ranges: Optional[Sequence[LossRange]],
) -> None:
    """Verify declared loss ranges do not contradict available samples.

    Raises:
        ValueError: If an available sample index falls within a declared loss range.
    """
    if not loss_ranges or not available_indices:
        return

    for loss in loss_ranges:
        if loss.sensor != sensor:
            continue

        # Check each available sample against the loss span
        for idx in available_indices:
            if loss.contains(idx):
                to_str = str(loss.to_index) if loss.to_index is not None else "None"
                raise ValueError(
                    f"Contradictory data on sensor {sensor}: sample index {idx} is "
                    f"present in available samples but declared lost in "
                    f"[{loss.from_index}, {to_str}] ({loss.reason})"
                )


def normalize_sensor_samples(
    samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
    expected_sensor: int,
) -> dict[int, tuple[int, ...]]:
    """Convert input sample representation into a validated index-to-values map.

    Rejects duplicate sample indices with conflicting channel values.

    Args:
        samples: Either a mapping {index: values} or a sequence of Sample objects.
        expected_sensor: Sensor ID (1 or 2).

    Returns:
        Dictionary mapping sample_index -> tuple of channel values.

    Raises:
        ValueError: On conflicting duplicate records or invalid channel counts.
    """
    expected_ch = SENSOR_1_CHANNELS if expected_sensor == 1 else SENSOR_2_CHANNELS
    result: dict[int, tuple[int, ...]] = {}

    if isinstance(samples, Mapping):
        for idx, vals in samples.items():
            if isinstance(vals, (int, float)):
                tup = (int(vals),)
            else:
                tup = tuple(int(v) for v in vals)
            result[idx] = tup
    elif isinstance(samples, Sequence):
        for s in samples:
            if isinstance(s, Sample):
                if s.sensor != expected_sensor:
                    continue
                idx = s.index
                vals = tuple(s.values)
            elif isinstance(s, (tuple, list)) and len(s) >= 2:
                # Format: (sensor, index, values) or (index, values)
                if len(s) == 3 and isinstance(s[0], int) and s[0] in (1, 2):
                    if s[0] != expected_sensor:
                        continue
                    idx = s[1]
                    vals = tuple(s[2])
                else:
                    idx = s[0]
                    vals = tuple(s[1])
            else:
                raise ValueError(f"Unsupported sample item format: {s}")

            if idx in result:
                if result[idx] != vals:
                    raise ValueError(
                        f"Conflicting duplicate sample records for sensor {expected_sensor} at index {idx}: "
                        f"existing {result[idx]} != incoming {vals}"
                    )
            else:
                result[idx] = vals
    else:
        raise ValueError(f"samples must be a Mapping or Sequence, got {type(samples)}")

    return result


def compute_channel_stats(
    samples_in_window: Sequence[Sequence[int]],
    expected_channels: int,
) -> tuple[list[Optional[float]], list[Optional[int]], list[Optional[int]]]:
    """Compute mean, min, and max per channel over available samples.

    Args:
        samples_in_window: List of channel value tuples for available samples.
        expected_channels: Number of channels expected for this sensor (3 for S1, 2 for S2).

    Returns:
        Tuple of (means, mins, maxs). If no samples are available, each channel
        contains None.
    """
    if not samples_in_window:
        return (
            [None] * expected_channels,
            [None] * expected_channels,
            [None] * expected_channels,
        )

    means: list[Optional[float]] = []
    mins: list[Optional[int]] = []
    maxs: list[Optional[int]] = []

    count = len(samples_in_window)
    for ch_idx in range(expected_channels):
        channel_vals = [s[ch_idx] for s in samples_in_window]
        ch_min = min(channel_vals)
        ch_max = max(channel_vals)
        ch_mean = round(sum(channel_vals) / count, 1)

        means.append(ch_mean)
        mins.append(ch_min)
        maxs.append(ch_max)

    return means, mins, maxs


def compute_sensor2_magnitude(samples_in_window: Sequence[Sequence[int]]) -> Optional[float]:
    """Compute Sensor 2 magnitude as mean of sqrt(ch1**2 + ch2**2) over available samples.

    Args:
        samples_in_window: List of (ch1, ch2) tuples for available Sensor 2 samples.

    Returns:
        Magnitude rounded to 1 decimal place, or None if no samples are available.
    """
    if not samples_in_window:
        return None

    total_mag = 0.0
    for sample in samples_in_window:
        ch1 = sample[0]
        ch2 = sample[1]
        mag = math.sqrt(ch1 * ch1 + ch2 * ch2)
        total_mag += mag

    return round(total_mag / len(samples_in_window), 1)


def is_peak_at_index(
    i: int,
    signal_map: Mapping[int, int],
    previous_peak_index: Optional[int] = None,
) -> bool:
    """Determine whether sample i is an accepted peak according to Section 3.

    A sample i is a peak only if all four conditions hold:
    1. The 800 preceding samples (i-800 to i-1) and the sample immediately
       following i (i+1) are available.
    2. x[i] > x[i-1] and x[i] > x[i+1] (strictly larger than both neighbours).
    3. x[i] > 0.5 * max(x[i-800:i]) (strictly larger than half of the maximum of the
       800 samples before it).
    4. At least 100 sample indices separate it from the previous accepted peak
       (i - previous_peak_index >= 100).

    Args:
        i: Candidate sample index.
        signal_map: Mapping of sample_index -> Sensor 1 Channel 2 value.
        previous_peak_index: Index of the most recently accepted peak, if any.

    Returns:
        True if sample i satisfies all four Section 3 peak conditions, False otherwise.
    """
    # Indices before 0 do not exist, so i must be at least 800
    if i < PEAK_LOOKBACK_SAMPLES:
        return False

    # Check sample i itself
    if i not in signal_map:
        return False
    xi = signal_map[i]

    # Condition 1: Sample immediately following i must be available
    if (i + 1) not in signal_map:
        return False
    x_next = signal_map[i + 1]

    # Condition 2: Strict neighbour comparison
    if (i - 1) not in signal_map:
        return False
    x_prev = signal_map[i - 1]

    if not (xi > x_prev and xi > x_next):
        return False

    # Condition 4: Refractory period check against previous accepted peak
    if previous_peak_index is not None:
        if (i - previous_peak_index) < PEAK_REFRACTORY_SAMPLES:
            return False

    # Condition 1 & 3: The 800 preceding samples must be available and xi > 0.5 * max(...)
    # Slicing x[i-800:i]: indices i-800 through i-1
    max_preceding = -float("inf")
    start_lookback = i - PEAK_LOOKBACK_SAMPLES
    for j in range(start_lookback, i):
        if j not in signal_map:
            # Missing sample in the required 800 preceding window
            return False
        val = signal_map[j]
        if val > max_preceding:
            max_preceding = val

    threshold = 0.5 * max_preceding
    if not (xi > threshold):
        return False

    return True


def detect_peaks_in_range(
    signal_map: Mapping[int, int],
    start_index: int = 0,
    end_index: Optional[int] = None,
    previous_peak_index: Optional[int] = None,
) -> list[int]:
    """Detect all Section 3 accepted peaks within [start_index, end_index].

    Candidate indices are examined in strictly increasing chronological order.
    When a peak is accepted, the refractory constraint is updated immediately.

    Args:
        signal_map: Mapping of sample index to Sensor 1 Channel 2 value.
        start_index: First candidate index to evaluate (must be >= 800 to be a peak).
        end_index: Last candidate index to evaluate (inclusive). If None, max index in map.
        previous_peak_index: Previous accepted peak index before start_index.

    Returns:
        List of accepted peak indices in strictly increasing order.
    """
    if not signal_map:
        return []

    min_eval = max(start_index, PEAK_LOOKBACK_SAMPLES)
    max_eval = end_index if end_index is not None else max(signal_map.keys())

    accepted: list[int] = []
    last_peak = previous_peak_index

    for i in range(min_eval, max_eval + 1):
        if is_peak_at_index(i, signal_map, previous_peak_index=last_peak):
            accepted.append(i)
            last_peak = i

    return accepted


def calculate_heart_rate_from_peaks(
    peaks_in_interval: Sequence[int],
) -> Optional[float]:
    """Calculate heart rate in bpm from accepted peaks in a 5-second interval.

    Rule:
    heart_rate = 60 / mean(gaps_between_consecutive_peaks_in_seconds)
    At 400 Hz, gap in seconds = (peak[k+1] - peak[k]) / 400.
    Mean gap in seconds = (peak[K-1] - peak[0]) / (400 * (K - 1)).
    heart_rate = (60 * 400 * (K - 1)) / (peak[K-1] - peak[0]).

    Args:
        peaks_in_interval: List of accepted peak indices in the 5-second interval.

    Returns:
        Heart rate in bpm rounded to 1 decimal place, or None if < 2 peaks.
    """
    k = len(peaks_in_interval)
    if k < HEART_RATE_MIN_PEAKS:
        return None

    total_span_samples = peaks_in_interval[-1] - peaks_in_interval[0]
    if total_span_samples <= 0:
        return None

    # Mean gap in seconds:
    mean_gap_sec = (total_span_samples / SAMPLING_RATE_HZ) / (k - 1)
    hr = 60.0 / mean_gap_sec
    return round(hr, 1)


def compute_window_heart_rate(
    window: int,
    sensor1_channel2_map: Mapping[int, int],
    accepted_peaks_history: Optional[Sequence[int]] = None,
) -> Optional[float]:
    """Compute Section 3 heart rate for window w.

    Enforces all Section 3 rate and no-rate rules:
    - heart_rate is null if w < 4;
    - heart_rate is null if any sample in the 5-second interval is missing;
    - heart_rate is null if fewer than 2 accepted peaks are present.
    - Otherwise, heart_rate = 60 / mean(gaps_between_consecutive_peaks_in_seconds).

    When accepted_peaks_history is not provided (stateless execution), peak
    detection scans from the earliest possible peak in the provided continuous
    history to preserve 100% refractory parity with stateful execution.

    Args:
        window: Window index w.
        sensor1_channel2_map: Mapping of index -> S1 Ch2 value.
        accepted_peaks_history: Precomputed list of accepted peak indices.
            If None, peaks are detected across the required interval preserving history.

    Returns:
        Heart rate in bpm rounded to 1 decimal place, or None.
    """
    # Rule 3: heart_rate is null if w < 4
    if window < HEART_RATE_MIN_WINDOW:
        return None

    interval_start, interval_end = get_heart_rate_interval(window)

    # Rule 3: heart_rate is null if ANY sample in those 5 seconds is missing
    # Under Section 3 Heart-rate rule, checks Sensor 1 Channel 2 samples:
    for idx in range(interval_start, interval_end + 1):
        if idx not in sensor1_channel2_map:
            return None

    # Filter accepted peaks whose index falls in the 5-second interval [interval_start, interval_end]
    if accepted_peaks_history is not None:
        peaks_in_5s = [p for p in accepted_peaks_history if interval_start <= p <= interval_end]
    else:
        # Stateless peak detection:
        # To guarantee refractory continuity from earlier peaks, scan from the earliest
        # valid peak index available in history.
        min_available = min(sensor1_channel2_map.keys()) if sensor1_channel2_map else 0
        earliest_possible_peak = max(PEAK_LOOKBACK_SAMPLES, min_available + PEAK_LOOKBACK_SAMPLES)
        all_peaks = detect_peaks_in_range(
            sensor1_channel2_map,
            start_index=earliest_possible_peak,
            end_index=interval_end,
        )
        peaks_in_5s = [p for p in all_peaks if interval_start <= p <= interval_end]

    # Rule 3: heart_rate is null if fewer than 2 peaks
    return calculate_heart_rate_from_peaks(peaks_in_5s)


def process_window(
    window: int,
    sensor1_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
    sensor2_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
    loss_ranges: Optional[Sequence[LossRange]] = None,
    session_id: Optional[str] = None,
    mode: str = "offline",
    accepted_peaks_history: Optional[Sequence[int]] = None,
) -> WindowResult:
    """Process a single 1-second window w conforming strictly to Section 3.

    Args:
        window: Window index w >= 0.
        sensor1_samples: Mapping of sample index -> values, or sequence of Sample objects.
        sensor2_samples: Mapping of sample index -> values, or sequence of Sample objects.
        loss_ranges: Optional sequence of LossRange objects (for loss accounting and validation).
        session_id: Optional session identifier.
        mode: 'online' or 'offline'.
        accepted_peaks_history: Optional precomputed accepted peaks across the session.

    Returns:
        WindowResult containing Section 3 outputs.

    Raises:
        ValueError: If sample inputs contradict declared loss ranges or contain
            conflicting duplicate values.
    """
    start_idx, end_idx = get_window_bounds(window)

    # 1. Normalize and validate inputs
    s1_map = normalize_sensor_samples(sensor1_samples, expected_sensor=1)
    s2_map = normalize_sensor_samples(sensor2_samples, expected_sensor=2)

    # Validate loss ledger consistency (reject contradictions between samples and loss)
    if loss_ranges:
        validate_loss_consistency(sensor=1, available_indices=set(s1_map.keys()), loss_ranges=loss_ranges)
        validate_loss_consistency(sensor=2, available_indices=set(s2_map.keys()), loss_ranges=loss_ranges)

    # 2. Determine available and missing indices in window [400*w, 400*w + 399]
    s1_available_in_window: list[Sequence[int]] = []
    s2_available_in_window: list[Sequence[int]] = []

    s1_missing_count = 0
    s2_missing_count = 0

    for idx in range(start_idx, end_idx + 1):
        if idx in s1_map:
            s1_available_in_window.append(s1_map[idx])
        else:
            s1_missing_count += 1

        if idx in s2_map:
            s2_available_in_window.append(s2_map[idx])
        else:
            s2_missing_count += 1

    complete = (s1_missing_count == 0) and (s2_missing_count == 0)

    # 3. Channel statistics (Sensor 1 has 3 channels, Sensor 2 has 2 channels)
    mean_s1, min_s1, max_s1 = compute_channel_stats(
        s1_available_in_window, expected_channels=SENSOR_1_CHANNELS
    )
    mean_s2, min_s2, max_s2 = compute_channel_stats(
        s2_available_in_window, expected_channels=SENSOR_2_CHANNELS
    )

    means: dict[int, list[Optional[float]]] = {1: mean_s1, 2: mean_s2}
    mins: dict[int, list[Optional[int]]] = {1: min_s1, 2: min_s2}
    maxs: dict[int, list[Optional[int]]] = {1: max_s1, 2: max_s2}

    # 4. Sensor 2 magnitude: mean of sqrt(ch1**2 + ch2**2) over available samples
    magnitude = compute_sensor2_magnitude(s2_available_in_window)

    # 5. Sensor 1 Channel 2 Heart Rate
    s1_ch2_map: dict[int, int] = {}
    for idx, vals in s1_map.items():
        if len(vals) > 1:
            s1_ch2_map[idx] = vals[1]
        elif len(vals) == 1:
            s1_ch2_map[idx] = vals[0]

    heart_rate = compute_window_heart_rate(
        window=window,
        sensor1_channel2_map=s1_ch2_map,
        accepted_peaks_history=accepted_peaks_history,
    )

    return WindowResult(
        window=window,
        mode=mode,
        missing_1=s1_missing_count,
        missing_2=s2_missing_count,
        complete=complete,
        mean=means,
        min=mins,
        max=maxs,
        magnitude=magnitude,
        heart_rate=heart_rate,
        session_id=session_id,
    )


class SignalProcessingEngine:
    """Pure, deterministic biosignal processing engine.

    Maintains sequential peak detection history to achieve O(1) amortized window
    processing time without unbounded re-scanning.

    Features:
    - Boundary peak progression: never permanently evaluates a boundary sample
      before its following sample arrives.
    - Session isolation: automatically resets state when invoked with a new session.
    - Loss ledger consistency: validates incoming data against authoritative loss.
    - Decoupled from databases, networking, filesystems, cloud services, and clocks.
    """

    def __init__(
        self,
        session_id: Optional[str] = None,
        mode: str = "offline",
    ) -> None:
        """Initialize the processing engine.

        Args:
            session_id: Optional session identifier.
            mode: 'offline' (device-side) or 'online' (cloud-side).
        """
        self.session_id = session_id
        self.mode = mode

        # Peak detection tracker
        self._accepted_peaks: list[int] = []
        self._last_peak_index: Optional[int] = None
        self._evaluated_through_index: int = -1

    @property
    def accepted_peaks(self) -> list[int]:
        """Return a copy of all accepted peak indices discovered so far."""
        return list(self._accepted_peaks)

    def reset(self, session_id: Optional[str] = None, mode: Optional[str] = None) -> None:
        """Reset internal peak state for a new session or run."""
        if session_id is not None:
            self.session_id = session_id
        if mode is not None:
            self.mode = mode
        self._accepted_peaks.clear()
        self._last_peak_index = None
        self._evaluated_through_index = -1

    def update_peaks(
        self,
        sensor1_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
        up_to_index: int,
    ) -> None:
        """Progressively detect and cache accepted peaks up to up_to_index.

        Guarantees boundary sample safety: an index i is never marked as
        definitively evaluated unless sample i+1 is present in the stream.

        Args:
            sensor1_samples: Mapping of sample index -> channel values.
            up_to_index: The highest sample index to evaluate candidate peaks for.
        """
        s1_map = normalize_sensor_samples(sensor1_samples, expected_sensor=1)
        s1_ch2 = {idx: vals[1] if len(vals) > 1 else vals[0] for idx, vals in s1_map.items()}

        if not s1_ch2:
            return

        max_available = max(s1_ch2.keys())

        # Start evaluation from the next unevaluated index
        start_idx = max(PEAK_LOOKBACK_SAMPLES, self._evaluated_through_index + 1)
        if start_idx > up_to_index:
            return

        last_evaluated = self._evaluated_through_index

        for i in range(start_idx, up_to_index + 1):
            # Boundary safety: to evaluate sample i, sample i+1 must be present.
            # If (i + 1) is beyond the highest sample available in this chunk,
            # sample i cannot be definitively evaluated as a peak or non-peak yet.
            # Stop evaluation here so it is evaluated when the next chunk arrives.
            if i + 1 > max_available:
                break

            if is_peak_at_index(i, s1_ch2, previous_peak_index=self._last_peak_index):
                self._accepted_peaks.append(i)
                self._last_peak_index = i

            last_evaluated = i

        self._evaluated_through_index = max(self._evaluated_through_index, last_evaluated)

    def process_window(
        self,
        window: int,
        sensor1_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
        sensor2_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
        loss_ranges: Optional[Sequence[LossRange]] = None,
        session_id: Optional[str] = None,
    ) -> WindowResult:
        """Process window w using accumulated peak history.

        Args:
            window: Window index w >= 0.
            sensor1_samples: All available Sensor 1 samples (or streaming chunk).
            sensor2_samples: All available Sensor 2 samples (or streaming chunk).
            loss_ranges: Optional sequence of LossRange objects.
            session_id: Optional session ID. If different from current session,
                the engine state is automatically reset for the new session.

        Returns:
            WindowResult with Section 3 metrics.
        """
        # Session isolation: reset state if a new session is supplied
        effective_session_id = session_id if session_id is not None else self.session_id
        if session_id is not None and self.session_id is not None and session_id != self.session_id:
            self.reset(session_id=session_id)
        elif self.session_id is None and session_id is not None:
            self.session_id = session_id

        # Ensure peaks are evaluated through the end of the 5-second interval
        _, interval_end = get_heart_rate_interval(window)
        self.update_peaks(sensor1_samples, up_to_index=interval_end)

        return process_window(
            window=window,
            sensor1_samples=sensor1_samples,
            sensor2_samples=sensor2_samples,
            loss_ranges=loss_ranges,
            session_id=effective_session_id,
            mode=self.mode,
            accepted_peaks_history=self._accepted_peaks,
        )

    def process_windows(
        self,
        windows: Sequence[int],
        sensor1_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
        sensor2_samples: Union[Mapping[int, Sequence[int]], Sequence[Sample]],
        loss_ranges: Optional[Sequence[LossRange]] = None,
        session_id: Optional[str] = None,
    ) -> list[WindowResult]:
        """Process multiple windows in chronological order.

        Args:
            windows: Sequence of window indices.
            sensor1_samples: All available Sensor 1 samples.
            sensor2_samples: All available Sensor 2 samples.
            loss_ranges: Optional sequence of LossRange objects.
            session_id: Optional session identifier.

        Returns:
            List of WindowResult objects corresponding to windows.
        """
        results: list[WindowResult] = []
        for w in sorted(windows):
            res = self.process_window(
                window=w,
                sensor1_samples=sensor1_samples,
                sensor2_samples=sensor2_samples,
                loss_ranges=loss_ranges,
                session_id=session_id,
            )
            results.append(res)
        return results
