"""Tests for MayaCardio Pure Deterministic Signal Processing Engine (Section 3).

Comprehensive unit and integration tests covering:
1. Exact window boundaries (indices 399, 400, 799).
2. Complete windows for both sensors.
3. Partial windows and known loss ranges.
4. Entirely missing sensor windows.
5. Correct channel statistics and empty-channel behavior (nulls).
6. Sensor 2 magnitude calculation.
7. Peak detection exact threshold boundaries (x[i] == 0.5 * max vs > 0.5 * max).
8. Strict neighbour comparisons (equality vs strictly greater).
9. Missing preceding samples and missing following samples.
10. Refractory distance: exactly 100 samples vs less than 100.
11. Heart rate calculation for known peak intervals.
12. The w < 4 null rule.
13. Missing data anywhere in the 5-second heart-rate interval.
14. Fewer than two peaks yielding null heart rate.
15. Rounding to one decimal place.
16. Synthetic signal transitions at 60, 90, and 72 bpm with SignalGenerator.
17. Determinism and idempotency under repeated calls.
18. Documented ambiguities (Sensor 2 availability impact on ECG heart rate).
"""

from __future__ import annotations

import math
from typing import Dict, List, Tuple
import pytest

from biosignal_mock.signal import SignalGenerator
from pipeline.processing import (
    HEART_RATE_MIN_PEAKS,
    HEART_RATE_MIN_WINDOW,
    PEAK_LOOKBACK_SAMPLES,
    PEAK_REFRACTORY_SAMPLES,
    SAMPLING_RATE_HZ,
    WINDOW_SAMPLES,
    ChannelStats,
    LossRange,
    Sample,
    SignalProcessingEngine,
    WindowResult,
    calculate_heart_rate_from_peaks,
    compute_channel_stats,
    compute_sensor2_magnitude,
    compute_window_heart_rate,
    detect_peaks_in_range,
    get_heart_rate_interval,
    get_window_bounds,
    is_peak_at_index,
    process_window,
)


# ==============================================================================
# 1. Window Boundaries
# ==============================================================================

def test_window_boundaries_exact_indices() -> None:
    """Verify window w sample index intervals [400*w, 400*w + 399]."""
    # Window 0: 0 through 399
    start0, end0 = get_window_bounds(0)
    assert start0 == 0
    assert end0 == 399
    assert end0 - start0 + 1 == 400

    # Window 1: 400 through 799
    start1, end1 = get_window_bounds(1)
    assert start1 == 400
    assert end1 == 799
    assert end1 - start1 + 1 == 400

    # Window 2: 800 through 1199
    start2, end2 = get_window_bounds(2)
    assert start2 == 800
    assert end2 == 1199

    # Negative window raises ValueError
    with pytest.raises(ValueError):
        get_window_bounds(-1)


def test_window_boundary_sample_isolation() -> None:
    """Verify samples at 399, 400, 799, 800 strictly belong to correct windows."""
    s1_samples: dict[int, tuple[int, int, int]] = {
        399: (100, 200, 300),
        400: (400, 500, 600),
        799: (700, 800, 900),
        800: (1000, 1100, 1200),
    }
    s2_samples: dict[int, tuple[int, int]] = {
        399: (10, 20),
        400: (30, 40),
        799: (50, 60),
        800: (70, 80),
    }

    # Window 0 should only contain index 399 (1 sample available, 399 missing)
    res0 = process_window(0, s1_samples, s2_samples)
    assert res0.missing_1 == 399
    assert res0.missing_2 == 399
    assert res0.min_1 == [100, 200, 300]
    assert res0.max_1 == [100, 200, 300]

    # Window 1 should contain indices 400 and 799 (2 samples available, 398 missing)
    res1 = process_window(1, s1_samples, s2_samples)
    assert res1.missing_1 == 398
    assert res1.missing_2 == 398
    assert res1.min_1 == [400, 500, 600]
    assert res1.max_1 == [700, 800, 900]

    # Window 2 should only contain index 800 (1 sample available, 399 missing)
    res2 = process_window(2, s1_samples, s2_samples)
    assert res2.missing_1 == 399
    assert res2.missing_2 == 399
    assert res2.min_1 == [1000, 1100, 1200]


# ==============================================================================
# 2. Complete Windows
# ==============================================================================

def test_complete_window_both_sensors() -> None:
    """A window with all 400 samples present for both sensors must be complete."""
    s1 = {i: (1000, 2000, 3000) for i in range(400)}
    s2 = {i: (400000, 150000) for i in range(400)}

    res = process_window(0, s1, s2, mode="online", session_id="sess_complete")

    assert res.window == 0
    assert res.mode == "online"
    assert res.session_id == "sess_complete"
    assert res.missing_1 == 0
    assert res.missing_2 == 0
    assert res.complete is True

    # Dict access and serialization
    d = res.to_dict()
    assert d["complete"] is True
    assert d["missing_1"] == 0
    assert d["missing_2"] == 0
    assert res["complete"] is True


# ==============================================================================
# 3. Partial Windows & Missing Counts
# ==============================================================================

def test_partial_window_missing_accounting() -> None:
    """Missing counts must accurately reflect unavailable indices."""
    # Sensor 1 missing indices 100..149 (50 samples)
    s1 = {i: (10, 20, 30) for i in range(400) if not (100 <= i < 150)}
    # Sensor 2 missing indices 350..399 (50 samples)
    s2 = {i: (40, 50) for i in range(400) if not (350 <= i < 400)}

    loss_ranges = [
        LossRange(sensor=1, from_index=100, to_index=149, reason="buffer_overflow"),
        LossRange(sensor=2, from_index=350, to_index=399, reason="network_gap"),
    ]

    res = process_window(0, s1, s2, loss_ranges=loss_ranges)

    assert res.missing_1 == 50
    assert res.missing_2 == 50
    assert res.complete is False


# ==============================================================================
# 4. Entirely Missing Sensor Windows
# ==============================================================================

def test_entirely_missing_sensor_windows() -> None:
    """Empty sensor windows must report 400 missing, complete=False, and null stats."""
    # Sensor 1 completely missing, Sensor 2 complete
    s1_empty: dict[int, tuple[int, int, int]] = {}
    s2_full = {i: (300, 400) for i in range(400)}

    res = process_window(0, s1_empty, s2_full)
    assert res.missing_1 == 400
    assert res.missing_2 == 0
    assert res.complete is False
    assert res.mean_1 == [None, None, None]
    assert res.min_1 == [None, None, None]
    assert res.max_1 == [None, None, None]

    # Sensor 2 stats should still be calculated
    assert res.min_2 == [300, 400]
    assert res.max_2 == [300, 400]
    assert res.mean_2 == [300.0, 400.0]
    # Magnitude: sqrt(300^2 + 400^2) = 500.0
    assert res.magnitude == 500.0

    # Now both sensors completely missing
    res_both_empty = process_window(0, {}, {})
    assert res_both_empty.missing_1 == 400
    assert res_both_empty.missing_2 == 400
    assert res_both_empty.complete is False
    assert res_both_empty.mean_1 == [None, None, None]
    assert res_both_empty.mean_2 == [None, None]
    assert res_both_empty.min_1 == [None, None, None]
    assert res_both_empty.min_2 == [None, None]
    assert res_both_empty.max_1 == [None, None, None]
    assert res_both_empty.max_2 == [None, None]
    assert res_both_empty.magnitude is None
    assert res_both_empty.heart_rate is None


# ==============================================================================
# 5. Channel Statistics & Rounding
# ==============================================================================

def test_channel_statistics_rounding_and_types() -> None:
    """Mean must be rounded to 1 decimal place; min and max must be signed integers."""
    # S1 values: 3 samples
    # ch1: [10, 11, 13] -> sum = 34 / 3 = 11.3333... -> 11.3
    # ch2: [-100, 0, 50] -> sum = -50 / 3 = -16.6666... -> -16.7
    # ch3: [1000, 1000, 1001] -> sum = 3001 / 3 = 1000.3333... -> 1000.3
    samples = [
        (10, -100, 1000),
        (11, 0, 1000),
        (13, 50, 1001),
    ]
    means, mins, maxs = compute_channel_stats(samples, expected_channels=3)

    assert means == [11.3, -16.7, 1000.3]
    assert mins == [10, -100, 1000]
    assert maxs == [13, 50, 1001]


# ==============================================================================
# 6. Sensor 2 Magnitude
# ==============================================================================

def test_sensor2_magnitude_exact_calculation() -> None:
    """Verify magnitude = mean of sqrt(ch1**2 + ch2**2) rounded to 1 decimal place."""
    # Sample 1: 3, 4 -> sqrt(9 + 16) = 5.0
    # Sample 2: 6, 8 -> sqrt(36 + 64) = 10.0
    # Sample 3: 1, 2 -> sqrt(1 + 4) = sqrt(5) = 2.2360679...
    # Mean = (5.0 + 10.0 + 2.2360679) / 3 = 17.2360679 / 3 = 5.74535... -> 5.7
    samples = [(3, 4), (6, 8), (1, 2)]
    mag = compute_sensor2_magnitude(samples)
    assert mag == 5.7

    # Empty samples -> None
    assert compute_sensor2_magnitude([]) is None


# ==============================================================================
# 7. Exact Peak Threshold Boundaries
# ==============================================================================

def test_peak_exact_threshold_boundary() -> None:
    """Condition 3: x[i] > 0.5 * max(x[i-800:i]) requires STRICT inequality."""
    # Construct an 802-sample signal for i=800:
    # Lookback 0..799 has maximum value 2000.
    # 0.5 * max = 1000.0.
    # If x[800] == 1000: NOT a peak.
    # If x[800] == 1001: IS a peak.
    signal: dict[int, int] = {j: 500 for j in range(802)}
    signal[400] = 2000  # Sets max(x[0:800]) = 2000
    signal[799] = 500
    signal[801] = 500

    # Exact threshold boundary: x[800] == 1000
    signal[800] = 1000
    assert is_peak_at_index(800, signal) is False, "Equality with 0.5*max must be rejected"

    # Strictly above threshold: x[800] == 1001
    signal[800] = 1001
    assert is_peak_at_index(800, signal) is True, "Strictly greater than 0.5*max must be accepted"

    # Below threshold: x[800] == 999
    signal[800] = 999
    assert is_peak_at_index(800, signal) is False, "Below 0.5*max must be rejected"


# ==============================================================================
# 8. Strict Neighbour Comparisons
# ==============================================================================

def test_strict_neighbour_comparisons() -> None:
    """Condition 2: x[i] > x[i-1] and x[i] > x[i+1] requires STRICT inequality."""
    signal = {j: 500 for j in range(802)}
    signal[400] = 2000  # Max lookback 2000 -> threshold 1000

    # Equal to left neighbor: x[799] == 1500, x[800] == 1500, x[801] == 500
    signal[799] = 1500
    signal[800] = 1500
    signal[801] = 500
    assert is_peak_at_index(800, signal) is False, "Plateau with left neighbor must be rejected"

    # Equal to right neighbor: x[799] == 500, x[800] == 1500, x[801] == 1500
    signal[799] = 500
    signal[800] = 1500
    signal[801] = 1500
    assert is_peak_at_index(800, signal) is False, "Plateau with right neighbor must be rejected"

    # Strictly larger than both: x[799] == 1499, x[800] == 1500, x[801] == 1499
    signal[799] = 1499
    signal[800] = 1500
    signal[801] = 1499
    assert is_peak_at_index(800, signal) is True, "Strictly larger than both must be accepted"


# ==============================================================================
# 9. Missing Preceding and Following Samples
# ==============================================================================

def test_missing_preceding_or_following_samples_rejects_peak() -> None:
    """Condition 1: All 800 preceding samples and sample i+1 must be available."""
    signal = {j: 500 for j in range(802)}
    signal[400] = 2000
    signal[800] = 1500

    assert is_peak_at_index(800, signal) is True

    # Missing immediate following sample i+1
    signal_no_next = dict(signal)
    del signal_no_next[801]
    assert is_peak_at_index(800, signal_no_next) is False

    # Missing sample i-1
    signal_no_prev = dict(signal)
    del signal_no_prev[799]
    assert is_peak_at_index(800, signal_no_prev) is False

    # Missing sample i-800 (boundary of the 800 preceding samples)
    signal_no_first = dict(signal)
    del signal_no_first[0]
    assert is_peak_at_index(800, signal_no_first) is False

    # Missing middle lookback sample
    signal_no_mid = dict(signal)
    del signal_no_mid[350]
    assert is_peak_at_index(800, signal_no_mid) is False

    # Index i < 800 can NEVER have 800 preceding samples
    assert is_peak_at_index(799, signal) is False
    assert is_peak_at_index(0, signal) is False


# ==============================================================================
# 10. Refractory Distance: Exactly 100 vs Less Than 100
# ==============================================================================

def test_refractory_distance_exact_100_and_99() -> None:
    """Condition 4: At least 100 samples separate it from previous accepted peak."""
    signal = {j: 500 for j in range(1200)}
    signal[400] = 2000
    signal[800] = 1800  # First peak at 800

    assert is_peak_at_index(800, signal, previous_peak_index=None) is True

    # Candidate at 899 (separation = 99 < 100): REJECTED due to refractory period
    signal[899] = 1800
    assert is_peak_at_index(899, signal, previous_peak_index=800) is False
    # Reset 899 back to baseline so it does not interfere as neighbor
    signal[899] = 500

    # Candidate at 900 (separation = 100 >= 100): ACCEPTED
    signal[900] = 1800
    assert is_peak_at_index(900, signal, previous_peak_index=800) is True
    # Reset 900 back to baseline
    signal[900] = 500

    # Candidate at 901 (separation = 101 >= 100): ACCEPTED
    signal[901] = 1800
    assert is_peak_at_index(901, signal, previous_peak_index=800) is True


# ==============================================================================
# 11. Heart Rate Calculation for Known Peak Intervals
# ==============================================================================

def test_heart_rate_formula_known_intervals() -> None:
    """Verify heart_rate = 60 / mean(gaps_in_seconds) rounded to 1 decimal place."""
    # Exactly 60.0 bpm: gaps of 400 samples (1.000 s)
    peaks_60 = [1000, 1400, 1800]
    assert calculate_heart_rate_from_peaks(peaks_60) == 60.0

    # Exactly 90.0 bpm: 4 peaks separated by [267, 266, 267] samples
    # Total samples = 800 over 3 gaps = 266.6667 samples/gap = 0.666667 s
    # 60 / 0.666667 = 90.0 bpm
    peaks_90 = [1000, 1267, 1533, 1800]
    assert calculate_heart_rate_from_peaks(peaks_90) == 90.0

    # Exactly 72.0 bpm: 4 peaks separated by [333, 334, 333] samples
    # Total samples = 1000 over 3 gaps = 333.3333 samples/gap = 0.833333 s
    # 60 / 0.833333 = 72.0 bpm
    peaks_72 = [1000, 1333, 1667, 2000]
    assert calculate_heart_rate_from_peaks(peaks_72) == 72.0

    # Irregular rhythm test: gaps [350, 450] -> mean gap 400 -> 60.0 bpm
    peaks_irregular = [1000, 1350, 1800]
    assert calculate_heart_rate_from_peaks(peaks_irregular) == 60.0


# ==============================================================================
# 12. w < 4 Null Heart Rate Rule
# ==============================================================================

def test_heart_rate_w_less_than_4_rule() -> None:
    """Heart rate must be null for w < 4 even if complete samples and peaks exist."""
    generator = SignalGenerator(seed=7)
    s1 = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(2000)}
    s2 = {i: (400000, 150000) for i in range(2000)}

    for w in [0, 1, 2, 3]:
        res = process_window(w, s1, s2)
        assert res.heart_rate is None, f"Window {w} (< 4) must return heart_rate = null"


# ==============================================================================
# 13. Missing Data in the 5-Second Interval Voids Heart Rate
# ==============================================================================

def test_missing_data_in_5s_interval_nulls_heart_rate() -> None:
    """If ANY sample in [400(w-4), 400(w+1)-1] is missing, heart_rate must be null."""
    generator = SignalGenerator(seed=7)
    # Window 4 interval: 400*(4-4)=0 to 400*(5)-1=1999 (2,000 samples)
    s1_full = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(2001)}
    s2_full = {i: (400000, 150000) for i in range(2001)}

    # Baseline window 4 should have valid heart rate
    res_valid = process_window(4, s1_full, s2_full)
    assert res_valid.heart_rate is not None

    # Missing sample at start of 5s interval (index 0)
    s1_miss_start = dict(s1_full)
    del s1_miss_start[0]
    assert process_window(4, s1_miss_start, s2_full).heart_rate is None

    # Missing sample in middle of 5s interval (index 1000)
    s1_miss_mid = dict(s1_full)
    del s1_miss_mid[1000]
    assert process_window(4, s1_miss_mid, s2_full).heart_rate is None

    # Missing sample at end of 5s interval (index 1999)
    s1_miss_end = dict(s1_full)
    del s1_miss_end[1999]
    assert process_window(4, s1_miss_end, s2_full).heart_rate is None

    # Missing sample outside the 5s interval (index 2000) does NOT void window 4
    # (provided sample 2000 is not needed as sample i+1 for a peak at 1999)
    s1_miss_outside = dict(s1_full)
    del s1_miss_outside[2000]
    assert process_window(4, s1_miss_outside, s2_full).heart_rate is not None


# ==============================================================================
# 14. Fewer Than Two Peaks Voids Heart Rate
# ==============================================================================

def test_fewer_than_two_peaks_nulls_heart_rate() -> None:
    """Heart rate must be null if fewer than 2 accepted peaks are present."""
    # Create flat signal in window 4 (0 peaks)
    s1_flat = {i: (1000, 1000, 1500) for i in range(2001)}
    s2 = {i: (400000, 150000) for i in range(2001)}
    assert process_window(4, s1_flat, s2).heart_rate is None

    # Create signal with exactly 1 peak (at index 1000)
    s1_one_peak = {i: (1000, 1000, 1500) for i in range(2001)}
    s1_one_peak[1000] = (1000, 3000, 1500)
    assert process_window(4, s1_one_peak, s2).heart_rate is None


# ==============================================================================
# 15. Rounding to One Decimal Place
# ==============================================================================

def test_rounding_to_one_decimal_place() -> None:
    """Verify that all floating point results round to exactly 1 decimal place."""
    # Heart rate: 24000 / 355 = 67.60563... -> 67.6
    peaks = [1000, 1355]
    hr = calculate_heart_rate_from_peaks(peaks)
    assert hr == 67.6

    # Magnitude: sqrt(100**2 + 200**2) = 223.60679... -> 223.6
    mag = compute_sensor2_magnitude([(100, 200)])
    assert mag == 223.6


# ==============================================================================
# 16. Synthetic Signal Transitions (60, 90, 72 bpm)
# ==============================================================================

def test_synthetic_signal_transitions_with_signal_generator() -> None:
    """Verify heart rate transitions at 60 bpm (min 0-2), 90 bpm (min 2-4), 72 bpm (min 4-6)."""
    generator = SignalGenerator(seed=7)
    engine = SignalProcessingEngine(session_id="test_transitions", mode="offline")

    # Generate samples covering 6 minutes (144,000 samples)
    # We test representative windows in each steady-state region:
    # 1. 60 bpm steady-state: Window 10 (10 seconds)
    # 2. 90 bpm steady-state: Window 130 (130 seconds = 2 min 10 s)
    # 3. 72 bpm steady-state: Window 250 (250 seconds = 4 min 10 s)

    # Window 10: needs samples 0..4400
    w10_end = 400 * 11
    s1_w10 = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(w10_end + 1)}
    s2_w10 = {i: (400000, 150000) for i in range(w10_end + 1)}
    res_w10 = engine.process_window(10, s1_w10, s2_w10)
    assert res_w10.heart_rate == 60.0

    # Window 130: 90 bpm region (samples around 52,000)
    # Needs lookback from 400*(130-4) - 800 = 50400 - 800 = 49600 through 400*131 = 52400
    w130_start = 400 * (130 - 4) - 800
    w130_end = 400 * (130 + 1)
    s1_w130 = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(w130_start, w130_end + 1)}
    s2_w130 = {i: (400000, 150000) for i in range(w130_start, w130_end + 1)}
    engine.reset()
    res_w130 = engine.process_window(130, s1_w130, s2_w130)
    assert res_w130.heart_rate == 90.0

    # Window 250: 72 bpm region (samples around 100,000)
    w250_start = 400 * (250 - 4) - 800
    w250_end = 400 * (250 + 1)
    s1_w250 = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(w250_start, w250_end + 1)}
    s2_w250 = {i: (400000, 150000) for i in range(w250_start, w250_end + 1)}
    engine.reset()
    res_w250 = engine.process_window(250, s1_w250, s2_w250)
    assert res_w250.heart_rate == 72.0


# ==============================================================================
# 17. Determinism & Idempotency
# ==============================================================================

def test_engine_determinism_and_idempotency() -> None:
    """Repeated runs on identical inputs must produce strictly identical results."""
    generator = SignalGenerator(seed=42)
    s1 = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(2401)}
    s2 = {i: (400000, 150000) for i in range(2401)}

    engine1 = SignalProcessingEngine(session_id="idemp", mode="offline")
    engine2 = SignalProcessingEngine(session_id="idemp", mode="offline")

    res1 = engine1.process_window(4, s1, s2)
    res2 = engine2.process_window(4, s1, s2)
    res3 = process_window(4, s1, s2, session_id="idemp", mode="offline")

    assert res1 == res2 == res3
    assert res1.to_dict() == res2.to_dict() == res3.to_dict()


# ==============================================================================
# 18. Exact Section 3 Heart Rate Rule: Sensor 1 Channel 2 Authority
# ==============================================================================

def test_section3_heart_rate_sensor1_authority() -> None:
    """Section 3: heart_rate is derived from Sensor 1 Channel 2.

    Missing Sensor 2 samples in the 5-second interval do not invalidate
    the Sensor 1 ECG heart rate, while any missing Sensor 1 sample in that
    interval invalidates the rate.
    """
    generator = SignalGenerator(seed=7)
    s1_full = {i: (1000, generator.generate_sensor1_channel2(i), 1500) for i in range(2001)}
    # Sensor 2 has missing samples in the 5-second interval
    s2_partial = {i: (400000, 150000) for i in range(2001) if not (500 <= i <= 600)}

    # Sensor 1 ECG rate is computed normally
    res = process_window(4, s1_full, s2_partial)
    assert res.heart_rate == 60.0
    assert res.missing_2 == 0  # In window 4 (1600..1999), S2 is complete

    # If Sensor 1 is missing a sample in that 5-second interval, heart_rate must be None
    s1_partial = dict(s1_full)
    del s1_partial[550]
    assert process_window(4, s1_partial, s2_partial).heart_rate is None


# ==============================================================================
# 19. Data Model Validation & Schemas
# ==============================================================================

def test_sample_and_loss_range_validation() -> None:
    """Explicitly typed Sample and LossRange prevent invalid sensor/channel inputs."""
    # Valid samples
    s1 = Sample(sensor=1, index=0, values=(100, 200, 300))
    s2 = Sample(sensor=2, index=0, values=(100, 200))
    assert s1.sensor == 1 and len(s1.values) == 3
    assert s2.sensor == 2 and len(s2.values) == 2

    # Invalid sensor ID
    with pytest.raises(ValueError):
        Sample(sensor=3, index=0, values=(100, 200))

    # Negative index
    with pytest.raises(ValueError):
        Sample(sensor=1, index=-1, values=(100, 200, 300))

    # Wrong channel counts
    with pytest.raises(ValueError):
        Sample(sensor=1, index=0, values=(100, 200))  # Sensor 1 requires 3
    with pytest.raises(ValueError):
        Sample(sensor=2, index=0, values=(100, 200, 300))  # Sensor 2 requires 2

    # Loss range contains helper
    lr = LossRange(sensor=1, from_index=400, to_index=799, reason="test")
    assert lr.contains(400) is True
    assert lr.contains(799) is True
    assert lr.contains(399) is False
    assert lr.contains(800) is False

    # Unbounded loss range
    lr_unbounded = LossRange(sensor=2, from_index=1000, to_index=None)
    assert lr_unbounded.contains(1000) is True
    assert lr_unbounded.contains(999999) is True
    assert lr_unbounded.contains(999) is False


# ==============================================================================
# 20. Regression: Boundary Peak Progressive Chunk Evaluation (Defect 1)
# ==============================================================================

def test_boundary_peak_progressive_chunk_evaluation() -> None:
    """A peak at boundary 400(w+1)-1 must be accepted when the following chunk arrives."""
    eng = SignalProcessingEngine(session_id="sess_boundary")

    # Chunk 1 (Window 4): samples 0..1999
    # Peak at index 1999: needs index 2000 to be evaluated
    s_w4 = {i: (1000, 500, 1500) for i in range(2000)}
    s_w4[400] = (1000, 2000, 1500)
    s_w4[1000] = (1000, 1800, 1500)
    s_w4[1999] = (1000, 1800, 1500)  # Boundary peak
    s2 = {i: (400000, 150000) for i in range(2401)}

    eng.process_window(4, s_w4, s2)
    # When window 4 was processed, index 2000 was unavailable, so 1999 could not be accepted yet
    assert 1999 not in eng.accepted_peaks

    # Chunk 2 (Window 5): now includes sample 2000 through 2400
    s_all = dict(s_w4)
    s_all.update({i: (1000, 500, 1500) for i in range(2000, 2401)})
    eng.process_window(5, s_all, s2)

    # With sample 2000 now available, boundary sample 1999 must be evaluated and accepted
    assert 1999 in eng.accepted_peaks, "Boundary peak at index 1999 was permanently skipped!"


# ==============================================================================
# 21. Regression: Stateless/Stateful Parity on Adversarial Waveform (Defect 2)
# ==============================================================================

def test_stateless_stateful_parity_adversarial_waveform() -> None:
    """Stateless process_window() and stateful engine must yield identical heart rates."""
    # Waveform with candidate peaks near the boundary of the lookback:
    # Peaks at 1070, 1130 (suppressed by 1070), 1210 (accepted), 1610 (accepted)
    sig = {i: (1000, 500, 1500) for i in range(4000)}
    sig[400] = (1000, 2000, 1500)
    sig[1070] = (1000, 1800, 1500)
    sig[1130] = (1000, 1800, 1500)
    sig[1210] = (1000, 1800, 1500)
    sig[1610] = (1000, 1800, 1500)
    s2 = {i: (400000, 150000) for i in range(4000)}

    eng = SignalProcessingEngine(session_id="parity_test")
    r_stateful = eng.process_window(7, sig, s2).heart_rate
    r_stateless = process_window(7, sig, s2).heart_rate

    assert r_stateless is not None, "Stateless processing falsely returned None!"
    assert r_stateful == r_stateless, f"Stateless ({r_stateless}) != Stateful ({r_stateful})"


# ==============================================================================
# 22. Regression: Session Isolation Prevents State Bleed (Defect 3)
# ==============================================================================

def test_session_isolation_prevents_state_bleed() -> None:
    """Engine must not leak peaks from a previous session into a new session."""
    g = SignalGenerator(seed=7)
    s1_a = {i: (1000, g.generate_sensor1_channel2(i), 1500) for i in range(5000)}
    s2 = {i: (400000, 150000) for i in range(5000)}

    eng = SignalProcessingEngine(session_id="session_A")
    res_a = eng.process_window(10, s1_a, s2, session_id="session_A")
    assert res_a.heart_rate == 60.0

    # Completely flat signal (zero peaks) from a different session
    s1_flat = {i: (1000, 500, 1500) for i in range(5000)}
    res_b = eng.process_window(10, s1_flat, s2, session_id="session_B")

    # Session B must NOT inherit session A's peaks!
    assert res_b.heart_rate is None, "State bleed: Session B inherited peaks from Session A!"


# ==============================================================================
# 23. Regression: Loss Ledger Contradiction Rejection (Defect 4)
# ==============================================================================

def test_loss_ranges_contradiction_rejected() -> None:
    """Engine must reject contradictory inputs where a sample is present in a declared loss range."""
    # Sample index 150 is provided in samples, but declared lost in loss_ranges
    s1 = {150: (10, 20, 30)}
    s2 = {}
    loss_conflict = [LossRange(sensor=1, from_index=100, to_index=200, reason="buffer_overflow")]

    with pytest.raises(ValueError, match="Contradictory data on sensor 1"):
        process_window(0, s1, s2, loss_ranges=loss_conflict)

    # Open-ended loss range contradiction
    loss_unbounded = [LossRange(sensor=1, from_index=100, to_index=None, reason="network_cut")]
    with pytest.raises(ValueError, match="Contradictory data on sensor 1"):
        process_window(0, s1, s2, loss_ranges=loss_unbounded)


# ==============================================================================
# 24. Regression: Conflicting Duplicate Sample Records Rejection
# ==============================================================================

def test_conflicting_duplicate_sample_records_rejected() -> None:
    """Duplicate samples with conflicting channel values must be rejected."""
    # Sequence with conflicting duplicates for index 10
    sample_seq = [
        Sample(sensor=1, index=10, values=(100, 200, 300)),
        Sample(sensor=1, index=10, values=(999, 999, 999)),
    ]
    with pytest.raises(ValueError, match="Conflicting duplicate sample records"):
        process_window(0, sample_seq, {})

    # Identical duplicates are idempotent and accepted
    identical_seq = [
        Sample(sensor=1, index=10, values=(100, 200, 300)),
        Sample(sensor=1, index=10, values=(100, 200, 300)),
    ]
    res = process_window(0, identical_seq, {})
    assert res.missing_1 == 399
    assert res.min_1 == [100, 200, 300]


# ==============================================================================
# 25. Regression: Repeated, Skipped, and Out-of-Order Window Processing
# ==============================================================================

def test_repeated_and_out_of_order_window_calls() -> None:
    """Engine produces identical window results regardless of call order."""
    g = SignalGenerator(seed=7)
    s1 = {i: (1000, g.generate_sensor1_channel2(i), 1500) for i in range(5000)}
    s2 = {i: (400000, 150000) for i in range(5000)}

    eng_seq = SignalProcessingEngine(session_id="order_test")
    # Sequential run: windows 4, 5, 6, 7, 8, 9, 10
    seq_results = {w: eng_seq.process_window(w, s1, s2).heart_rate for w in range(4, 11)}

    # Out-of-order run: window 10, then 5, then 8, then 4, then 10 again
    eng_ooo = SignalProcessingEngine(session_id="order_test")
    r10_1 = eng_ooo.process_window(10, s1, s2).heart_rate
    r5 = eng_ooo.process_window(5, s1, s2).heart_rate
    r8 = eng_ooo.process_window(8, s1, s2).heart_rate
    r4 = eng_ooo.process_window(4, s1, s2).heart_rate
    r10_2 = eng_ooo.process_window(10, s1, s2).heart_rate

    assert r10_1 == seq_results[10]
    assert r5 == seq_results[5]
    assert r8 == seq_results[8]
    assert r4 == seq_results[4]
    assert r10_1 == r10_2, "Repeated invocation yielded different result!"


# ==============================================================================
# 26. Regression: Open-Ended Loss Range Accounting
# ==============================================================================

def test_open_ended_loss_range_accounting() -> None:
    """Open-ended loss ranges (to_index=None) correctly declare all subsequent indices lost."""
    loss_open = [LossRange(sensor=1, from_index=200, to_index=None, reason="network_loss")]
    # Valid available samples only before index 200 (indices 0..199)
    s1 = {i: (10, 20, 30) for i in range(200)}
    s2 = {i: (10, 20) for i in range(800)}

    # Window 0 spans 0..399. Indices 0..199 are available (200 samples), 200..399 missing (200 samples)
    res0 = process_window(0, s1, s2, loss_ranges=loss_open)
    assert res0.missing_1 == 200
    assert res0.missing_2 == 0
    assert res0.complete is False

    # Window 1 spans 400..799. All 400 samples are covered by open-ended loss (0 available)
    res1 = process_window(1, s1, s2, loss_ranges=loss_open)
    assert res1.missing_1 == 400
    assert res1.missing_2 == 0
    assert res1.complete is False


# ==============================================================================
# 27. Regression: Boundary Peaks Exactly 100 Samples Apart
# ==============================================================================

def test_boundary_peaks_exactly_100_samples_apart() -> None:
    """Peaks located across 1-second boundaries separated by exactly 100 samples are both accepted."""
    # Window boundary is at 799/800. Place peak 1 at 750, peak 2 at 850 (diff = 100)
    sig = {i: 500 for i in range(1201)}
    sig[200] = 2000  # Sets lookback max
    sig[850] = 1800  # Peak 2 at index 850
    sig[849] = 500
    sig[851] = 500

    # Test peak 2 at 850 with previous peak at 750 (difference = 100)
    assert is_peak_at_index(850, sig, previous_peak_index=750) is True

    # Test peak 2 at 849 with previous peak at 750 (difference = 99 < 100) -> Rejected
    sig[849] = 1800
    sig[848] = 500
    sig[850] = 500
    assert is_peak_at_index(849, sig, previous_peak_index=750) is False
