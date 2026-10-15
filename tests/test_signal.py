"""Unit tests for biosignal_mock/signal.py (Increment C).

Verifies REQ-A-03, REQ-A-04, REQ-A-06:
- Sensor 1 channel scaling: Ch1 = round(Ch2 * 0.6), Ch3 = round(Ch2 * 1.3)
- Sensor 2 values stay near 400,000 and 150,000
- Heart rate transitions: 60 bpm (min 0-2), 90 bpm (min 2-4), 72 bpm (min 4-6)
- Evaluates against Section 3 4-condition ECG peak detection rules
- Deterministic reproducibility with --seed
"""

import pytest
from biosignal_mock.signal import (
    SAMPLING_RATE,
    SignalGenerator,
)
from shared.protocol import MAX_INT24, MIN_INT24


def test_signal_channel_scaling() -> None:
    """Verifies REQ-A-04: Channel 1 and 3 are exact linear scalings of Channel 2."""
    gen = SignalGenerator(seed=7)
    for idx in range(0, 2000, 10):
        sample = gen.generate_sensor1_sample(idx)
        ch1, ch2, ch3 = sample
        assert ch1 == int(round(ch2 * 0.6))
        assert ch3 == int(round(ch2 * 1.3))
        for v in sample:
            assert MIN_INT24 <= v <= MAX_INT24


def test_sensor2_baselines() -> None:
    """Verifies REQ-A-04: Sensor 2 Channel 1 near 400,000 and Channel 2 near 150,000."""
    gen = SignalGenerator(seed=7)
    for idx in range(0, 10000, 50):
        ch1, ch2 = gen.generate_sensor2_sample(idx)
        assert 380_000 <= ch1 <= 420_000
        assert 140_000 <= ch2 <= 160_000
        assert MIN_INT24 <= ch1 <= MAX_INT24
        assert MIN_INT24 <= ch2 <= MAX_INT24


def test_deterministic_seed_reproducibility() -> None:
    """Verifies REQ-A-06: Same seed produces bit-for-bit identical outputs."""
    gen1 = SignalGenerator(seed=42)
    gen2 = SignalGenerator(seed=42)

    for idx in range(500):
        assert gen1.generate_sensor1_sample(idx) == gen2.generate_sensor1_sample(idx)
        assert gen1.generate_sensor2_sample(idx) == gen2.generate_sensor2_sample(idx)


def test_ecg_peak_detection_and_rate_cycle() -> None:
    """Verifies REQ-A-03: Peaks satisfy Section 3 rules and match 60, 90, 72 bpm sequence."""
    gen = SignalGenerator(seed=7)

    # Helper function implementing the exact Section 3 peak detection rules:
    # 1. 800 samples before it, and sample after it, are available
    # 2. x[i] > x[i-1] and x[i] > x[i+1]
    # 3. x[i] > 0.5 * max(x[i-800:i-1])
    # 4. i - i_prev >= 100
    def detect_peaks_in_range(start_idx: int, end_idx: int) -> list[int]:
        # Pre-generate channel 2 values with 800 history margin
        gen_start = max(0, start_idx - 800)
        gen_end = end_idx + 2
        values = {
            i: gen.generate_sensor1_channel2(i)
            for i in range(gen_start, gen_end)
        }

        detected_peaks: list[int] = []
        last_peak = -1000

        for i in range(start_idx, end_idx):
            if i < 800:
                continue
            x_i = values[i]
            # Rule 2: strict local maximum
            if not (x_i > values[i - 1] and x_i > values[i + 1]):
                continue
            # Rule 3: > 0.5 * max(last 800)
            max_prev = max(values[j] for j in range(i - 800, i))
            if x_i <= 0.5 * max_prev:
                continue
            # Rule 4: refractory period >= 100
            if i - last_peak < 100:
                continue

            detected_peaks.append(i)
            last_peak = i

        return detected_peaks

    # Test Segment 1: Minute 1 (samples 400 * 60 to 400 * 110 = 24,000 to 44,000)
    # Expected rate: 60 bpm -> interval = 400 samples = 1.0s
    peaks_seg1 = detect_peaks_in_range(24_000, 28_000)
    assert len(peaks_seg1) >= 8
    gaps_seg1 = [peaks_seg1[k] - peaks_seg1[k - 1] for k in range(1, len(peaks_seg1))]
    mean_gap_sec_1 = sum(gaps_seg1) / len(gaps_seg1) / SAMPLING_RATE
    bpm_1 = 60.0 / mean_gap_sec_1
    assert pytest.approx(bpm_1, rel=0.01) == 60.0

    # Test Segment 2: Minute 3 (samples 400 * 150 to 400 * 170 = 60,000 to 68,000)
    # Expected rate: 90 bpm -> interval = 266.67 samples = 0.667s
    peaks_seg2 = detect_peaks_in_range(60_000, 64_000)
    assert len(peaks_seg2) >= 12
    gaps_seg2 = [peaks_seg2[k] - peaks_seg2[k - 1] for k in range(1, len(peaks_seg2))]
    mean_gap_sec_2 = sum(gaps_seg2) / len(gaps_seg2) / SAMPLING_RATE
    bpm_2 = 60.0 / mean_gap_sec_2
    assert pytest.approx(bpm_2, rel=0.01) == 90.0

    # Test Segment 3: Minute 5 (samples 400 * 270 to 400 * 290 = 108,000 to 116,000)
    # Expected rate: 72 bpm -> interval = 333.33 samples = 0.833s
    peaks_seg3 = detect_peaks_in_range(108_000, 112_000)
    assert len(peaks_seg3) >= 10
    gaps_seg3 = [peaks_seg3[k] - peaks_seg3[k - 1] for k in range(1, len(peaks_seg3))]
    mean_gap_sec_3 = sum(gaps_seg3) / len(gaps_seg3) / SAMPLING_RATE
    bpm_3 = 60.0 / mean_gap_sec_3
    assert pytest.approx(bpm_3, rel=0.01) == 72.0
