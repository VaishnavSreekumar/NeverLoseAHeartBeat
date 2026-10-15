"""Realistic synthetic ECG/PPG signal generator for MayaCardio biosignal-mock.

Strictly follows Section 6.1:
- Sensor 1:
  - Rate: 400 samples/second.
  - Channel 2: Realistic ECG waveform with one sharp peak per heartbeat.
    Heart rate cycle:
      - Minutes 0–2: 60 bpm (1 beat/s, interval 400 samples)
      - Minutes 2–4: 90 bpm (1.5 beats/s, interval 266.67 samples)
      - Minutes 4–6: 72 bpm (1.2 beats/s, interval 333.33 samples)
      - Repeating cyclically every 6 minutes (360 seconds = 144,000 samples).
  - Channel 1: Channel 2 * 0.6 (rounded to nearest integer).
  - Channel 3: Channel 2 * 1.3 (rounded to nearest integer).
- Sensor 2:
  - Channel 1: Near 400,000, varying slowly (e.g. respiratory modulation).
  - Channel 2: Near 150,000, varying slowly (e.g. PPG blood volume pulse).
- Values fit in signed 24-bit integer range [-8,388,608, 8,388,607].
- Reproducible with deterministic --seed.
"""

from __future__ import annotations

import math
from typing import Tuple

SAMPLING_RATE = 400  # 400 Hz
CYCLE_DURATION_SEC = 360  # 6 minutes per full rate cycle
CYCLE_SAMPLES = CYCLE_DURATION_SEC * SAMPLING_RATE  # 144,000 samples

# Interval boundaries in seconds within the 6-minute cycle
INTERVAL_1_END_SEC = 120  # 2 minutes = 60 bpm
INTERVAL_2_END_SEC = 240  # 4 minutes = 90 bpm
INTERVAL_3_END_SEC = 360  # 6 minutes = 72 bpm


class SignalGenerator:
    """Deterministic generator for synthetic ECG (Sensor 1) and PPG (Sensor 2)."""

    def __init__(self, seed: int = 7) -> None:
        self.seed = seed
        # Seed phase offset
        self.phase_offset = (seed * 1013904223 + 1664525) % 10000 / 10000.0

    @staticmethod
    def get_target_bpm(t_sec: float) -> float:
        """Return the target heart rate in bpm for the given elapsed time."""
        cycle_t = t_sec % CYCLE_DURATION_SEC
        if cycle_t < INTERVAL_1_END_SEC:
            return 60.0
        elif cycle_t < INTERVAL_2_END_SEC:
            return 90.0
        else:
            return 72.0

    @classmethod
    def get_phase_at_time(cls, t_sec: float) -> float:
        """Compute the integrated continuous cardiac phase phi(t) in [0, 1).
        
        Guarantees phase continuity across bpm transitions.
        phi'(t) = bpm(t) / 60 beats per second.
        """
        cycle_t = t_sec % CYCLE_DURATION_SEC
        
        # Integral of bpm / 60 over the segments:
        # [0, 120): rate = 60/60 = 1.0 beats/sec -> beats = t
        # [120, 240): rate = 90/60 = 1.5 beats/sec -> beats = 120 + 1.5 * (t - 120)
        # [240, 360): rate = 72/60 = 1.2 beats/sec -> beats = 120 + 180 + 1.2 * (t - 240) = 300 + 1.2 * (t - 240)
        # Total beats per 360s cycle = 120 + 180 + 144 = 444 beats.
        num_cycles = int(t_sec // CYCLE_DURATION_SEC)
        beats_in_full_cycles = num_cycles * 444.0
        
        if cycle_t < 120.0:
            current_beats = cycle_t * 1.0
        elif cycle_t < 240.0:
            current_beats = 120.0 + (cycle_t - 120.0) * 1.5
        else:
            current_beats = 300.0 + (cycle_t - 240.0) * 1.2
            
        total_beats = beats_in_full_cycles + current_beats
        return total_beats % 1.0

    def generate_sensor1_channel2(self, sample_index: int) -> int:
        """Generate realistic ECG waveform with sharp R-peak for Sensor 1 Channel 2.
        
        Baseline ~ 1000, R-peak ~ 3000, satisfying all Section 3 peak conditions:
        - Strict local maximum
        - Larger than half of max in last 800 samples (3000 > 0.5 * 3000 = 1500)
        - P and T waves < 1400 (do not falsely trigger peak)
        """
        t = sample_index / SAMPLING_RATE
        phase = self.get_phase_at_time(t)
        
        # Center R-peak at phase 0.5 for symmetry
        # Distance from peak in phase space [-0.5, 0.5]
        dp = phase - 0.5
        if dp > 0.5:
            dp -= 1.0
        elif dp < -0.5:
            dp += 1.0
            
        # Physiological baseline with slight slow baseline wander
        baseline = 1000.0 + 30.0 * math.sin(2 * math.pi * 0.15 * t + self.phase_offset)
        
        # 1. Sharp R-peak: high amplitude, narrow Gaussian pulse
        # Sigma = 0.015 phase units (~10-15 samples wide at 400Hz)
        r_peak = 2000.0 * math.exp(- (dp / 0.016) ** 2)
        
        # 2. Q dip immediately preceding R-peak
        q_dip = -250.0 * math.exp(- ((dp + 0.035) / 0.012) ** 2)
        
        # 3. S dip immediately following R-peak
        s_dip = -350.0 * math.exp(- ((dp - 0.035) / 0.014) ** 2)
        
        # 4. P wave preceding QRS
        p_wave = 150.0 * math.exp(- ((dp + 0.20) / 0.045) ** 2)
        
        # 5. T wave following QRS
        t_wave = 280.0 * math.exp(- ((dp - 0.22) / 0.065) ** 2)
        
        val = baseline + r_peak + q_dip + s_dip + p_wave + t_wave
        return int(round(val))

    def generate_sensor1_sample(self, sample_index: int) -> list[int]:
        """Generate all 3 channels for Sensor 1.
        
        Channel 2: ECG
        Channel 1: Channel 2 * 0.6
        Channel 3: Channel 2 * 1.3
        """
        ch2 = self.generate_sensor1_channel2(sample_index)
        ch1 = int(round(ch2 * 0.6))
        ch3 = int(round(ch2 * 1.3))
        return [ch1, ch2, ch3]

    def generate_sensor2_sample(self, sample_index: int) -> list[int]:
        """Generate 2 channels for Sensor 2.
        
        Channel 1: Near 400,000, varying slowly.
        Channel 2: Near 150,000, varying slowly.
        """
        t = sample_index / SAMPLING_RATE
        # Slow respiratory variation (~0.25 Hz = 15 breaths/min)
        resp_phase = 2 * math.pi * 0.25 * t + self.phase_offset
        # Cardiac pulse modulation (~1 - 1.5 Hz)
        cardiac_phase = 2 * math.pi * self.get_phase_at_time(t)
        
        # Channel 1: near 400,000
        ch1 = 400_000.0 + 8000.0 * math.sin(resp_phase) + 3000.0 * math.cos(cardiac_phase)
        
        # Channel 2: near 150,000
        ch2 = 150_000.0 + 4000.0 * math.cos(resp_phase) + 6000.0 * math.sin(cardiac_phase)
        
        return [int(round(ch1)), int(round(ch2))]

    def generate_sample(self, sensor: int, sample_index: int) -> list[int]:
        """Generate sample for requested sensor (1 or 2)."""
        if sensor == 1:
            return self.generate_sensor1_sample(sample_index)
        elif sensor == 2:
            return self.generate_sensor2_sample(sample_index)
        else:
            raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")

    def generate_block(self, sensor: int, start_index: int, count: int) -> list[list[int]]:
        """Generate contiguous block of samples."""
        if count <= 0:
            return []
        if sensor == 1:
            return [self.generate_sensor1_sample(start_index + i) for i in range(count)]
        elif sensor == 2:
            return [self.generate_sensor2_sample(start_index + i) for i in range(count)]
        else:
            raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")
