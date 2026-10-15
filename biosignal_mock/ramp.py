"""Deterministic ramp mode generator for MayaCardio biosignal-mock.

Strictly follows Section 6.1:
- Sensor 1 channel c at sample i = (3i + c) mod 8,388,608 (c in {0, 1, 2})
- Sensor 2 channel c at sample i = (2i + c) mod 8,388,608 (c in {0, 1})
- Values fit in signed 24-bit integer range [-8,388,608, 8,388,607]
- The same seed produces reproducible deterministic values.
"""

from __future__ import annotations

MODULUS_24BIT = 8_388_608  # 2^23


def generate_ramp_sample_sensor1(sample_index: int) -> list[int]:
    """Generate 3 channels for Sensor 1 at sample_index i."""
    base = 3 * sample_index
    return [
        (base + 0) % MODULUS_24BIT,
        (base + 1) % MODULUS_24BIT,
        (base + 2) % MODULUS_24BIT,
    ]


def generate_ramp_sample_sensor2(sample_index: int) -> list[int]:
    """Generate 2 channels for Sensor 2 at sample_index i."""
    base = 2 * sample_index
    return [
        (base + 0) % MODULUS_24BIT,
        (base + 1) % MODULUS_24BIT,
    ]


def generate_ramp_sample(sensor: int, sample_index: int) -> list[int]:
    """Generate sample array for requested sensor (1 or 2)."""
    if sensor == 1:
        return generate_ramp_sample_sensor1(sample_index)
    elif sensor == 2:
        return generate_ramp_sample_sensor2(sample_index)
    else:
        raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")


def generate_ramp_block(sensor: int, start_index: int, count: int) -> list[list[int]]:
    """Generate a contiguous block of samples [start_index, start_index + count - 1]."""
    if count <= 0:
        return []
    if sensor == 1:
        return [generate_ramp_sample_sensor1(start_index + offset) for offset in range(count)]
    elif sensor == 2:
        return [generate_ramp_sample_sensor2(start_index + offset) for offset in range(count)]
    else:
        raise ValueError(f"Invalid sensor {sensor}; must be 1 or 2.")
