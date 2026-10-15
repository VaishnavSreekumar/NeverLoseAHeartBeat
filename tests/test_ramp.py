"""Unit tests for biosignal_mock/ramp.py (Increment B).

Verifies REQ-A-05, REQ-A-06:
- Mathematical exactness of ramp formulas:
  Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
  Sensor 2 channel c at sample i = (2i + c) mod 8,388,608
- Word size within signed 24-bit limits
- Block generation matches sample generation bit-for-bit
"""

import pytest
from biosignal_mock.ramp import (
    MODULUS_24BIT,
    generate_ramp_block,
    generate_ramp_sample,
    generate_ramp_sample_sensor1,
    generate_ramp_sample_sensor2,
)
from shared.protocol import MAX_INT24, MIN_INT24


def test_ramp_exact_initial_values() -> None:
    """Verifies i=0, i=1, i=2 initial conditions."""
    # i = 0
    s1_0 = generate_ramp_sample_sensor1(0)
    assert s1_0 == [0, 1, 2]
    s2_0 = generate_ramp_sample_sensor2(0)
    assert s2_0 == [0, 1]

    # i = 1
    s1_1 = generate_ramp_sample_sensor1(1)
    assert s1_1 == [3, 4, 5]
    s2_1 = generate_ramp_sample_sensor2(1)
    assert s2_1 == [2, 3]

    # i = 100
    s1_100 = generate_ramp_sample_sensor1(100)
    assert s1_100 == [300, 301, 302]
    s2_100 = generate_ramp_sample_sensor2(100)
    assert s2_100 == [200, 201]


def test_ramp_modulus_wraparound() -> None:
    """Verifies that modulo 8,388,608 wraps around without overflowing 24-bit integer."""
    # For Sensor 1: 3 * i >= 8,388,608
    # 8,388,608 // 3 = 2,796,202 with remainder 2
    i_wrap = 2_796_202
    # 3 * 2,796,202 = 8,388,606
    s1_wrap = generate_ramp_sample_sensor1(i_wrap)
    assert s1_wrap[0] == 8_388_606
    assert s1_wrap[1] == 8_388_607  # MAX_INT24
    assert s1_wrap[2] == 0          # Wrapped!

    # For Sensor 2: 2 * i >= 8,388,608
    # 8,388,608 // 2 = 4,194,304
    i2_wrap = 4_194_304
    s2_wrap = generate_ramp_sample_sensor2(i2_wrap)
    assert s2_wrap[0] == 0          # Wrapped!
    assert s2_wrap[1] == 1


def test_ramp_large_sequence_and_24bit_range() -> None:
    """Verifies 10,000 samples across range are strictly within signed 24-bit range."""
    test_indices = [0, 1, 400, 10_000, 100_000, 1_000_000, 5_000_000]
    for idx in test_indices:
        s1 = generate_ramp_sample_sensor1(idx)
        assert len(s1) == 3
        for v in s1:
            assert MIN_INT24 <= v <= MAX_INT24
            assert 0 <= v < MODULUS_24BIT

        s2 = generate_ramp_sample_sensor2(idx)
        assert len(s2) == 2
        for v in s2:
            assert MIN_INT24 <= v <= MAX_INT24
            assert 0 <= v < MODULUS_24BIT


def test_generate_ramp_block_consistency() -> None:
    """Verifies block generation produces identical samples as single calls."""
    start = 120_000
    count = 400
    block_s1 = generate_ramp_block(1, start, count)
    assert len(block_s1) == count
    for offset in range(count):
        assert block_s1[offset] == generate_ramp_sample_sensor1(start + offset)

    block_s2 = generate_ramp_block(2, start, count)
    assert len(block_s2) == count
    for offset in range(count):
        assert block_s2[offset] == generate_ramp_sample_sensor2(start + offset)


def test_invalid_sensor_raises_value_error() -> None:
    """Verifies invalid sensor parameter raises ValueError."""
    with pytest.raises(ValueError):
        generate_ramp_sample(3, 0)

    with pytest.raises(ValueError):
        generate_ramp_block(0, 0, 10)
