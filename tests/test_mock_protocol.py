"""Unit tests for shared/protocol.py (Increment A).

Verifies REQ-A-07, REQ-A-08, REQ-A-09, REQ-A-10:
- NDJSON command framing and parsing
- Mock reply serialization and deserialization
- CRC-32 (zlib) calculation from '[' to matching ']'
- Detection and rejection of corrupt CRC-32 replies
- 24-bit signed integer limits and channel count validation
"""

import json
import pytest
from shared.protocol import (
    MAX_INT24,
    MIN_INT24,
    LossRecord,
    MockReply,
    ProtocolError,
    ReadCommand,
    compute_crc32,
    extract_raw_data_slice,
    format_mock_reply,
    parse_mock_reply,
)


def test_read_command_serialization() -> None:
    """Verifies REQ-A-08: format and parsing of read commands."""
    cmd1 = ReadCommand(sensor=1)
    assert cmd1.to_json() == '{"cmd": "read", "sensor": 1}\n'
    
    cmd2 = ReadCommand(sensor=2, from_index=120000)
    assert cmd2.to_json() == '{"cmd": "read", "sensor": 2, "from_index": 120000}\n'

    # Round trip deserialization
    parsed1 = ReadCommand.from_json(cmd1.to_json())
    assert parsed1.sensor == 1
    assert parsed1.from_index is None

    parsed2 = ReadCommand.from_json(cmd2.to_json())
    assert parsed2.sensor == 2
    assert parsed2.from_index == 120000


def test_read_command_validation_failures() -> None:
    """Verifies that invalid commands raise ProtocolError."""
    with pytest.raises(ProtocolError):
        ReadCommand(sensor=3)  # Only 1 or 2 allowed
        
    with pytest.raises(ProtocolError):
        ReadCommand(sensor=1, from_index=-5)  # Must be >= 0

    with pytest.raises(ProtocolError):
        ReadCommand.from_json('{"cmd": "write", "sensor": 1}')

    with pytest.raises(ProtocolError):
        ReadCommand.from_json('{"sensor": 1}')

    with pytest.raises(ProtocolError):
        ReadCommand.from_json('')


def test_crc32_computation_and_spec_vector() -> None:
    """Verifies REQ-A-10 & AUDIT-07: zlib CRC-32 computed over data text matches exact spec vector."""
    # Data text matching spec line 311:
    # "data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]
    spec_data_text = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]"
    crc = compute_crc32(spec_data_text)
    assert len(crc) == 8
    assert crc.islower()
    # Literal test vector from raw_assessment_spec.txt line 311
    assert crc == "f9a5bf62"


def test_extract_raw_data_slice() -> None:
    """Verifies exact raw substring extraction from '[' to matching ']'."""
    wire_line = (
        '{"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, '
        '"lost": [], "session_end": false, "crc32": "f9a5bf62", '
        '"data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]}\n'
    )
    raw_slice = extract_raw_data_slice(wire_line)
    assert raw_slice == "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]"
    assert compute_crc32(raw_slice) == "f9a5bf62"

    # Malformed lines
    with pytest.raises(ProtocolError):
        extract_raw_data_slice('{"no_data_key": 1}')
    with pytest.raises(ProtocolError):
        extract_raw_data_slice('{"data": [1, 2')  # Unmatched


def test_extract_raw_data_slice_adversarial_cases() -> None:
    """Verifies AUDIT-05: parser handles tricky metadata, escaped quotes, and whitespace without false match."""
    # Case 1: Preceding string containing "data": [fake] with escaped quote
    line1 = '{"session_id": "data\\": [fake]", "sensor": 1, "data": [[1, 2, 3]]}'
    assert extract_raw_data_slice(line1) == "[[1, 2, 3]]"

    # Case 2: Preceding metadata object with nested array and key mentioning data
    line2 = '{"lost": [{"reason": "data: [overflow]"}], "data": [[720, 1200, 1560]]}'
    assert extract_raw_data_slice(line2) == "[[720, 1200, 1560]]"

    # Case 3: Whitespace between key, colon, and opening bracket
    line3 = '{"notes": "some text",   "data"   :   \n  [[10, 20, 30]]   }'
    assert extract_raw_data_slice(line3) == "[[10, 20, 30]]"

    # Case 4: Deeply nested data array
    line4 = '{"sensor": 1, "data": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]}'
    assert extract_raw_data_slice(line4) == "[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]"

    # Case 5: Data key with non-array value raises ProtocolError
    with pytest.raises(ProtocolError):
        extract_raw_data_slice('{"data": "not an array"}')

    # Case 6: Full parse_mock_reply succeeds with adversarial preceding keys
    line_full = (
        '{"session_id": "data\\": [fake]", "sensor": 1, "first_index": 0, "count": 1, '
        '"lost": [{"from_index": 0, "to_index": 1, "reason": "data"}], "session_end": false, '
        f'"crc32": "{compute_crc32("[[1, 2, 3]]")}", '
        '"data": [[1, 2, 3]]}\n'
    )
    reply, is_valid = parse_mock_reply(line_full)
    assert is_valid is True
    assert reply.raw_data_text == "[[1, 2, 3]]"


def test_format_and_parse_mock_reply_success() -> None:
    """Verifies REQ-A-09 & REQ-A-10: reply serialization, CRC-32 verification."""
    data_samples = [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]
    wire_str = format_mock_reply(
        session_id="5f0c2a9e",
        sensor=1,
        first_index=120000,
        data=data_samples,
        lost=[{"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}],
        session_end=False,
    )
    assert wire_str.endswith("\n")

    reply, is_valid = parse_mock_reply(wire_str)
    assert is_valid is True
    assert reply.session_id == "5f0c2a9e"
    assert reply.sensor == 1
    assert reply.first_index == 120000
    assert reply.count == 3
    assert len(reply.data) == 3
    assert len(reply.lost) == 1
    assert reply.lost[0].from_index == 4000
    assert reply.lost[0].to_index == 4399
    assert reply.lost[0].reason == "buffer_overflow"
    assert reply.session_end is False


def test_corrupted_crc_detection() -> None:
    """Verifies that corrupted CRC-32 replies are cleanly flagged as invalid."""
    data_samples = [[720, 1200, 1560], [708, 1180, 1534]]
    # Format with deliberate corrupted CRC
    corrupt_wire_str = format_mock_reply(
        session_id="abc12345",
        sensor=1,
        first_index=10,
        data=data_samples,
        corrupt_crc=True,
    )

    reply, is_valid = parse_mock_reply(corrupt_wire_str)
    assert is_valid is False
    assert reply.count == 2


def test_payload_tampering_detection() -> None:
    """Verifies that altering any character in the data text invalidates the CRC."""
    data_samples = [[720, 1200, 1560]]
    wire_str = format_mock_reply(
        session_id="abc12345",
        sensor=1,
        first_index=10,
        data=data_samples,
    )
    # Tamper with the integer value in the raw JSON string
    tampered_wire_str = wire_str.replace("720", "721")

    reply, is_valid = parse_mock_reply(tampered_wire_str)
    assert is_valid is False


def test_24bit_and_channel_constraints() -> None:
    """Verifies REQ-A-02: signed 24-bit integer validation and channel checks."""
    # Sensor 1 requires 3 channels
    valid_s1_data = [[MIN_INT24, 0, MAX_INT24]]
    reply_s1 = MockReply(
        session_id="s1",
        sensor=1,
        first_index=0,
        count=1,
        lost=[],
        session_end=False,
        crc32="00000000",
        data=valid_s1_data,
    )
    reply_s1.validate_schema()

    # Out of range 24-bit value (> MAX_INT24)
    with pytest.raises(ProtocolError):
        bad_reply = MockReply(
            session_id="s1",
            sensor=1,
            first_index=0,
            count=1,
            lost=[],
            session_end=False,
            crc32="00000000",
            data=[[MAX_INT24 + 1, 0, 0]],
        )
        bad_reply.validate_schema()

    # Out of range 24-bit value (< MIN_INT24)
    with pytest.raises(ProtocolError):
        bad_reply = MockReply(
            session_id="s1",
            sensor=1,
            first_index=0,
            count=1,
            lost=[],
            session_end=False,
            crc32="00000000",
            data=[[MIN_INT24 - 1, 0, 0]],
        )
        bad_reply.validate_schema()

    # Channel count mismatch: Sensor 1 with 2 channels
    with pytest.raises(ProtocolError):
        bad_reply = MockReply(
            session_id="s1",
            sensor=1,
            first_index=0,
            count=1,
            lost=[],
            session_end=False,
            crc32="00000000",
            data=[[100, 200]],
        )
        bad_reply.validate_schema()

    # Sensor 2 requires 2 channels
    valid_s2_data = [[100, 200]]
    reply_s2 = MockReply(
        session_id="s2",
        sensor=2,
        first_index=0,
        count=1,
        lost=[],
        session_end=False,
        crc32="00000000",
        data=valid_s2_data,
    )
    reply_s2.validate_schema()

    # Channel count mismatch: Sensor 2 with 3 channels
    with pytest.raises(ProtocolError):
        bad_reply = MockReply(
            session_id="s2",
            sensor=2,
            first_index=0,
            count=1,
            lost=[],
            session_end=False,
            crc32="00000000",
            data=[[100, 200, 300]],
        )
        bad_reply.validate_schema()
