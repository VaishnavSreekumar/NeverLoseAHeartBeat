"""MayaCardio biosignal-mock protocol framing, validation, and data schemas.

Strictly preserves Section 6.2 and 6.3 of the MayaCardio assessment:
- NDJSON framing over TCP
- Commands: {"cmd": "read", "sensor": 1} / {"cmd": "read", "sensor": 1, "from_index": N}
- Replies: session_id, sensor, first_index, count, lost, session_end, crc32, data
- Exact zlib CRC-32 checksum calculation over raw data payload text from '[' to ']'
- 24-bit signed integer limits: [-8,388,608 to 8,388,607]
"""

from __future__ import annotations

import json
import zlib
from dataclasses import asdict, dataclass, field
from typing import Any, List, Optional, Tuple

# Signed 24-bit integer limits
MIN_INT24 = -8_388_608
MAX_INT24 = 8_388_607
MAX_WINDOW_SAMPLES = 400  # At most 1 second of data per reply


class ProtocolError(ValueError):
    """Raised when an NDJSON frame or command violates protocol specifications."""


@dataclass(frozen=True)
class ReadCommand:
    """Command payload sent from pipeline client to mock service."""
    sensor: int
    cmd: str = "read"
    from_index: Optional[int] = None

    def __post_init__(self) -> None:
        if self.cmd != "read":
            raise ProtocolError(f"Invalid command '{self.cmd}'; expected 'read'.")
        if self.sensor not in (1, 2):
            raise ProtocolError(f"Invalid sensor '{self.sensor}'; expected 1 or 2.")
        if self.from_index is not None and self.from_index < 0:
            raise ProtocolError(f"Invalid from_index {self.from_index}; must be non-negative.")

    def to_json(self) -> str:
        """Serialize command to newline-delimited JSON string."""
        payload: dict[str, Any] = {"cmd": self.cmd, "sensor": self.sensor}
        if self.from_index is not None:
            payload["from_index"] = self.from_index
        return json.dumps(payload) + "\n"

    @classmethod
    def from_json(cls, line: str | bytes) -> ReadCommand:
        """Deserialize and validate command from NDJSON line."""
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = line.strip()
        if not line:
            raise ProtocolError("Empty command line received.")
        try:
            data = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"Malformed JSON command: {exc}") from exc
        
        if not isinstance(data, dict):
            raise ProtocolError("Command payload must be a JSON object.")
        
        cmd = data.get("cmd")
        sensor = data.get("sensor")
        from_index = data.get("from_index")
        
        if cmd is None or sensor is None:
            raise ProtocolError("Command missing required fields 'cmd' or 'sensor'.")
        
        return cls(cmd=cmd, sensor=sensor, from_index=from_index)


@dataclass(frozen=True)
class LossRecord:
    """Represents an explicit data loss interval reported by the mock."""
    from_index: int
    to_index: Optional[int]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_index": self.from_index,
            "to_index": self.to_index,
            "reason": self.reason,
        }


@dataclass
class MockReply:
    """Complete mock service reply frame."""
    session_id: str
    sensor: int
    first_index: int
    count: int
    lost: list[LossRecord]
    session_end: bool
    crc32: str
    data: list[list[int]]
    raw_data_text: Optional[str] = field(default=None, repr=False)

    def validate_schema(self) -> None:
        """Validate structural compliance and value ranges."""
        if not self.session_id:
            raise ProtocolError("session_id must not be empty.")
        if self.sensor not in (1, 2):
            raise ProtocolError(f"sensor must be 1 or 2, got {self.sensor}.")
        if self.first_index < 0:
            raise ProtocolError(f"first_index must be >= 0, got {self.first_index}.")
        if self.count < 0 or self.count > MAX_WINDOW_SAMPLES:
            raise ProtocolError(f"count must be in [0, {MAX_WINDOW_SAMPLES}], got {self.count}.")
        if len(self.data) != self.count:
            raise ProtocolError(
                f"Data length mismatch: count={self.count} but len(data)={len(self.data)}."
            )
        
        expected_channels = 3 if self.sensor == 1 else 2
        for i, sample in enumerate(self.data):
            if len(sample) != expected_channels:
                raise ProtocolError(
                    f"Sample at index {self.first_index + i} has {len(sample)} channels, "
                    f"expected {expected_channels} for sensor {self.sensor}."
                )
            for ch_val in sample:
                if not (MIN_INT24 <= ch_val <= MAX_INT24):
                    raise ProtocolError(
                        f"Sample value {ch_val} exceeds signed 24-bit range "
                        f"[{MIN_INT24}, {MAX_INT24}]."
                    )


def compute_crc32(raw_text: str | bytes) -> str:
    """Compute zlib CRC-32 over raw text and return 8-char lowercase hex."""
    if isinstance(raw_text, str):
        data_bytes = raw_text.encode("utf-8")
    else:
        data_bytes = raw_text
    checksum = zlib.crc32(data_bytes) & 0xFFFFFFFF
    return f"{checksum:08x}"


def extract_raw_data_slice(raw_line: str) -> str:
    """Extract the raw substring of the 'data' array from '[' to matching ']'.
    
    Preserves exact whitespace and character representation as sent over the wire.
    Uses token-level scanning to avoid false matches on '"data"' inside preceding
    string values or metadata fields.
    """
    n = len(raw_line)
    i = 0
    in_string = False
    escape = False
    brace_depth = 0
    bracket_depth = 0
    
    data_start_bracket = -1
    
    while i < n:
        c = raw_line[i]
        
        if escape:
            escape = False
            i += 1
            continue
            
        if c == '\\':
            if in_string:
                escape = True
            i += 1
            continue
            
        if c == '"':
            if not in_string:
                # Starting a string literal.
                # Check if this string could be the key "data" at top-level object (brace_depth == 1, bracket_depth == 0)
                str_start = i
                j = i + 1
                str_escaped = False
                while j < n:
                    cj = raw_line[j]
                    if str_escaped:
                        str_escaped = False
                        j += 1
                        continue
                    if cj == '\\':
                        str_escaped = True
                        j += 1
                        continue
                    if cj == '"':
                        break
                    j += 1
                
                if j >= n:
                    raise ProtocolError("Unterminated string literal in JSON line.")
                
                content = raw_line[str_start + 1 : j]
                
                # Check if this string is the property key "data"
                if brace_depth == 1 and bracket_depth == 0 and content == "data":
                    # Look ahead after closing quote for optional whitespace, then ':'
                    k = j + 1
                    while k < n and raw_line[k].isspace():
                        k += 1
                    if k < n and raw_line[k] == ':':
                        k += 1
                        while k < n and raw_line[k].isspace():
                            k += 1
                        if k < n and raw_line[k] == '[':
                            data_start_bracket = k
                            break
                        else:
                            raise ProtocolError("Value for 'data' key must be an array starting with '['.")
                
                # If not the "data" key or value not bracket, advance past this string
                i = j + 1
                continue
            else:
                in_string = False
                i += 1
                continue
                
        if not in_string:
            if c == '{':
                brace_depth += 1
            elif c == '}':
                brace_depth -= 1
            elif c == '[':
                bracket_depth += 1
            elif c == ']':
                bracket_depth -= 1
                
        i += 1
        
    if data_start_bracket == -1:
        raise ProtocolError("Missing '\"data\"' key in payload line.")
        
    # Now find the matching ']' for data_start_bracket
    depth = 0
    in_string = False
    escape = False
    end_bracket = -1
    
    for idx in range(data_start_bracket, n):
        char = raw_line[idx]
        if escape:
            escape = False
            continue
        if char == '\\':
            if in_string:
                escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
            
        if char == '[':
            depth += 1
        elif char == ']':
            depth -= 1
            if depth == 0:
                end_bracket = idx
                break
                
    if end_bracket == -1 or depth != 0:
        raise ProtocolError("Unmatched brackets in 'data' array payload.")
        
    return raw_line[data_start_bracket : end_bracket + 1]


def format_mock_reply(
    session_id: str,
    sensor: int,
    first_index: int,
    data: list[list[int]],
    lost: Optional[list[dict[str, Any] | LossRecord]] = None,
    session_end: bool = False,
    corrupt_crc: bool = False,
) -> str:
    """Format and serialize a compliant mock reply frame to NDJSON string.
    
    Computes exact zlib CRC-32 over the data text slice.
    """
    lost_records: list[dict[str, Any]] = []
    if lost:
        for item in lost:
            if isinstance(item, LossRecord):
                lost_records.append(item.to_dict())
            elif isinstance(item, dict):
                lost_records.append(item)
    
    # Format data array with standard json dumps
    data_str = json.dumps(data)
    crc = compute_crc32(data_str)
    
    if corrupt_crc:
        # Deliberately invert or alter CRC hex string
        crc = f"{(int(crc, 16) ^ 0xFFFFFFFF) & 0xFFFFFFFF:08x}"
    
    payload = {
        "session_id": session_id,
        "sensor": sensor,
        "first_index": first_index,
        "count": len(data),
        "lost": lost_records,
        "session_end": session_end,
        "crc32": crc,
    }
    
    # Construct exact payload string by combining metadata and raw data string
    # This guarantees that the serialized data text is bit-for-bit identical to what CRC was computed on
    meta_json = json.dumps(payload)
    # Remove trailing '}' and insert data
    wire_line = meta_json[:-1] + f', "data": {data_str}' + "}\n"
    return wire_line


def parse_mock_reply(raw_line: str | bytes) -> tuple[MockReply, bool]:
    """Parse an NDJSON reply line, validate CRC-32, and return (MockReply, is_valid).
    
    Returns:
        (MockReply, True) if CRC-32 strictly matches the raw data text.
        (MockReply, False) if CRC-32 mismatch detected (corrupt reply).
    """
    if isinstance(raw_line, bytes):
        raw_text = raw_line.decode("utf-8")
    else:
        raw_text = raw_line
        
    trimmed = raw_text.strip()
    if not trimmed:
        raise ProtocolError("Empty NDJSON line received.")
        
    try:
        parsed = json.loads(trimmed)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"JSON decode failed for reply line: {exc}") from exc
        
    if not isinstance(parsed, dict):
        raise ProtocolError("Reply payload must be a JSON object.")
        
    # Extract raw data text slice directly from the unparsed wire string
    raw_data_slice = extract_raw_data_slice(raw_text)
    expected_crc = compute_crc32(raw_data_slice)
    received_crc = parsed.get("crc32", "").lower()
    is_crc_valid = (expected_crc == received_crc)
    
    lost_list: list[LossRecord] = []
    for lr in parsed.get("lost", []):
        lost_list.append(
            LossRecord(
                from_index=lr["from_index"],
                to_index=lr.get("to_index"),
                reason=lr.get("reason", "unknown"),
            )
        )
        
    reply = MockReply(
        session_id=str(parsed.get("session_id", "")),
        sensor=int(parsed.get("sensor", 0)),
        first_index=int(parsed.get("first_index", 0)),
        count=int(parsed.get("count", 0)),
        lost=lost_list,
        session_end=bool(parsed.get("session_end", False)),
        crc32=received_crc,
        data=parsed.get("data", []),
        raw_data_text=raw_data_slice,
    )
    reply.validate_schema()
    return reply, is_crc_valid
