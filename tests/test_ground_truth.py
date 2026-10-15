"""Unit tests for independent ground-truth recording and auditing engine (Increment F).

Verifies REQ-H-04, REQ-H-05:
- Independent ground truth tap records raw generated stream
- Auditor detects missing samples (silent loss)
- Auditor detects duplicate samples
- Auditor detects altered / corrupted channel values
- Auditor reconciles explicit loss entries against dropped spans
"""

import json
import tempfile
from pathlib import Path
import pytest

from biosignal_mock.generator import DualSensorGenerator
from biosignal_mock.ground_truth import (
    AuditReport,
    GroundTruthRecorder,
    audit_export_against_ground_truth,
)


def test_clean_export_auditing() -> None:
    """Verifies that an export matching ground truth 100% passes as clean."""
    gt_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),
        json.dumps({"session_id": "s1", "sensor": 2, "index": 0, "values": [0, 1]}),
    ]
    export_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),
        json.dumps({"session_id": "s1", "sensor": 2, "index": 0, "values": [0, 1]}),
    ]

    report = audit_export_against_ground_truth(gt_lines, export_lines)
    assert report.is_clean is True
    assert report.missing_samples == 0
    assert report.duplicate_samples == 0
    assert report.altered_samples == 0


def test_silent_loss_detection() -> None:
    """Verifies REQ-H-05: missing sample without loss record is flagged as silent loss."""
    gt_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),  # Missing in export!
        json.dumps({"session_id": "s1", "sensor": 1, "index": 2, "values": [6, 7, 8]}),
    ]
    export_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 2, "values": [6, 7, 8]}),
    ]

    report = audit_export_against_ground_truth(gt_lines, export_lines)
    assert report.is_clean is False
    assert report.missing_samples == 1
    assert any("SILENT LOSS DEFECT" in d for d in report.defects)


def test_explicit_loss_reconciliation() -> None:
    """Verifies that missing sample covered by explicit loss record is accepted."""
    gt_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 2, "values": [6, 7, 8]}),
    ]
    export_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        # Loss record covering index 1 to 2
        json.dumps({"sensor": 1, "from_index": 1, "to_index": 2, "reason": "buffer_overflow"}),
    ]

    report = audit_export_against_ground_truth(gt_lines, export_lines)
    assert report.is_clean is True
    assert report.missing_samples == 0


def test_duplicate_sample_detection() -> None:
    """Verifies duplicate sample index in export is flagged as defect."""
    gt_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),
    ]
    export_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),  # Duplicate!
        json.dumps({"session_id": "s1", "sensor": 1, "index": 1, "values": [3, 4, 5]}),
    ]

    report = audit_export_against_ground_truth(gt_lines, export_lines)
    assert report.is_clean is False
    assert report.duplicate_samples == 1
    assert any("DUPLICATION DEFECT" in d for d in report.defects)


def test_altered_value_detection() -> None:
    """Verifies corrupted value in export is flagged as defect."""
    gt_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 1, 2]}),
    ]
    export_lines = [
        json.dumps({"session_id": "s1", "sensor": 1, "index": 0, "values": [0, 9999, 2]}),  # Corrupted!
    ]

    report = audit_export_against_ground_truth(gt_lines, export_lines)
    assert report.is_clean is False
    assert report.altered_samples == 1
    assert any("CORRUPTION DEFECT" in d for d in report.defects)


def test_live_ground_truth_tap_recording() -> None:
    """Verifies that GroundTruthRecorder correctly taps live generator output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        gt_file = Path(tmpdir) / "test_gt.ndjson"
        recorder = GroundTruthRecorder(gt_file)

        gen = DualSensorGenerator(
            mode="ramp",
            seed=7,
            duration=10,
            ground_truth_tap=recorder.tap,
        )
        gen.advance_samples(50)  # Generates 50 samples for both sensors = 100 sample records
        recorder.close()

        assert gt_file.exists()
        lines = gt_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 100  # 50 samples * 2 sensors
        first_record = json.loads(lines[0])
        assert first_record["sensor"] == 1
        assert first_record["index"] == 0
        assert first_record["values"] == [0, 1, 2]
