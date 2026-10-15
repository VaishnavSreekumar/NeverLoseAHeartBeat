"""Tests for MayaCardio Pipeline CLI and Offline Processing Engine (Increment B).

Covers all Phase 3 Increment B requirements:
1. `pipeline status`: single JSON line with mode, samples_saved, samples_in_cloud, backlog.
2. `pipeline export --session ID --from device`: NDJSON stream of samples and loss records.
3. `pipeline export --session ID --from cloud`: honest non-zero error without cloud invention.
4. `pipeline results --session ID`: Section 3 window metrics derived from persisted SQLite data.
5. JSONL validity and machine-readable stdout / diagnostic stderr separation.
6. Exit codes, malformed arguments, empty sessions, and unknown sessions.
7. End-to-end integration proving `pipeline results` calculates Section 3 metrics from disk.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List
import pytest

from biosignal_mock.signal import SignalGenerator
from pipeline.cli import build_parser, main
from pipeline.protocol_client import IngestBatch
from pipeline.storage import StorageEngine
from shared.protocol import LossRecord


# ==============================================================================
# Helpers
# ==============================================================================

def create_populated_db(db_path: Path, session_id: str = "sess_cli_test") -> tuple[StorageEngine, int]:
    """Helper creating a SQLite database populated with 2,400 samples (6 windows)."""
    storage = StorageEngine(db_path=db_path)
    gen = SignalGenerator(seed=7)

    # 2,400 samples = 6 windows (windows 0 through 5)
    # Sensor 1: 3 channels
    # Sensor 2: 2 channels
    total_samples = 0
    batch_size = 400
    for chunk_start in range(0, 2400, batch_size):
        # Sensor 1
        s1_data = [
            [
                int(gen.generate_sensor1_channel2(i) * 0.6),
                gen.generate_sensor1_channel2(i),
                int(gen.generate_sensor1_channel2(i) * 1.3),
            ]
            for i in range(chunk_start, chunk_start + batch_size)
        ]
        batch1 = IngestBatch(
            session_id=session_id,
            sensor=1,
            first_index=chunk_start,
            count=batch_size,
            data=s1_data,
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        storage.commit_batch(batch1)
        total_samples += batch_size

        # Sensor 2
        s2_data = [
            [400000 + (i % 100), 150000 + (i % 50)]
            for i in range(chunk_start, chunk_start + batch_size)
        ]
        batch2 = IngestBatch(
            session_id=session_id,
            sensor=2,
            first_index=chunk_start,
            count=batch_size,
            data=s2_data,
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        storage.commit_batch(batch2)
        total_samples += batch_size

    return storage, total_samples


# ==============================================================================
# 1. `pipeline status` Command
# ==============================================================================

def test_cli_status_empty_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline status with no database reports 0 samples and null cloud fields."""
    db_file = tmp_path / "non_existent.db"
    code = main(["status", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert len(lines) == 1, "pipeline status must emit exactly one line to stdout"

    status = json.loads(lines[0])
    assert status["mode"] == "offline"
    assert status["samples_saved"] == 0
    assert status["samples_in_cloud"] is None
    assert status["backlog"] is None


def test_cli_status_populated_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline status with populated database reports correct sample count."""
    db_file = tmp_path / "status_test.db"
    storage, total_samples = create_populated_db(db_file, session_id="sess_status")
    storage.close()

    code = main(["status", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    status = json.loads(captured.out.strip())
    assert status["mode"] == "offline"
    assert status["samples_saved"] == total_samples
    assert status["samples_in_cloud"] is None
    assert status["backlog"] is None


# ==============================================================================
# 2. `pipeline export` Command
# ==============================================================================

def test_cli_export_device_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline export --from device outputs valid NDJSON matching Section 4 schema."""
    db_file = tmp_path / "export_test.db"
    storage, total_samples = create_populated_db(db_file, session_id="sess_export")

    # Add an authoritative loss record
    loss_batch = IngestBatch(
        session_id="sess_export",
        sensor=1,
        first_index=2400,
        count=0,
        data=[],
        lost=[LossRecord(from_index=2400, to_index=2799, reason="buffer_overflow")],
        session_end=False,
        crc32="dummy",
    )
    storage.commit_batch(loss_batch)
    storage.close()

    code = main(["export", "--session", "sess_export", "--from", "device", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.strip().splitlines()]

    # 2,400 samples + 1 loss record = 2,401 lines
    assert len(lines) == total_samples + 1

    sample_records = [l for l in lines if "values" in l]
    loss_records = [l for l in lines if "reason" in l]

    assert len(sample_records) == total_samples
    assert len(loss_records) == 1

    # Verify sample record schema
    s0 = sample_records[0]
    assert s0["session_id"] == "sess_export"
    assert s0["sensor"] in (1, 2)
    assert "index" in s0
    assert isinstance(s0["values"], list)

    # Verify loss record schema
    l0 = loss_records[0]
    assert l0["session_id"] == "sess_export"
    assert l0["sensor"] == 1
    assert l0["from_index"] == 2400
    assert l0["to_index"] == 2799
    assert l0["reason"] == "buffer_overflow"

    # Stderr must contain informational diagnostics, not stdout
    assert "Exported" in captured.err


def test_cli_export_from_cloud_rejected(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline export --from cloud returns clear non-zero error without inventing data."""
    code = main(["export", "--session", "sess_cloud", "--from", "cloud"])
    assert code != 0, "Cloud export must return non-zero exit code when not active"

    captured = capsys.readouterr()
    assert captured.out == "", "Stdout must remain completely clean on error"
    assert "Cloud export is unavailable" in captured.err


def test_cli_export_unknown_session(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline export for non-existent session returns exit code 1 and error on stderr."""
    db_file = tmp_path / "unknown_sess.db"
    storage = StorageEngine(db_path=db_file)
    storage.close()

    code = main(["export", "--session", "does_not_exist", "--from", "device", "--db", str(db_file)])
    assert code == 1

    captured = capsys.readouterr()
    assert "Session 'does_not_exist' not found" in captured.err
    assert captured.out == ""


def test_cli_export_empty_session(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline export for a valid empty session returns exit code 0 and emits 0 lines to stdout."""
    db_file = tmp_path / "empty_sess_export.db"
    storage = StorageEngine(db_path=db_file)
    # Commit an empty session batch (count=0, lost=[])
    empty_batch = IngestBatch(
        session_id="sess_empty_exp",
        sensor=1,
        first_index=0,
        count=0,
        data=[],
        lost=[],
        session_end=True,
        crc32="dummy",
    )
    storage.commit_batch(empty_batch)
    storage.close()

    code = main(["export", "--session", "sess_empty_exp", "--from", "device", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    assert captured.out == "", "Stdout must contain zero data lines for empty session"
    assert "Exported 0 lines" in captured.err


# ==============================================================================
# 3. `pipeline results` Command
# ==============================================================================

def test_cli_results_end_to_end_from_persisted_storage(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline results derives exact Section 3 metrics from SQLite persisted samples."""
    db_file = tmp_path / "results_test.db"
    # Create 2,400 samples = 6 windows (windows 0, 1, 2, 3, 4, 5) with 60 bpm signal
    storage, _ = create_populated_db(db_file, session_id="sess_results")
    storage.close()

    code = main(["results", "--session", "sess_results", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.strip().splitlines()]

    # 6 windows (w = 0, 1, 2, 3, 4, 5)
    assert len(lines) == 6

    for w_idx, row in enumerate(lines):
        assert row["window"] == w_idx
        assert row["mode"] == "offline"
        assert row["complete"] is True
        assert row["missing_1"] == 0
        assert row["missing_2"] == 0
        assert row["session_id"] == "sess_results"

        # Channel statistics schemas
        assert len(row["mean"]["1"]) == 3
        assert len(row["mean"]["2"]) == 2
        assert len(row["min"]["1"]) == 3
        assert len(row["min"]["2"]) == 2
        assert len(row["max"]["1"]) == 3
        assert len(row["max"]["2"]) == 2

        # Magnitude check
        assert row["magnitude"] is not None
        assert row["magnitude"] > 0

        # Exact Heart-rate null rules
        if w_idx < 4:
            assert row["heart_rate"] is None, f"Window {w_idx} (< 4) must return heart_rate = null"
        else:
            # For 60 bpm signal, heart rate must be 60.0
            assert row["heart_rate"] == 60.0


def test_cli_results_with_persisted_loss_ledger(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline results accounts for persisted loss records and produces accurate missing counts."""
    db_file = tmp_path / "results_loss.db"
    storage = StorageEngine(db_path=db_file)

    # Window 0 (0..399): Complete for both sensors
    gen = SignalGenerator(seed=7)
    s1_w0 = [
        [int(gen.generate_sensor1_channel2(i) * 0.6), gen.generate_sensor1_channel2(i), int(gen.generate_sensor1_channel2(i) * 1.3)]
        for i in range(400)
    ]
    s2_w0 = [[400000, 150000] for _ in range(400)]
    storage.commit_batch(IngestBatch("sess_gap", 1, 0, 400, s1_w0, [], False, "crc"))
    storage.commit_batch(IngestBatch("sess_gap", 2, 0, 400, s2_w0, [], False, "crc"))

    # Window 1 (400..799): Completely lost on Sensor 1, complete on Sensor 2
    s2_w1 = [[400000, 150000] for _ in range(400)]
    storage.commit_batch(IngestBatch("sess_gap", 2, 400, 400, s2_w1, [], False, "crc"))
    storage.commit_batch(
        IngestBatch(
            "sess_gap",
            1,
            400,
            0,
            [],
            [LossRecord(from_index=400, to_index=799, reason="network_dropout")],
            False,
            "crc",
        )
    )
    storage.close()

    code = main(["results", "--session", "sess_gap", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    results = [json.loads(line) for line in captured.out.strip().splitlines()]

    assert len(results) == 2

    # Window 0: complete
    res0 = results[0]
    assert res0["window"] == 0
    assert res0["complete"] is True
    assert res0["missing_1"] == 0
    assert res0["missing_2"] == 0

    # Window 1: Sensor 1 completely lost (400 missing), Sensor 2 complete (0 missing)
    res1 = results[1]
    assert res1["window"] == 1
    assert res1["complete"] is False
    assert res1["missing_1"] == 400
    assert res1["missing_2"] == 0
    assert res1["mean"]["1"] == [None, None, None]
    assert res1["min"]["1"] == [None, None, None]
    assert res1["max"]["1"] == [None, None, None]
    assert res1["heart_rate"] is None


def test_cli_results_unknown_session(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline results for unknown session returns code 1 and error on stderr."""
    db_file = tmp_path / "unknown_res.db"
    storage = StorageEngine(db_path=db_file)
    storage.close()

    code = main(["results", "--session", "nonexistent", "--db", str(db_file)])
    assert code == 1

    captured = capsys.readouterr()
    assert "Session 'nonexistent' not found" in captured.err
    assert captured.out == ""


def test_cli_results_empty_session(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """pipeline results for a valid empty session returns exit code 0 and emits 0 lines to stdout."""
    db_file = tmp_path / "empty_sess_results.db"
    storage = StorageEngine(db_path=db_file)
    # Commit an empty session batch (count=0, lost=[])
    empty_batch = IngestBatch(
        session_id="sess_empty_res",
        sensor=1,
        first_index=0,
        count=0,
        data=[],
        lost=[],
        session_end=True,
        crc32="dummy",
    )
    storage.commit_batch(empty_batch)
    storage.close()

    code = main(["results", "--session", "sess_empty_res", "--db", str(db_file)])
    assert code == 0

    captured = capsys.readouterr()
    assert captured.out == "", "Stdout must contain zero data lines for empty session"
    assert "has no sample or loss data" in captured.err


# ==============================================================================
# 4. CLI Argument Parsing & Subprocess Invocations
# ==============================================================================

def test_cli_invalid_arguments_returns_code_2(capsys: pytest.CaptureFixture[str]) -> None:
    """Invalid commands and missing required arguments return exit code 2."""
    # No arguments
    assert main([]) == 2

    # Unknown subcommand
    assert main(["foo"]) == 2

    # export missing --session
    assert main(["export", "--from", "device"]) == 2

    # export missing --from
    assert main(["export", "--session", "sess1"]) == 2

    # results missing --session
    assert main(["results"]) == 2


def test_cli_subprocess_invocation(tmp_path: Path) -> None:
    """Execute CLI via python -m pipeline.cli in a real subprocess."""
    db_file = tmp_path / "subproc.db"
    storage, _ = create_populated_db(db_file, session_id="sess_subproc")
    storage.close()

    # 1. Test status via subprocess
    cmd_stat = [sys.executable, "-m", "pipeline.cli", "status", "--db", str(db_file)]
    proc_stat = subprocess.run(cmd_stat, capture_output=True, text=True)
    assert proc_stat.returncode == 0
    stat_json = json.loads(proc_stat.stdout.strip())
    assert stat_json["mode"] == "offline"
    assert stat_json["samples_saved"] == 4800

    # 2. Test results via subprocess
    cmd_res = [sys.executable, "-m", "pipeline.cli", "results", "--session", "sess_subproc", "--db", str(db_file)]
    proc_res = subprocess.run(cmd_res, capture_output=True, text=True)
    assert proc_res.returncode == 0
    res_lines = [json.loads(l) for l in proc_res.stdout.strip().splitlines()]
    assert len(res_lines) == 6
    assert res_lines[4]["heart_rate"] == 60.0
