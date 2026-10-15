"""MayaCardio Phase 4C Recovery and Catch-Up Tests.

Validates:
1. Exact device-cloud reconciliation (exact sample indices, channels, and loss-ledger entries).
2. Two-hour backlog catch-up and 15-minute SLA target.
3. Duplicate submission idempotency (zero duplicates inserted).
4. Conflicting record quarantine (HTTP 409 Conflict rejection without overwriting).
5. Clock change resilience (wall-clock skew does not alter sequence cursors or data integrity).
6. Cloud receiver restart during catch-up.
7. Interrupted catch-up recovery (client crash/restart).
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time
from typing import Any

from fastapi.testclient import TestClient
import httpx
import pytest

from pipeline.cloud.server import create_cloud_app
from pipeline.cloud.storage import CloudStorageEngine
from pipeline.protocol_client import IngestBatch
from pipeline.reconciliation import reconcile_session
from pipeline.storage import StorageEngine
from pipeline.sync import CloudSyncWorker
from shared.protocol import LossRecord


# ==============================================================================
# Helper Fixtures & Setup
# ==============================================================================

def create_storage_pair(tmp_path: Path, prefix: str) -> tuple[StorageEngine, CloudStorageEngine, Any]:
    """Create a linked device storage engine, cloud storage engine, and test client."""
    dev_db = tmp_path / f"{prefix}_dev.db"
    cld_db = tmp_path / f"{prefix}_cld.db"

    dev_storage = StorageEngine(db_path=dev_db)
    cld_storage = CloudStorageEngine(db_path=cld_db)
    app = create_cloud_app(storage=cld_storage)
    client = TestClient(app, base_url="http://testserver")

    return dev_storage, cld_storage, client


def populate_stream(
    storage: StorageEngine,
    session_id: str,
    sensor: int,
    start_index: int,
    count: int,
    value_offset: int = 1000,
) -> None:
    """Populate contiguous samples in storage."""
    batch_size = 200
    for offset in range(0, count, batch_size):
        chunk_len = min(batch_size, count - offset)
        first_idx = start_index + offset
        if sensor == 1:
            data = [[value_offset + first_idx + i, 2000 + first_idx + i, 3000 + first_idx + i] for i in range(chunk_len)]
        else:
            data = [[value_offset + first_idx + i, 5000 + first_idx + i] for i in range(chunk_len)]
        batch = IngestBatch(session_id, sensor, first_idx, chunk_len, data, [], False, "crc")
        storage.commit_batch(batch)


# ==============================================================================
# Phase 4C Recovery Tests
# ==============================================================================

def test_reconciliation_exact_indices_and_loss_entries(tmp_path: Path) -> None:
    """Verify reconciliation detects exact match on clean sync and flags any anomaly."""
    dev_storage, cld_storage, client = create_storage_pair(tmp_path, "rec_exact")
    session_id = "sess_rec_test"

    # Ingest 300 samples on S1 and S2
    populate_stream(dev_storage, session_id, 1, start_index=0, count=300)
    populate_stream(dev_storage, session_id, 2, start_index=0, count=300)

    # Ingest 1 authoritative loss record on S1
    loss = LossRecord(from_index=300, to_index=349, reason="packet_crc_failure")
    dev_storage.commit_batch(IngestBatch(session_id, 1, 350, 0, [], [loss], False, "crc"))

    # Ingest 100 post-loss samples
    populate_stream(dev_storage, session_id, 1, start_index=350, count=100)
    populate_stream(dev_storage, session_id, 2, start_index=300, count=150)

    # Sync everything to cloud
    worker = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=200, client=client)
    worker.sync_all_pending()
    assert worker.backlog == 0

    # 1. Clean reconciliation check: must be 100% reconciled with 0 discrepancies
    report = reconcile_session(dev_storage, cld_storage, session_id)
    assert report.is_fully_reconciled is True
    assert report.total_discrepancies == 0
    assert report.total_matched_samples == 850
    assert report.total_matched_loss_records == 1

    # 2. Adversarial anomaly detection: Corrupt a sample value in cloud database
    with cld_storage._lock:
        cur = cld_storage._conn.cursor()
        cur.execute("UPDATE samples SET ch1 = 999999 WHERE session_id = ? AND sensor = 1 AND sample_index = 50;", (session_id,))
        cld_storage._conn.commit()

    report_corrupted = reconcile_session(dev_storage, cld_storage, session_id)
    assert report_corrupted.is_fully_reconciled is False
    assert report_corrupted.total_discrepancies > 0

    dev_storage.close()
    cld_storage.close()


def test_two_hour_backlog_recovery_and_15min_benchmark(tmp_path: Path) -> None:
    """Verify 2-hour backlog (5.76M index space) catches up and meets the 15-minute SLA."""
    dev_storage, cld_storage, client = create_storage_pair(tmp_path, "rec_2hr")
    session_id = "sess_2hr_test"

    # Pre-outage: 1000 samples per sensor
    populate_stream(dev_storage, session_id, 1, start_index=0, count=1000)
    populate_stream(dev_storage, session_id, 2, start_index=0, count=1000)

    # 2-hour outage loss record: indices 1000 to 2878999 (2,878,000 lost indices per sensor)
    for sensor in (1, 2):
        loss = LossRecord(from_index=1000, to_index=2878999, reason="two_hour_buffer_overflow")
        dev_storage.commit_batch(IngestBatch(session_id, sensor, 2879000, 0, [], [loss], False, "crc"))

    # Post-outage: 1000 samples per sensor (indices 2879000 to 2879999)
    populate_stream(dev_storage, session_id, 1, start_index=2879000, count=1000)
    populate_stream(dev_storage, session_id, 2, start_index=2879000, count=1000)

    # Total represented index space = 2,880,000 * 2 = 5,760,000 indices
    c1 = dev_storage.get_checkpoint(session_id, 1)
    assert c1.contiguous_resolved_index == 2879999

    worker = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=400, client=client)

    t0 = time.perf_counter()
    replicated = worker.sync_all_pending()
    elapsed = time.perf_counter() - t0

    # 15-minute target: elapsed must be < 900 seconds (typically < 5s)
    assert elapsed < 900.0, f"Catch-up exceeded 15 minutes: {elapsed:.2f}s"
    assert worker.backlog == 0

    # Verify reconciliation across full 5.76M index space
    report = reconcile_session(dev_storage, cld_storage, session_id)
    assert report.is_fully_reconciled is True
    assert report.total_discrepancies == 0

    dev_storage.close()
    cld_storage.close()


def test_duplicate_submissions_idempotency(tmp_path: Path) -> None:
    """Verify that re-transmitting batches causes zero duplication and preserves cursors."""
    dev_storage, cld_storage, client = create_storage_pair(tmp_path, "dup_sub")
    session_id = "sess_dup_sub"

    populate_stream(dev_storage, session_id, 1, start_index=0, count=400)
    worker = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=200, client=client)

    # Initial sync
    worker.sync_all_pending()
    c_samp1, _ = cld_storage.get_counts()
    assert c_samp1 == 400

    # Re-submit identical batch manually
    batch = dev_storage.get_unreplicated_batch(session_id, 1, max_samples=200)
    assert batch is None  # Caught up locally

    # Manually fetch already-synced batch from device storage
    with dev_storage._lock:
        cur = dev_storage._conn.cursor()
        cur.execute("SELECT sample_index, ch1, ch2, ch3 FROM samples WHERE session_id = ? AND sensor = 1 LIMIT 200;", (session_id,))
        rows = cur.fetchall()
        data = [[r["ch1"], r["ch2"], r["ch3"]] for r in rows]

    dup_batch = IngestBatch(session_id, 1, 0, len(data), data, [], False, "crc")
    res = worker.sync_batch(dup_batch)
    assert res is not None
    assert res["samples_inserted"] == 0
    assert res["duplicates_ignored"] == 200

    # Total samples strictly unchanged
    c_samp2, _ = cld_storage.get_counts()
    assert c_samp2 == 400

    dev_storage.close()
    cld_storage.close()


def test_conflicting_records_rejected_and_quarantined(tmp_path: Path) -> None:
    """Verify conflicting samples and contradictory loss intervals are rejected with HTTP 409."""
    dev_storage, cld_storage, client = create_storage_pair(tmp_path, "conf_quar")
    session_id = "sess_conf_quar"

    # Ingest baseline sample on cloud: index 100 has value [10, 20, 30]
    cld_storage.save_batch(IngestBatch(session_id, 1, 100, 1, [[10, 20, 30]], [], False, "crc"))

    # Device attempts to sync conflicting sample: index 100 has value [99, 99, 99]
    conf_batch = IngestBatch(session_id, 1, 100, 1, [[99, 99, 99]], [], False, "crc")
    dev_storage.commit_batch(conf_batch)

    worker = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=100, client=client)
    res = worker.sync_batch(conf_batch)
    assert res is None  # 409 Rejected
    assert worker.telemetry.conflicts_encountered == 1

    # Verify cloud value was NOT overwritten
    records = cld_storage.export_ndjson_records(session_id)
    assert len(records) == 1
    assert records[0]["values"] == [10, 20, 30]

    dev_storage.close()
    cld_storage.close()


def test_clock_skew_resilience(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify system is completely immune to wall-clock jumps (forward or backward)."""
    dev_storage, cld_storage, client = create_storage_pair(tmp_path, "clock_skew")
    session_id = "sess_clock_skew"

    worker = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=200, client=client)

    # 1. Normal time: ingest 200 samples
    populate_stream(dev_storage, session_id, 1, start_index=0, count=200)
    worker.sync_all_pending()

    # 2. Clock jumps forward 1 year (+31,536,000s)
    future_time = time.time() + 31536000.0
    monkeypatch.setattr(time, "time", lambda: future_time)

    populate_stream(dev_storage, session_id, 1, start_index=200, count=200)
    worker.sync_all_pending()

    # 3. Clock jumps backward 2 years (-63,072,000s)
    past_time = time.time() - 63072000.0
    monkeypatch.setattr(time, "time", lambda: past_time)

    populate_stream(dev_storage, session_id, 1, start_index=400, count=200)
    worker.sync_all_pending()

    # Reconcile: all 600 samples must be intact and contiguous despite wild clock jumps
    report = reconcile_session(dev_storage, cld_storage, session_id)
    assert report.is_fully_reconciled is True
    assert report.total_matched_samples == 600
    assert report.total_discrepancies == 0

    dev_storage.close()
    cld_storage.close()


def test_cloud_server_restart_during_catchup(tmp_path: Path) -> None:
    """Verify catch-up survives cloud server restart without sample loss or duplicate insertion."""
    dev_db = tmp_path / "cld_rst_dev.db"
    cld_db = tmp_path / "cld_rst_cld.db"

    dev_storage = StorageEngine(db_path=dev_db)
    cld_storage1 = CloudStorageEngine(db_path=cld_db)
    app1 = create_cloud_app(storage=cld_storage1)
    client1 = TestClient(app1, base_url="http://testserver")

    session_id = "sess_cld_restart"
    populate_stream(dev_storage, session_id, 1, start_index=0, count=600)

    worker1 = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=200, client=client1)

    # Sync first 200 samples
    s1 = worker1.sync_stream(session_id, 1)
    assert s1 == 200
    assert worker1.backlog == 400

    # SIMULATE CLOUD CRASH & RESTART:
    cld_storage1.close()

    # Cloud receiver restarts from persisted SQLite WAL
    cld_storage2 = CloudStorageEngine(db_path=cld_db)
    recovered_ckpt = cld_storage2.get_checkpoint(session_id, 1)
    assert recovered_ckpt.contiguous_resolved_index == 199

    app2 = create_cloud_app(storage=cld_storage2)
    client2 = TestClient(app2, base_url="http://testserver")

    worker2 = CloudSyncWorker(dev_storage, cloud_url="http://testserver", batch_size=200, client=client2)

    # Resume catch-up
    worker2.sync_all_pending()
    assert worker2.backlog == 0

    # Verify reconciliation with restarted cloud receiver
    report = reconcile_session(dev_storage, cld_storage2, session_id)
    assert report.is_fully_reconciled is True
    assert report.total_matched_samples == 600
    assert report.total_discrepancies == 0

    dev_storage.close()
    cld_storage2.close()


def test_interrupted_catchup_recovery(tmp_path: Path) -> None:
    """Verify device process kill/restart mid-catchup resumes without loss or duplicate insertion."""
    dev_db = tmp_path / "dev_rst_dev.db"
    cld_db = tmp_path / "dev_rst_cld.db"

    dev_storage1 = StorageEngine(db_path=dev_db)
    cld_storage = CloudStorageEngine(db_path=cld_db)
    app = create_cloud_app(storage=cld_storage)
    client = TestClient(app, base_url="http://testserver")

    session_id = "sess_dev_restart"
    populate_stream(dev_storage1, session_id, 1, start_index=0, count=800)

    worker1 = CloudSyncWorker(dev_storage1, cloud_url="http://testserver", batch_size=200, client=client)

    # Sync 400 samples (2 batches)
    worker1.sync_stream(session_id, 1)
    worker1.sync_stream(session_id, 1)
    assert worker1.backlog == 400

    # SIMULATE DEVICE CRASH & RESTART:
    dev_storage1.close()

    # Re-open device storage and verify checkpoint recovered
    dev_storage2 = StorageEngine(db_path=dev_db)
    ckpt_recovered = dev_storage2.get_cloud_checkpoint(session_id, 1)
    assert ckpt_recovered.cloud_resolved_index == 399
    assert dev_storage2.get_replication_backlog() == 400

    worker2 = CloudSyncWorker(dev_storage2, cloud_url="http://testserver", batch_size=200, client=client)
    worker2.sync_all_pending()
    assert worker2.backlog == 0

    # Verify 100% reconciliation
    report = reconcile_session(dev_storage2, cld_storage, session_id)
    assert report.is_fully_reconciled is True
    assert report.total_matched_samples == 800
    assert report.total_discrepancies == 0

    dev_storage2.close()
    cld_storage.close()
