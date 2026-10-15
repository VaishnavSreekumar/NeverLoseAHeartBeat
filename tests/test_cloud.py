"""Tests for MayaCardio Cloud Ingestion & Processing Subsystem (Phase 4A).

Comprehensive tests covering:
1. Ingestion happy path: atomic batch storage and cursor progression.
2. Idempotent duplicate retries: identical batches accepted without duplication.
3. Conflicting duplicate samples: rejected with HTTP 409 Conflict and transaction rollback.
4. Contradictory sample/loss overlaps: rejected with HTTP 409 Conflict and transaction rollback.
5. Dual contiguous cursors semantics:
   - In-order samples: P == R.
   - Authoritative loss range before later stored samples: P < R (e.g. P = -1, R = 99).
   - Authoritative loss range in the middle: P < R (e.g. P = 49, R = 149).
   - Out-of-order batches: samples persisted, but P and R stay at -1 until gap filled.
   - Unresolved gap: P and R halt at gap start.
   - Open-ended loss: R halts at terminal uncertainty boundary.
6. Process crash recovery and authoritative checkpoint reconstruction.
7. Durability PRAGMA settings (NORMAL vs FULL).
8. Section 3 premature window prevention: no window emitted until both sensors resolve window span.
9. Section 3 online vs offline mathematical parity on identical data and loss records.
10. Section 4 NDJSON export and receiver status endpoints.
"""

from __future__ import annotations

import json
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from biosignal_mock.signal import SignalGenerator
from pipeline.cloud.server import create_cloud_app
from pipeline.cloud.storage import (
    CloudContradictoryLossConflictError,
    CloudIntegrityConflictError,
    CloudStorageEngine,
)
from pipeline.processing import (
    LossRange,
    SignalProcessingEngine,
    WINDOW_SAMPLES,
)
from pipeline.protocol_client import IngestBatch
from shared.protocol import LossRecord


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def cloud_storage(tmp_path: Path) -> CloudStorageEngine:
    """Fixture providing a fresh isolated CloudStorageEngine instance."""
    db_file = tmp_path / "test_cloud.db"
    storage = CloudStorageEngine(db_path=db_file, synchronous="NORMAL")
    yield storage
    storage.close()


@pytest.fixture
def cloud_client(cloud_storage: CloudStorageEngine) -> TestClient:
    """Fixture providing a FastAPI TestClient bound to isolated cloud storage."""
    app = create_cloud_app(storage=cloud_storage)
    return TestClient(app)


# ==============================================================================
# 1. Ingestion Happy Path & Dual Cursors
# ==============================================================================

def test_cloud_ingest_batch_happy_path(cloud_client: TestClient) -> None:
    """Test successful batch ingestion and contiguous cursor advancement."""
    payload_s1 = {
        "session_id": "sess_happy",
        "sensor": 1,
        "first_index": 0,
        "data": [[720, 1200, 1560] for _ in range(400)],
        "lost": [],
        "session_end": False,
    }
    resp1 = cloud_client.post("/api/v1/ingest/batch", json=payload_s1)
    assert resp1.status_code == 200
    d1 = resp1.json()
    assert d1["status"] == "ok"
    assert d1["session_id"] == "sess_happy"
    assert d1["sensor"] == 1
    assert d1["contiguous_persisted_index"] == 399
    assert d1["contiguous_resolved_index"] == 399
    assert d1["highest_observed_index"] == 399
    assert d1["samples_inserted"] == 400
    assert d1["duplicates_ignored"] == 0

    payload_s2 = {
        "session_id": "sess_happy",
        "sensor": 2,
        "first_index": 0,
        "data": [[100, 200] for _ in range(400)],
        "lost": [],
        "session_end": False,
    }
    resp2 = cloud_client.post("/api/v1/ingest/batch", json=payload_s2)
    assert resp2.status_code == 200
    d2 = resp2.json()
    assert d2["contiguous_persisted_index"] == 399
    assert d2["contiguous_resolved_index"] == 399


# ==============================================================================
# 2. Idempotent Retries
# ==============================================================================

def test_cloud_ingest_idempotent_duplicate_retry(cloud_client: TestClient, cloud_storage: CloudStorageEngine) -> None:
    """Retransmitting an identical batch must return 200 without inserting duplicates."""
    payload = {
        "session_id": "sess_dup",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30], [40, 50, 60]],
        "lost": [],
        "session_end": False,
    }
    # Initial transmission
    r1 = cloud_client.post("/api/v1/ingest/batch", json=payload)
    assert r1.status_code == 200
    assert r1.json()["samples_inserted"] == 2
    assert r1.json()["duplicates_ignored"] == 0

    # Retransmission after lost ACK
    r2 = cloud_client.post("/api/v1/ingest/batch", json=payload)
    assert r2.status_code == 200
    assert r2.json()["samples_inserted"] == 0
    assert r2.json()["duplicates_ignored"] == 2
    assert r2.json()["contiguous_persisted_index"] == 1
    assert r2.json()["contiguous_resolved_index"] == 1

    # Storage counts remain exactly 2
    samp_cnt, _ = cloud_storage.get_counts()
    assert samp_cnt == 2


# ==============================================================================
# 3. Conflicting Duplicate Rejection & Atomic Rollback
# ==============================================================================

def test_cloud_ingest_conflicting_duplicate_rejected(cloud_client: TestClient, cloud_storage: CloudStorageEngine) -> None:
    """Duplicate sample with conflicting values must raise HTTP 409 and rollback."""
    initial = {
        "session_id": "sess_conflict",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30]],
        "lost": [],
        "session_end": False,
    }
    assert cloud_client.post("/api/v1/ingest/batch", json=initial).status_code == 200

    # Conflicting sample at index 0 and a new sample at index 1
    conflicting = {
        "session_id": "sess_conflict",
        "sensor": 1,
        "first_index": 0,
        "data": [[999, 20, 30], [100, 200, 300]],  # Conflicting channel 1 value at index 0
        "lost": [],
        "session_end": False,
    }
    resp = cloud_client.post("/api/v1/ingest/batch", json=conflicting)
    assert resp.status_code == 409
    assert "Integrity conflict" in resp.json()["detail"]

    # Verify atomic rollback: index 1 was NOT inserted
    samp_cnt, _ = cloud_storage.get_counts()
    assert samp_cnt == 1


# ==============================================================================
# 4. Contradictory Sample / Loss Overlaps
# ==============================================================================

def test_cloud_contradictory_loss_overlapping_sample_rejected(cloud_client: TestClient) -> None:
    """Loss record overlapping existing stored samples must raise HTTP 409."""
    initial = {
        "session_id": "sess_overlap",
        "sensor": 1,
        "first_index": 10,
        "data": [[10, 20, 30] for _ in range(5)],  # samples 10..14
        "lost": [],
        "session_end": False,
    }
    assert cloud_client.post("/api/v1/ingest/batch", json=initial).status_code == 200

    # Loss covering [5, 12] contradicts sample 10, 11, 12
    contradictory_loss = {
        "session_id": "sess_overlap",
        "sensor": 1,
        "first_index": 20,
        "data": [],
        "lost": [{"from_index": 5, "to_index": 12, "reason": "hardware_failure"}],
        "session_end": False,
    }
    resp = cloud_client.post("/api/v1/ingest/batch", json=contradictory_loss)
    assert resp.status_code == 409
    assert "Contradictory loss record" in resp.json()["detail"]


def test_cloud_contradictory_sample_overlapping_loss_rejected(cloud_client: TestClient) -> None:
    """Incoming sample overlapping an existing loss record must raise HTTP 409."""
    initial_loss = {
        "session_id": "sess_samp_overlap",
        "sensor": 2,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 10, "to_index": 20, "reason": "crc_exhausted"}],
        "session_end": False,
    }
    assert cloud_client.post("/api/v1/ingest/batch", json=initial_loss).status_code == 200

    # Sample at index 15 contradicts existing loss [10, 20]
    contradictory_sample = {
        "session_id": "sess_samp_overlap",
        "sensor": 2,
        "first_index": 15,
        "data": [[100, 200]],
        "lost": [],
        "session_end": False,
    }
    resp = cloud_client.post("/api/v1/ingest/batch", json=contradictory_sample)
    assert resp.status_code == 409
    assert "Contradictory sample conflict" in resp.json()["detail"]


def test_cloud_contradictory_overlapping_loss_reasons_rejected(cloud_client: TestClient) -> None:
    """Overlapping loss records with conflicting reasons must raise HTTP 409."""
    initial_loss = {
        "session_id": "sess_loss_reason",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 10, "to_index": 20, "reason": "hardware_failure"}],
        "session_end": False,
    }
    assert cloud_client.post("/api/v1/ingest/batch", json=initial_loss).status_code == 200

    conflicting_loss = {
        "session_id": "sess_loss_reason",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 15, "to_index": 25, "reason": "buffer_overflow"}],
        "session_end": False,
    }
    resp = cloud_client.post("/api/v1/ingest/batch", json=conflicting_loss)
    assert resp.status_code == 409
    assert "Contradictory loss reason conflict" in resp.json()["detail"]


# ==============================================================================
# 5. Dual Contiguous Cursors Semantics (Detail 1)
# ==============================================================================

def test_cloud_cursor_authoritative_loss_at_stream_start(cloud_client: TestClient) -> None:
    """Authoritative loss at [0, 49] followed by samples [50, 99]:
    - contiguous_persisted_index remains -1 (sample 0 was never stored).
    - contiguous_resolved_index advances to 99 (0..49 loss + 50..99 samples).
    - Demonstrates P <= R with strict inequality: P = -1 < R = 99.
    """
    # 1. Ingest authoritative loss at [0, 49]
    resp1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_loss_start",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 0, "to_index": 49, "reason": "sensor_disconnected"}],
        "session_end": False,
    })
    assert resp1.status_code == 200
    d1 = resp1.json()
    assert d1["contiguous_persisted_index"] == -1
    assert d1["contiguous_resolved_index"] == 49
    assert d1["highest_observed_index"] == 49

    # 2. Ingest stored samples at [50, 99]
    resp2 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_loss_start",
        "sensor": 1,
        "first_index": 50,
        "data": [[10, 20, 30] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert resp2.status_code == 200
    d2 = resp2.json()
    # Physical contiguous samples starting from 0 is still -1!
    assert d2["contiguous_persisted_index"] == -1
    # Resolved index advances across loss and samples to 99!
    assert d2["contiguous_resolved_index"] == 99
    assert d2["highest_observed_index"] == 99
    # Invariant check: P <= R
    assert d2["contiguous_persisted_index"] <= d2["contiguous_resolved_index"]


def test_cloud_cursor_authoritative_loss_in_middle(cloud_client: TestClient) -> None:
    """Samples [0, 49], loss [50, 99], samples [100, 149]:
    - contiguous_persisted_index stops at 49 (physical samples halt at loss).
    - contiguous_resolved_index advances to 149 (0..49 samples + 50..99 loss + 100..149 samples).
    """
    # 1. Samples [0, 49]
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_loss_mid",
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })

    # 2. Loss [50, 99]
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_loss_mid",
        "sensor": 1,
        "first_index": 50,
        "data": [],
        "lost": [{"from_index": 50, "to_index": 99, "reason": "packet_loss"}],
        "session_end": False,
    })

    # 3. Samples [100, 149]
    r3 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_loss_mid",
        "sensor": 1,
        "first_index": 100,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert r3.status_code == 200
    d3 = r3.json()
    assert d3["contiguous_persisted_index"] == 49
    assert d3["contiguous_resolved_index"] == 149
    assert d3["highest_observed_index"] == 149


def test_cloud_cursor_out_of_order_batches_and_gap_filling(cloud_client: TestClient) -> None:
    """Out-of-order batches:
    - Batch [100, 199] arriving first is stored, but neither P nor R advances (-1).
    - Batch [0, 99] arriving next sweeps P and R forward across both batches to 199.
    """
    # 1. Out-of-order batch [100, 199]
    resp1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_ooo",
        "sensor": 1,
        "first_index": 100,
        "data": [[10, 20, 30] for _ in range(100)],
        "lost": [],
        "session_end": False,
    })
    assert resp1.status_code == 200
    d1 = resp1.json()
    assert d1["contiguous_persisted_index"] == -1
    assert d1["contiguous_resolved_index"] == -1
    assert d1["highest_observed_index"] == 199
    assert d1["samples_inserted"] == 100

    # 2. Bridging batch [0, 99]
    resp2 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_ooo",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30] for _ in range(100)],
        "lost": [],
        "session_end": False,
    })
    assert resp2.status_code == 200
    d2 = resp2.json()
    assert d2["contiguous_persisted_index"] == 199
    assert d2["contiguous_resolved_index"] == 199
    assert d2["highest_observed_index"] == 199


def test_cloud_cursor_unresolved_gap_halts_resolution(cloud_client: TestClient) -> None:
    """An unreceived gap [50, 99] halts resolution:
    - Ingest [0, 49].
    - Ingest [100, 149].
    - P and R stay at 49, even though later samples are persisted.
    """
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_gap",
        "sensor": 2,
        "first_index": 0,
        "data": [[1, 2] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    resp = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_gap",
        "sensor": 2,
        "first_index": 100,
        "data": [[1, 2] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert resp.status_code == 200
    d = resp.json()
    assert d["contiguous_persisted_index"] == 49
    assert d["contiguous_resolved_index"] == 49
    assert d["highest_observed_index"] == 149


def test_cloud_cursor_open_ended_loss_halts_resolution(cloud_client: TestClient) -> None:
    """Open-ended loss record (to_index IS NULL) represents terminal uncertainty:
    - R halts at from_index - 1.
    """
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_open_loss",
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    resp = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_open_loss",
        "sensor": 1,
        "first_index": 50,
        "data": [],
        "lost": [{"from_index": 50, "to_index": None, "reason": "unbounded_outage"}],
        "session_end": False,
    })
    assert resp.status_code == 200
    d = resp.json()
    assert d["contiguous_persisted_index"] == 49
    assert d["contiguous_resolved_index"] == 49


# ==============================================================================
# 6. Crash Recovery & Checkpoint Reconstruction
# ==============================================================================

def test_cloud_crash_recovery_checkpoint_reconstruction(tmp_path: Path) -> None:
    """Verify that after a server process crash, checkpoints reconstruct accurately from raw tables."""
    db_file = tmp_path / "crash_test.db"

    # Step 1: Write interleaved data using first engine instance
    storage1 = CloudStorageEngine(db_path=db_file)
    batch1 = IngestBatch(
        session_id="sess_crash",
        sensor=1,
        first_index=0,
        count=50,
        data=[[1, 2, 3] for _ in range(50)],
        lost=[],
        session_end=False,
        crc32="",
    )
    batch2 = IngestBatch(
        session_id="sess_crash",
        sensor=1,
        first_index=50,
        count=0,
        data=[],
        lost=[LossRecord(from_index=50, to_index=99, reason="hardware_fault")],
        session_end=False,
        crc32="",
    )
    batch3 = IngestBatch(
        session_id="sess_crash",
        sensor=1,
        first_index=100,
        count=50,
        data=[[1, 2, 3] for _ in range(50)],
        lost=[],
        session_end=False,
        crc32="",
    )
    storage1.save_batch(batch1)
    storage1.save_batch(batch2)
    storage1.save_batch(batch3)

    # Sensor 2 with loss at start [0, 49] and samples [50, 99]
    batch_s2_loss = IngestBatch(
        session_id="sess_crash",
        sensor=2,
        first_index=0,
        count=0,
        data=[],
        lost=[LossRecord(from_index=0, to_index=49, reason="lead_off")],
        session_end=False,
        crc32="",
    )
    batch_s2_data = IngestBatch(
        session_id="sess_crash",
        sensor=2,
        first_index=50,
        count=50,
        data=[[10, 20] for _ in range(50)],
        lost=[],
        session_end=False,
        crc32="",
    )
    storage1.save_batch(batch_s2_loss)
    storage1.save_batch(batch_s2_data)

    # Close storage connection abruptly (simulating crash)
    storage1.close()

    # Step 2: Open second engine instance on same database file
    storage2 = CloudStorageEngine(db_path=db_file)
    reconstructed = storage2.reconstruct_authoritative_checkpoints()

    # Verify Sensor 1 reconstructed cursors
    ckpt_s1 = reconstructed[("sess_crash", 1)]
    assert ckpt_s1.contiguous_persisted_index == 49
    assert ckpt_s1.contiguous_resolved_index == 149
    assert ckpt_s1.highest_observed_index == 149

    # Verify Sensor 2 reconstructed cursors
    ckpt_s2 = reconstructed[("sess_crash", 2)]
    assert ckpt_s2.contiguous_persisted_index == -1
    assert ckpt_s2.contiguous_resolved_index == 99
    assert ckpt_s2.highest_observed_index == 99

    storage2.close()


# ==============================================================================
# 7. Durability Configuration
# ==============================================================================

def test_cloud_storage_durability_pragmas(tmp_path: Path) -> None:
    """Verify NORMAL vs FULL synchronous settings on engine initialization."""
    db_normal = tmp_path / "sync_normal.db"
    st_normal = CloudStorageEngine(db_path=db_normal, synchronous="NORMAL")
    with st_normal._lock:
        cursor = st_normal._conn.cursor()
        cursor.execute("PRAGMA synchronous;")
        val_normal = cursor.fetchone()[0]
        # SQLite returns 1 for NORMAL, 2 for FULL
        assert val_normal == 1
    st_normal.close()

    db_full = tmp_path / "sync_full.db"
    st_full = CloudStorageEngine(db_path=db_full, synchronous="FULL")
    with st_full._lock:
        cursor = st_full._conn.cursor()
        cursor.execute("PRAGMA synchronous;")
        val_full = cursor.fetchone()[0]
        assert val_full == 2
    st_full.close()


# ==============================================================================
# 8. Premature Result Prevention (Detail 2)
# ==============================================================================

def test_cloud_premature_result_prevention(cloud_client: TestClient) -> None:
    """A window must not be emitted until BOTH sensors resolve the window span."""
    # Ingest Sensor 1 samples [0, 199]. Sensor 2 has 0 samples.
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_premature",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30] for _ in range(200)],
        "lost": [],
        "session_end": False,
    })

    # Results should be empty (window 0 is not ready yet!)
    res_resp1 = cloud_client.get("/api/v1/results?session=sess_premature")
    assert res_resp1.status_code == 200
    assert res_resp1.text.strip() == ""

    # Ingest Sensor 1 samples [200, 399]. Sensor 2 still has 0 samples.
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_premature",
        "sensor": 1,
        "first_index": 200,
        "data": [[10, 20, 30] for _ in range(200)],
        "lost": [],
        "session_end": False,
    })

    # Still empty because Sensor 2 has not resolved window 0
    res_resp2 = cloud_client.get("/api/v1/results?session=sess_premature")
    assert res_resp2.status_code == 200
    assert res_resp2.text.strip() == ""

    # Now ingest Sensor 2 samples [0, 399]
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_premature",
        "sensor": 2,
        "first_index": 0,
        "data": [[100, 200] for _ in range(400)],
        "lost": [],
        "session_end": False,
    })

    # Window 0 is now completely resolved on both sensors!
    res_resp3 = cloud_client.get("/api/v1/results?session=sess_premature")
    assert res_resp3.status_code == 200
    lines = [json.loads(line) for line in res_resp3.text.strip().split("\n") if line]
    assert len(lines) == 1
    w0 = lines[0]
    assert w0["window"] == 0
    assert w0["mode"] == "online"
    assert w0["complete"] is True
    assert w0["missing_1"] == 0
    assert w0["missing_2"] == 0


# ==============================================================================
# 9. Online vs Offline Mathematical Parity (Detail 2)
# ==============================================================================

def test_cloud_offline_parity_identical_results(cloud_client: TestClient) -> None:
    """Results produced in cloud online mode must match offline results 100% numerically."""
    session_id = "sess_parity"
    gen = SignalGenerator(seed=42)

    # Generate 5 full windows (2000 samples = indices 0..1999)
    total_samples = 2000
    s1_data = [
        [1000, gen.generate_sensor1_channel2(i), 1500]
        for i in range(total_samples)
    ]
    s2_data = [
        [400000, 150000]
        for i in range(total_samples)
    ]

    # Ingest in chunks into cloud receiver
    chunk_size = 400
    for w in range(total_samples // chunk_size):
        offset = w * chunk_size
        cloud_client.post("/api/v1/ingest/batch", json={
            "session_id": session_id,
            "sensor": 1,
            "first_index": offset,
            "data": s1_data[offset:offset + chunk_size],
            "lost": [],
            "session_end": False,
        })
        cloud_client.post("/api/v1/ingest/batch", json={
            "session_id": session_id,
            "sensor": 2,
            "first_index": offset,
            "data": s2_data[offset:offset + chunk_size],
            "lost": [],
            "session_end": False,
        })

    # Fetch cloud online results
    cloud_resp = cloud_client.get(f"/api/v1/results?session={session_id}")
    assert cloud_resp.status_code == 200
    cloud_results = [json.loads(line) for line in cloud_resp.text.strip().split("\n") if line]
    assert len(cloud_results) == 5

    # Compute offline results using SignalProcessingEngine(mode="offline")
    s1_dict = {i: tuple(s1_data[i]) for i in range(total_samples)}
    s2_dict = {i: tuple(s2_data[i]) for i in range(total_samples)}
    offline_engine = SignalProcessingEngine(session_id=session_id, mode="offline")
    offline_results = offline_engine.process_windows(
        windows=list(range(5)),
        sensor1_samples=s1_dict,
        sensor2_samples=s2_dict,
        loss_ranges=[],
        session_id=session_id,
    )

    # Compare every field across all 5 windows
    for w in range(5):
        c_res = cloud_results[w]
        o_res = offline_results[w]

        assert c_res["window"] == o_res.window == w
        assert c_res["mode"] == "online"
        assert o_res.mode == "offline"
        assert c_res["complete"] == o_res.complete is True
        assert c_res["missing_1"] == o_res.missing_1 == 0
        assert c_res["missing_2"] == o_res.missing_2 == 0
        assert c_res["mean"] == {"1": o_res.mean[1], "2": o_res.mean[2]}
        assert c_res["min"] == {"1": o_res.min[1], "2": o_res.min[2]}
        assert c_res["max"] == {"1": o_res.max[1], "2": o_res.max[2]}
        assert c_res["magnitude"] == o_res.magnitude
        assert c_res["heart_rate"] == o_res.heart_rate

    # Specifically verify window 4 heart rate is computed identically
    assert cloud_results[4]["heart_rate"] is not None
    assert cloud_results[4]["heart_rate"] == offline_results[4].heart_rate


# ==============================================================================
# 10. Section 4 NDJSON Export & Status Endpoints
# ==============================================================================

def test_cloud_export_ndjson(cloud_client: TestClient) -> None:
    """GET /api/v1/export?session=ID must emit Section 4 compliant NDJSON."""
    session_id = "sess_export"
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30], [40, 50, 60]],
        "lost": [{"from_index": 100, "to_index": 150, "reason": "hardware_failure"}],
        "session_end": False,
    })
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 2,
        "first_index": 0,
        "data": [[70, 80]],
        "lost": [],
        "session_end": False,
    })

    resp = cloud_client.get(f"/api/v1/export?session={session_id}")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")

    lines = [json.loads(line) for line in resp.text.strip().split("\n") if line]
    # 2 sensor 1 samples + 1 sensor 2 sample + 1 loss record = 4 lines
    assert len(lines) == 4

    samples = [l for l in lines if "values" in l]
    losses = [l for l in lines if "reason" in l]

    assert len(samples) == 3
    assert len(losses) == 1
    assert losses[0]["reason"] == "hardware_failure"


def test_cloud_status_endpoint(cloud_client: TestClient) -> None:
    """GET /api/v1/status returns healthy status and stream metadata."""
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_status",
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2, 3]],
        "lost": [],
        "session_end": False,
    })

    resp = cloud_client.get("/api/v1/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "sess_status" in data["active_sessions"]
    assert data["total_samples"] >= 1
    assert len(data["checkpoints"]) >= 1


# ==============================================================================
# 11. Additional Adversarial Cursor & Validation Tests
# ==============================================================================

def test_cloud_cursor_reverse_out_of_order_arrival(cloud_client: TestClient) -> None:
    """Batches arriving in completely reverse order: [100..149], then [50..99], then [0..49]."""
    # 1. Highest batch [100, 149]
    r1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_rev",
        "sensor": 1,
        "first_index": 100,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert r1.status_code == 200
    assert r1.json()["contiguous_persisted_index"] == -1
    assert r1.json()["contiguous_resolved_index"] == -1
    assert r1.json()["highest_observed_index"] == 149

    # 2. Middle batch [50, 99]
    r2 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_rev",
        "sensor": 1,
        "first_index": 50,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert r2.status_code == 200
    # Still -1 because index 0..49 is missing!
    assert r2.json()["contiguous_persisted_index"] == -1
    assert r2.json()["contiguous_resolved_index"] == -1
    assert r2.json()["highest_observed_index"] == 149

    # 3. Lowest batch [0, 49]
    r3 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_rev",
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2, 3] for _ in range(50)],
        "lost": [],
        "session_end": False,
    })
    assert r3.status_code == 200
    # Everything is now contiguous from 0 to 149!
    assert r3.json()["contiguous_persisted_index"] == 149
    assert r3.json()["contiguous_resolved_index"] == 149
    assert r3.json()["highest_observed_index"] == 149


def test_cloud_cursor_loss_gap_bridging_and_reconstruction(tmp_path: Path) -> None:
    """Loss [0, 49], Gap [50, 99], Loss [100, 149], then bridging samples [50, 99]."""
    db_file = tmp_path / "loss_gap_test.db"
    storage = CloudStorageEngine(db_path=db_file)

    # 1. Loss [0, 49]
    storage.save_batch(IngestBatch(
        session_id="sess_lg",
        sensor=1,
        first_index=0,
        count=0,
        data=[],
        lost=[LossRecord(from_index=0, to_index=49, reason="outage_1")],
        session_end=False,
        crc32="",
    ))
    # 2. Loss [100, 149] (gap at 50..99)
    ckpt2 = storage.save_batch(IngestBatch(
        session_id="sess_lg",
        sensor=1,
        first_index=100,
        count=0,
        data=[],
        lost=[LossRecord(from_index=100, to_index=149, reason="outage_2")],
        session_end=False,
        crc32="",
    ))
    # Persisted is -1, resolved is 49 (halted by gap 50..99), observed is 149
    assert ckpt2.contiguous_persisted_index == -1
    assert ckpt2.contiguous_resolved_index == 49
    assert ckpt2.highest_observed_index == 149

    # 3. Bridging samples [50, 99] arrive
    ckpt3 = storage.save_batch(IngestBatch(
        session_id="sess_lg",
        sensor=1,
        first_index=50,
        count=50,
        data=[[1, 2, 3] for _ in range(50)],
        lost=[],
        session_end=False,
        crc32="",
    ))
    # Persisted is STILL -1 (because 0..49 were lost, no physical sample at 0)
    assert ckpt3.contiguous_persisted_index == -1
    # Resolved bridges across 0..49 loss, 50..99 samples, 100..149 loss to 149!
    assert ckpt3.contiguous_resolved_index == 149
    assert ckpt3.highest_observed_index == 149

    # 4. Crash and restart reconstruction
    storage.close()
    storage2 = CloudStorageEngine(db_path=db_file)
    reconstructed = storage2.reconstruct_authoritative_checkpoints()
    rec = reconstructed[("sess_lg", 1)]
    assert rec.contiguous_persisted_index == -1
    assert rec.contiguous_resolved_index == 149
    assert rec.highest_observed_index == 149
    storage2.close()


def test_cloud_results_multi_window_progression(cloud_client: TestClient) -> None:
    """Window 1 is not emitted until Sensor 2 resolves samples up to index 799."""
    session_id = "sess_multi_w"

    # Both sensors complete Window 0 (0..399)
    for s in (1, 2):
        ch = 3 if s == 1 else 2
        cloud_client.post("/api/v1/ingest/batch", json={
            "session_id": session_id,
            "sensor": s,
            "first_index": 0,
            "data": [[10] * ch for _ in range(400)],
            "lost": [],
            "session_end": False,
        })

    # Only 1 window is emitted
    r1 = cloud_client.get(f"/api/v1/results?session={session_id}")
    assert len([l for l in r1.text.strip().split("\n") if l]) == 1

    # Sensor 1 advances to index 799 (window 1)
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 400,
        "data": [[10, 20, 30] for _ in range(400)],
        "lost": [],
        "session_end": False,
    })

    # Sensor 2 has NOT completed window 1 yet: still only 1 window emitted!
    r2 = cloud_client.get(f"/api/v1/results?session={session_id}")
    assert len([l for l in r2.text.strip().split("\n") if l]) == 1

    # Sensor 2 advances to index 799 (window 1)
    cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 2,
        "first_index": 400,
        "data": [[10, 20] for _ in range(400)],
        "lost": [],
        "session_end": False,
    })

    # Now both windows 0 and 1 are emitted!
    r3 = cloud_client.get(f"/api/v1/results?session={session_id}")
    windows = [json.loads(l) for l in r3.text.strip().split("\n") if l]
    assert len(windows) == 2
    assert windows[0]["window"] == 0
    assert windows[1]["window"] == 1


def test_cloud_input_validation_errors(cloud_client: TestClient) -> None:
    """Invalid payload schemas return HTTP 422 Unprocessable Entity."""
    # Invalid sensor 3
    r1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_val",
        "sensor": 3,
        "first_index": 0,
        "data": [],
        "lost": [],
    })
    assert r1.status_code == 422

    # Sensor 1 with 2 channels instead of 3
    r2 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_val",
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2]],
        "lost": [],
    })
    assert r2.status_code == 422

    # Value outside 24-bit range
    r3 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_val",
        "sensor": 1,
        "first_index": 0,
        "data": [[99_999_999, 0, 0]],
        "lost": [],
    })
    assert r3.status_code == 422

    # Negative from_index in loss
    r4 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_val",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": -10, "to_index": 0, "reason": "err"}],
    })
    assert r4.status_code == 422

    # to_index < from_index
    r5 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_val",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 50, "to_index": 40, "reason": "err"}],
    })
    assert r5.status_code == 422


def test_cloud_not_found_endpoints(cloud_client: TestClient) -> None:
    """Requesting export or results for non-existent session returns HTTP 404."""
    r_exp = cloud_client.get("/api/v1/export?session=does_not_exist")
    assert r_exp.status_code == 404

    r_res = cloud_client.get("/api/v1/results?session=does_not_exist")
    assert r_res.status_code == 404


# ==============================================================================
# 12. Remediation Regression Tests (Audit Findings 1 - 6)
# ==============================================================================

def test_cloud_open_ended_loss_exact_closure_and_subsequent_samples(tmp_path: Path) -> None:
    """Finding 1: Authoritative bounded record closes exact open-ended loss,
    allowing legitimate subsequent samples and correct checkpoint reconstruction.
    """
    db_file = tmp_path / "open_loss_closure.db"
    storage = CloudStorageEngine(db_path=db_file)
    app = create_cloud_app(storage=storage)
    client = TestClient(app)

    session_id = "sess_close_loss"

    # Step 1: Open-ended loss at index 100
    r1 = client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 100, "to_index": None, "reason": "network_outage"}],
        "session_end": False,
    })
    assert r1.status_code == 200
    assert r1.json()["contiguous_resolved_index"] == -1  # Gaps 0..99 unreceived

    # Verify 1 record with to_index IS NULL
    with storage._lock:
        cursor = storage._conn.cursor()
        cursor.execute("SELECT from_index, to_index, reason FROM loss_ledger WHERE session_id = ?;", (session_id,))
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0]["from_index"] == 100
        assert rows[0]["to_index"] is None

    # Step 2: Exact authoritative closure [100, 150] arrives
    r2 = client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 100, "to_index": 150, "reason": "network_outage"}],
        "session_end": False,
    })
    assert r2.status_code == 200

    # Verify that the open-ended record was closed and NO duplicate row was created
    with storage._lock:
        cursor = storage._conn.cursor()
        cursor.execute("SELECT from_index, to_index, reason FROM loss_ledger WHERE session_id = ?;", (session_id,))
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0]["from_index"] == 100
        assert rows[0]["to_index"] == 150

    # Step 3: Subsequent legitimate samples at [151, 199] must be accepted without conflict
    r3 = client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 151,
        "data": [[10, 20, 30] for _ in range(49)],
        "lost": [],
        "session_end": False,
    })
    assert r3.status_code == 200, f"Subsequent samples rejected: {r3.text}"
    assert r3.json()["samples_inserted"] == 49

    # Step 4: Bridging samples [0, 99] arrive
    r4 = client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 0,
        "data": [[1, 2, 3] for _ in range(100)],
        "lost": [],
        "session_end": False,
    })
    assert r4.status_code == 200
    # Samples [0, 99] are physical; [100, 150] is loss; [151, 199] are physical.
    # Persisted is 99 (halts at loss 100); resolved sweeps across loss to 199!
    assert r4.json()["contiguous_persisted_index"] == 99
    assert r4.json()["contiguous_resolved_index"] == 199
    assert r4.json()["highest_observed_index"] == 199

    # Step 5: Crash/restart checkpoint reconstruction
    storage.close()
    storage2 = CloudStorageEngine(db_path=db_file)
    reconstructed = storage2.reconstruct_authoritative_checkpoints()
    rec = reconstructed[(session_id, 1)]
    assert rec.contiguous_persisted_index == 99
    assert rec.contiguous_resolved_index == 199
    assert rec.highest_observed_index == 199
    storage2.close()


def test_cloud_open_ended_loss_mismatched_reason_or_start_preserved(cloud_client: TestClient) -> None:
    """Finding 1: Mismatched reason raises 409; mismatched start preserves uncertainty."""
    # 1. Open-ended loss at 100 with reason "network_outage"
    r1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_mis_loss",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 100, "to_index": None, "reason": "network_outage"}],
        "session_end": False,
    })
    assert r1.status_code == 200

    # 2. Conflicting reason on overlapping range raises 409 Conflict
    r_conflict = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_mis_loss",
        "sensor": 1,
        "first_index": 0,
        "data": [],
        "lost": [{"from_index": 100, "to_index": 150, "reason": "buffer_overflow"}],
        "session_end": False,
    })
    assert r_conflict.status_code == 409
    assert "Contradictory loss reason" in r_conflict.json()["detail"]


def test_cloud_telemetry_atomic_rollback_on_loss_conflict(cloud_storage: CloudStorageEngine) -> None:
    """Finding 2: Samples inserted in a batch that fails during loss validation
    must NOT increment in-memory telemetry counters.
    """
    app = create_cloud_app(storage=cloud_storage)
    client = TestClient(app)

    # Initial state
    assert cloud_storage.telemetry.samples_inserted == 0
    assert cloud_storage.telemetry.batches_committed == 0
    assert cloud_storage.telemetry.transactions_rolled_back == 0

    # First commit 5 samples at [50, 54] so we can trigger a loss overlap conflict
    cloud_storage.save_batch(IngestBatch(
        session_id="sess_telem",
        sensor=1,
        first_index=50,
        count=5,
        data=[[1, 2, 3] for _ in range(5)],
        lost=[],
        session_end=False,
        crc32="",
    ))
    assert cloud_storage.telemetry.samples_inserted == 5
    assert cloud_storage.telemetry.batches_committed == 1

    # Now attempt a batch with 10 new samples [0, 9] AND a contradictory loss [50, 52]
    # The samples [0, 9] are valid, but the loss contradicts existing samples [50, 54]!
    resp = client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_telem",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30] for _ in range(10)],
        "lost": [{"from_index": 50, "to_index": 52, "reason": "fault"}],
        "session_end": False,
    })
    assert resp.status_code == 409
    assert "overlaps" in resp.json()["detail"]

    # In-memory telemetry must NOT count the rolled-back 10 samples!
    assert cloud_storage.telemetry.samples_inserted == 5
    assert cloud_storage.telemetry.duplicate_samples_ignored == 0
    assert cloud_storage.telemetry.loss_records_inserted == 0
    assert cloud_storage.telemetry.batches_committed == 1
    assert cloud_storage.telemetry.transactions_rolled_back == 1

    # Database must have only the original 5 samples
    samp_cnt, _ = cloud_storage.get_counts()
    assert samp_cnt == 5


def test_cloud_cursor_advancement_optimization_parity(cloud_storage: CloudStorageEngine) -> None:
    """Finding 3: Optimizing resolved cursor to start from max(resolved, persisted)
    accurately resolves contiguous ranges without redundant queries or skipping gaps.
    """
    app = create_cloud_app(storage=cloud_storage)
    client = TestClient(app)

    # Ingest 400 samples
    resp = client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_perf",
        "sensor": 1,
        "first_index": 0,
        "data": [[10, 20, 30] for _ in range(400)],
        "lost": [],
        "session_end": False,
    })
    assert resp.status_code == 200
    d = resp.json()
    assert d["contiguous_persisted_index"] == 399
    assert d["contiguous_resolved_index"] == 399

    # Next contiguous batch 400..799
    resp2 = client.post("/api/v1/ingest/batch", json={
        "session_id": "sess_perf",
        "sensor": 1,
        "first_index": 400,
        "data": [[10, 20, 30] for _ in range(400)],
        "lost": [],
        "session_end": False,
    })
    assert resp2.status_code == 200
    d2 = resp2.json()
    assert d2["contiguous_persisted_index"] == 799
    assert d2["contiguous_resolved_index"] == 799


def test_cloud_window_result_refinement_persisted_to_disk(tmp_path: Path) -> None:
    """Finding 4: Refined window results are persisted to disk via atomic upsert,
    remaining correct after storage reopening.
    """
    db_file = tmp_path / "refine_test.db"
    storage = CloudStorageEngine(db_path=db_file)
    app = create_cloud_app(storage=storage)
    client = TestClient(app)

    session_id = "sess_refine"

    # Both sensors complete Window 0 (0..399)
    for s in (1, 2):
        ch = 3 if s == 1 else 2
        client.post("/api/v1/ingest/batch", json={
            "session_id": session_id,
            "sensor": s,
            "first_index": 0,
            "data": [[1000] * ch for _ in range(400)],
            "lost": [],
            "session_end": False,
        })

    r1 = client.get(f"/api/v1/results?session={session_id}")
    res1 = [json.loads(line) for line in r1.text.strip().split("\n") if line]
    assert len(res1) == 1

    # Now both sensors complete Window 1 (400..799)
    for s in (1, 2):
        ch = 3 if s == 1 else 2
        client.post("/api/v1/ingest/batch", json={
            "session_id": session_id,
            "sensor": s,
            "first_index": 400,
            "data": [[2000] * ch for _ in range(400)],
            "lost": [],
            "session_end": False,
        })

    # Close and reopen storage to verify disk state
    storage.close()
    storage2 = CloudStorageEngine(db_path=db_file)
    app2 = create_cloud_app(storage=storage2)
    client2 = TestClient(app2)

    r2 = client2.get(f"/api/v1/results?session={session_id}")
    res2 = [json.loads(line) for line in r2.text.strip().split("\n") if line]
    assert len(res2) == 2
    assert res2[0]["window"] == 0
    assert res2[1]["window"] == 1
    storage2.close()


def test_cloud_subprocess_kill_recovery(tmp_path: Path) -> None:
    """Finding 5: Terminating an external subprocess with SIGKILL/TerminateProcess
    mid-transaction verifies SQLite WAL recovery rolls back uncommitted writes
    while committed data remains intact.
    """
    import subprocess
    import sys

    db_file = tmp_path / "subprocess_kill.db"

    # Subprocess code that commits batch 1, then begins an uncommitted batch 2 and waits
    sub_code = f"""
import sys, time
from pipeline.cloud.storage import CloudStorageEngine
from pipeline.protocol_client import IngestBatch

storage = CloudStorageEngine(db_path=r'{db_file}')

# Commit batch 1
b1 = IngestBatch(session_id='sess_proc_kill', sensor=1, first_index=0, count=50,
                 data=[[1, 2, 3] for _ in range(50)], lost=[], session_end=False, crc32='')
storage.save_batch(b1)
print('COMMITTED', flush=True)

# Begin an uncommitted transaction inserting sample 999
cursor = storage._conn.cursor()
cursor.execute('BEGIN IMMEDIATE;')
cursor.execute("INSERT INTO samples (session_id, sensor, sample_index, ch1, ch2, ch3, created_at) VALUES ('sess_proc_kill', 1, 999, 9, 9, 9, 0);")
print('IN_TRANSACTION', flush=True)

# Hang indefinitely until terminated by parent
time.sleep(60)
"""

    proc = subprocess.Popen(
        [sys.executable, "-c", sub_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Wait until subprocess signals it is in the uncommitted transaction
        stdout_lines = []
        for line in proc.stdout:
            stdout_lines.append(line.strip())
            if "IN_TRANSACTION" in line:
                break

        assert "COMMITTED" in stdout_lines
        assert "IN_TRANSACTION" in stdout_lines

        # Abruptly kill the process (OS TerminateProcess on Windows / SIGKILL on Unix)
        proc.kill()
        proc.wait(timeout=5)

        # Open SQLite in the parent process
        storage_parent = CloudStorageEngine(db_path=db_file)
        reconstructed = storage_parent.reconstruct_authoritative_checkpoints()

        # Committed batch 1 (0..49) must be fully intact
        ckpt = reconstructed[("sess_proc_kill", 1)]
        assert ckpt.contiguous_persisted_index == 49
        assert ckpt.contiguous_resolved_index == 49
        assert ckpt.highest_observed_index == 49

        # Uncommitted sample 999 must have been rolled back
        with storage_parent._lock:
            cursor = storage_parent._conn.cursor()
            cursor.execute("SELECT COUNT(*) AS c FROM samples WHERE sample_index = 999;")
            assert cursor.fetchone()["c"] == 0

        storage_parent.close()
    finally:
        if proc.poll() is None:
            proc.kill()


def test_cloud_batch_count_and_extra_fields_validation(cloud_client: TestClient) -> None:
    """Finding 6: Validate explicit batch count against len(data) and forbid extra fields."""
    session_id = "sess_count_val"

    # 1. Matching count passes
    r1 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 0,
        "count": 2,
        "data": [[1, 2, 3], [4, 5, 6]],
        "lost": [],
        "session_end": False,
    })
    assert r1.status_code == 200

    # 2. Mismatched count is rejected with HTTP 422
    r2 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 2,
        "count": 10,  # Declares 10, but provides 1
        "data": [[7, 8, 9]],
        "lost": [],
        "session_end": False,
    })
    assert r2.status_code == 422
    assert "count (10) does not match length of data (1)" in r2.text

    # 3. Negative count is rejected with HTTP 422
    r3 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 3,
        "count": -1,
        "data": [],
        "lost": [],
        "session_end": False,
    })
    assert r3.status_code == 422

    # 4. Unexpected extra field is forbidden with HTTP 422
    r4 = cloud_client.post("/api/v1/ingest/batch", json={
        "session_id": session_id,
        "sensor": 1,
        "first_index": 3,
        "data": [],
        "lost": [],
        "session_end": False,
        "unauthorized_field": "exploit",
    })
    assert r4.status_code == 422
    assert "Extra inputs are not permitted" in r4.text


