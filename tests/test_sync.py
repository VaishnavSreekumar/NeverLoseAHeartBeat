"""Tests for MayaCardio Cloud Synchronization and Mode State Machine (Phase 4B).

Validates:
1. Local durable cloud replication checkpoints in StorageEngine.
2. Un-replicated batch extraction with sample layout and authoritative loss intervals.
3. Live HTTP synchronization via CloudSyncWorker to FastAPI cloud receiver.
4. Idempotent re-transmission and duplicate protection (zero sample loss, zero sample duplication).
5. Online/Offline state machine automatic switching within bounded time.
6. Catch-up synchronization following simulated network outage.
7. Device CLI status reporting accurate cloud counts and backlog in online and offline modes.
8. Device CLI export --from cloud matching Section 4 schema and parity with local export.
9. Session isolation across multiple sessions.
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

from pipeline.cli import main
from pipeline.cloud.server import create_cloud_app
from pipeline.cloud.storage import CloudStorageEngine
from pipeline.protocol_client import IngestBatch
from pipeline.storage import CloudReplicationCheckpoint, StorageEngine
from pipeline.sync import CloudSyncWorker, SyncTelemetry
from shared.protocol import LossRecord


# ==============================================================================
# Helper Fixtures & Functions
# ==============================================================================

def create_local_storage(db_path: Path) -> StorageEngine:
    """Create local device storage engine."""
    return StorageEngine(db_path=db_path)


def populate_device_samples(
    storage: StorageEngine,
    session_id: str,
    sensor: int,
    start_index: int,
    count: int,
) -> None:
    """Populate local storage with simulated biosignal samples."""
    channels = 3 if sensor == 1 else 2
    batch_size = 200
    for offset in range(0, count, batch_size):
        chunk_count = min(batch_size, count - offset)
        first_idx = start_index + offset
        data: list[list[int]] = []
        for i in range(chunk_count):
            idx = first_idx + i
            if channels == 3:
                data.append([1000 + idx, 2000 + idx, 3000 + idx])
            else:
                data.append([4000 + idx, 5000 + idx])

        batch = IngestBatch(
            session_id=session_id,
            sensor=sensor,
            first_index=first_idx,
            count=len(data),
            data=data,
            lost=[],
            session_end=(offset + chunk_count >= count),
            crc32="test_crc",
        )
        storage.commit_batch(batch)


class FlakyClient(TestClient):
    """TestClient with simulated network connectivity control."""

    def __init__(self, app: Any, **kwargs: Any) -> None:
        super().__init__(app, **kwargs)
        self.network_alive = True

    def request(self, *args: Any, **kwargs: Any) -> httpx.Response:
        if not self.network_alive:
            raise httpx.ConnectError("Simulated network outage (cable cut)")
        return super().request(*args, **kwargs)


# ==============================================================================
# 1. Local Cloud Replication Checkpoints
# ==============================================================================

def test_cloud_checkpoints_persistence_and_recovery(tmp_path: Path) -> None:
    """Verify cloud checkpoints persist and survive engine restart."""
    db_file = tmp_path / "local_ckpt.db"
    storage = create_local_storage(db_file)

    # Initial checkpoint is uninitialized (-1)
    ckpt0 = storage.get_cloud_checkpoint("sess_1", 1)
    assert ckpt0.cloud_persisted_index == -1
    assert ckpt0.cloud_resolved_index == -1
    assert ckpt0.highest_observed_index == -1

    # Update checkpoint
    t0 = time.time()
    storage.update_cloud_checkpoint("sess_1", 1, 99, 99, 99, acked_at=t0)
    storage.update_cloud_checkpoint("sess_1", 2, 49, 49, 49, acked_at=t0)

    # Verify query
    c1 = storage.get_cloud_checkpoint("sess_1", 1)
    assert c1.cloud_persisted_index == 99
    assert c1.cloud_resolved_index == 99

    c2 = storage.get_cloud_checkpoint("sess_1", 2)
    assert c2.cloud_persisted_index == 49
    assert c2.cloud_resolved_index == 49

    storage.close()

    # Restart storage engine and verify recovery
    storage2 = create_local_storage(db_file)
    c1_restored = storage2.get_cloud_checkpoint("sess_1", 1)
    assert c1_restored.cloud_persisted_index == 99
    assert c1_restored.cloud_resolved_index == 99
    storage2.close()


def test_get_unreplicated_batch_samples_and_loss(tmp_path: Path) -> None:
    """Verify get_unreplicated_batch extracts bounded samples and authoritative loss."""
    db_file = tmp_path / "unrep.db"
    storage = create_local_storage(db_file)

    # Populate 100 samples on sensor 1
    populate_device_samples(storage, "sess_loss_test", 1, start_index=0, count=100)

    # Add loss record for indices 100..149
    loss_batch = IngestBatch(
        session_id="sess_loss_test",
        sensor=1,
        first_index=100,
        count=0,
        data=[],
        lost=[LossRecord(from_index=100, to_index=149, reason="sensor_disconnect")],
        session_end=False,
        crc32="",
    )
    storage.commit_batch(loss_batch)

    # Populate subsequent samples 150..249
    populate_device_samples(storage, "sess_loss_test", 1, start_index=150, count=100)

    # Checkpoint is at -1; first unreplicated batch should be samples 0..99
    b1 = storage.get_unreplicated_batch("sess_loss_test", 1, max_samples=400)
    assert b1 is not None
    assert b1.first_index == 0
    assert len(b1.data) == 100
    assert len(b1.lost) == 0

    # Advance cloud checkpoint to 99
    storage.update_cloud_checkpoint("sess_loss_test", 1, 99, 99, 99)

    # Next unreplicated batch MUST be the authoritative loss record [100, 149]
    b2 = storage.get_unreplicated_batch("sess_loss_test", 1, max_samples=400)
    assert b2 is not None
    assert len(b2.lost) == 1
    assert b2.lost[0].from_index == 100
    assert b2.lost[0].to_index == 149
    assert b2.lost[0].reason == "sensor_disconnect"
    assert len(b2.data) == 0

    # Advance cloud checkpoint past loss to 149
    storage.update_cloud_checkpoint("sess_loss_test", 1, 99, 149, 149)

    # Next unreplicated batch should be samples 150..249
    b3 = storage.get_unreplicated_batch("sess_loss_test", 1, max_samples=400)
    assert b3 is not None
    assert b3.first_index == 150
    assert len(b3.data) == 100

    # Advance cloud checkpoint to 249
    storage.update_cloud_checkpoint("sess_loss_test", 1, 249, 249, 249)

    # Now fully caught up; next call returns None
    assert storage.get_unreplicated_batch("sess_loss_test", 1) is None
    storage.close()


# ==============================================================================
# 2. CloudSyncWorker & State Machine Tests
# ==============================================================================

def test_sync_worker_clean_stream(tmp_path: Path) -> None:
    """Verify live synchronization replicates entire stream cleanly to cloud."""
    local_db = tmp_path / "dev_clean.db"
    cloud_db = tmp_path / "cld_clean.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Populate 500 samples on sensor 1 and 500 samples on sensor 2
    populate_device_samples(local_storage, "sess_sync_clean", 1, start_index=0, count=500)
    populate_device_samples(local_storage, "sess_sync_clean", 2, start_index=0, count=500)

    assert local_storage.get_replication_backlog() == 1000

    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=200,
        client=client,
    )

    total_synced = worker.sync_all_pending()
    assert total_synced == 1000
    assert worker.backlog == 0

    # Verify cloud storage counts
    c_samp, c_loss = cloud_storage.get_counts()
    assert c_samp == 1000
    assert c_loss == 0

    # Verify cloud checkpoints
    ckpt1 = cloud_storage.get_checkpoint("sess_sync_clean", 1)
    assert ckpt1.contiguous_persisted_index == 499
    assert ckpt1.contiguous_resolved_index == 499

    ckpt2 = cloud_storage.get_checkpoint("sess_sync_clean", 2)
    assert ckpt2.contiguous_persisted_index == 499
    assert ckpt2.contiguous_resolved_index == 499

    local_storage.close()
    cloud_storage.close()


def test_idempotent_duplicate_retry_and_zero_duplication(tmp_path: Path) -> None:
    """Verify duplicate re-transmissions are acknowledged idempotently with zero sample duplication."""
    local_db = tmp_path / "dev_dup.db"
    cloud_db = tmp_path / "cld_dup.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    populate_device_samples(local_storage, "sess_dup", 1, start_index=0, count=200)

    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=200,
        client=client,
    )

    # Initial sync
    batch = local_storage.get_unreplicated_batch("sess_dup", 1, max_samples=200)
    assert batch is not None
    res1 = worker.sync_batch(batch)
    assert res1 is not None
    assert res1["samples_inserted"] == 200
    assert res1["duplicates_ignored"] == 0

    # Re-send identical batch (simulating lost response or client retry)
    res2 = worker.sync_batch(batch)
    assert res2 is not None
    assert res2["samples_inserted"] == 0
    assert res2["duplicates_ignored"] == 200

    # Total samples in cloud strictly unchanged
    c_samp, _ = cloud_storage.get_counts()
    assert c_samp == 200

    local_storage.close()
    cloud_storage.close()


def test_network_interruption_and_mode_switching(tmp_path: Path) -> None:
    """Verify mode state machine switches to OFFLINE after threshold, and back to ONLINE on restore."""
    local_db = tmp_path / "dev_net.db"
    cloud_db = tmp_path / "cld_net.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    populate_device_samples(local_storage, "sess_switch", 1, start_index=0, count=400)

    client = FlakyClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=200,
        offline_threshold_s=0.2,
        client=client,
    )

    assert worker.mode == "online"

    # Sync first 200 samples
    synced = worker.sync_stream("sess_switch", 1)
    assert synced == 200
    assert worker.backlog == 200
    assert worker.mode == "online"

    # CUT NETWORK
    client.network_alive = False

    # Attempt sync; should fail
    fail_synced = worker.sync_stream("sess_switch", 1)
    assert fail_synced == 0

    # Sleep past the 0.2s threshold
    time.sleep(0.25)

    # Trigger health check; state machine MUST transition to offline
    worker.check_cloud_health()
    assert worker.mode == "offline"

    # Samples remain durably stored locally, not lost
    assert worker.backlog == 200

    # RESTORE NETWORK
    client.network_alive = True

    # Health check restores ONLINE mode immediately (<10s requirement)
    assert worker.check_cloud_health() is True
    assert worker.mode == "online"

    # Catch up backlog completely
    worker.sync_all_pending()
    assert worker.backlog == 0

    # Verify all 400 samples present in cloud
    c_samp, _ = cloud_storage.get_counts()
    assert c_samp == 400

    local_storage.close()
    cloud_storage.close()


# ==============================================================================
# 3. CLI Status and Export Parity Tests
# ==============================================================================

def test_cli_status_with_cloud_integration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pipeline status accurately reflects cloud counts and backlog."""
    local_db = tmp_path / "cli_status_dev.db"
    cloud_db = tmp_path / "cli_status_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    populate_device_samples(local_storage, "sess_cli", 1, start_index=0, count=300)
    local_storage.close()

    # 1. Status before any replication (offline, backlog=None as unconfigured)
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://unreachable.test:9999"])
    assert code == 0
    cap = capsys.readouterr()
    st = json.loads(cap.out.strip())
    assert st["mode"] == "offline"
    assert st["samples_saved"] == 300
    assert st["samples_in_cloud"] is None
    assert st["backlog"] is None

    # 2. Replicate 100 samples using worker
    local_storage = create_local_storage(local_db)
    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=100,
        client=client,
    )
    worker.sync_stream("sess_cli", 1)
    assert worker.backlog == 200
    local_storage.close()

    # 3. Status when cloud receiver is unreachable but local cloud_checkpoints exists:
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://unreachable.test:9999"])
    assert code == 0
    cap = capsys.readouterr()
    st2 = json.loads(cap.out.strip())
    assert st2["mode"] == "offline"
    assert st2["samples_saved"] == 300
    assert st2["samples_in_cloud"] == 100
    assert st2["backlog"] == 200

    # 4. Status when cloud receiver IS reachable: reports ONLINE and live counts
    monkeypatch.setattr("httpx.Client", lambda *args, **kwargs: TestClient(app, base_url="http://localhost:8000"))
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://localhost:8000"])
    assert code == 0
    cap = capsys.readouterr()
    st3 = json.loads(cap.out.strip())
    assert st3["mode"] == "online"
    assert st3["samples_saved"] == 300
    assert st3["samples_in_cloud"] == 100
    assert st3["backlog"] == 200

    # 5. Status when authentication is enabled on cloud receiver
    monkeypatch.setenv("CLOUD_API_KEY", "cli-test-secret")
    # Without api key -> cloud probe returns 401, falls back to offline
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://localhost:8000"])
    assert code == 0
    st4 = json.loads(capsys.readouterr().out.strip())
    assert st4["mode"] == "offline"

    # With correct --api-key -> succeeds with online mode
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://localhost:8000", "--api-key", "cli-test-secret"])
    assert code == 0
    st5 = json.loads(capsys.readouterr().out.strip())
    assert st5["mode"] == "online"
    assert st5["samples_in_cloud"] == 100

    cloud_storage.close()


def test_cli_export_from_cloud_parity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pipeline export --from cloud streams identical NDJSON matching Section 4 schema."""
    local_db = tmp_path / "exp_dev.db"
    cloud_db = tmp_path / "exp_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Ingest 200 samples and 1 loss record locally
    populate_device_samples(local_storage, "sess_parity", 1, start_index=0, count=200)
    loss_batch = IngestBatch(
        session_id="sess_parity",
        sensor=1,
        first_index=200,
        count=0,
        data=[],
        lost=[LossRecord(from_index=200, to_index=249, reason="packet_corruption")],
        session_end=True,
        crc32="",
    )
    local_storage.commit_batch(loss_batch)

    # Replicate everything to cloud
    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=400,
        client=client,
    )
    worker.sync_all_pending()
    assert worker.backlog == 0

    # 1. Export --from device
    code_dev = main(["export", "--session", "sess_parity", "--from", "device", "--db", str(local_db)])
    assert code_dev == 0
    dev_out = capsys.readouterr().out.strip().splitlines()
    assert len(dev_out) == 201  # 200 samples + 1 loss record

    # 2. Export --from cloud
    monkeypatch.setattr("httpx.Client", lambda *args, **kwargs: TestClient(app, base_url="http://testserver"))
    code_cld = main(["export", "--session", "sess_parity", "--from", "cloud", "--cloud-url", "http://testserver"])
    assert code_cld == 0
    cld_out = capsys.readouterr().out.strip().splitlines()
    assert len(cld_out) == 201

    # Exact parity line by line
    for d_line, c_line in zip(dev_out, cld_out):
        d_obj = json.loads(d_line)
        c_obj = json.loads(c_line)
        assert d_obj == c_obj

    local_storage.close()
    cloud_storage.close()


def test_session_isolation_across_multiple_sessions(tmp_path: Path) -> None:
    """Verify multiple concurrent sessions synchronize with strict data and checkpoint isolation."""
    local_db = tmp_path / "iso_dev.db"
    cloud_db = tmp_path / "iso_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Ingest distinct data for session A and session B
    populate_device_samples(local_storage, "session_AAA", 1, start_index=0, count=150)
    populate_device_samples(local_storage, "session_BBB", 1, start_index=0, count=350)

    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=100,
        client=client,
    )

    worker.sync_all_pending()
    assert worker.backlog == 0

    # Checkpoints for A and B are completely independent
    ck_a = cloud_storage.get_checkpoint("session_AAA", 1)
    ck_b = cloud_storage.get_checkpoint("session_BBB", 1)
    assert ck_a.contiguous_resolved_index == 149
    assert ck_b.contiguous_resolved_index == 349

    # Sample queries are isolated
    samples_a = cloud_storage.export_ndjson_records("session_AAA")
    samples_b = cloud_storage.export_ndjson_records("session_BBB")
    assert len(samples_a) == 150
    assert len(samples_b) == 350

    local_storage.close()
    cloud_storage.close()


def test_sync_response_loss_idempotent_recovery(tmp_path: Path) -> None:
    """Verify that if a response is lost after server commits, re-transmitting causes zero duplication."""
    local_db = tmp_path / "resp_loss_dev.db"
    cloud_db = tmp_path / "resp_loss_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    populate_device_samples(local_storage, "sess_loss_resp", 1, start_index=0, count=200)

    # Custom transport that drops the response on first POST
    post_count = 0
    real_client = TestClient(app, base_url="http://testserver")

    class DropFirstResponseClient(TestClient):
        def request(self, *args: Any, **kwargs: Any) -> httpx.Response:
            nonlocal post_count
            resp = super().request(*args, **kwargs)
            if args and args[0] == "POST":
                post_count += 1
                if post_count == 1:
                    # Server committed, but response connection died before returning to client!
                    raise httpx.ReadTimeout("Simulated server response timeout")
            return resp

    client = DropFirstResponseClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=200,
        client=client,
    )

    # First attempt: sends batch, server commits 200 samples, but client experiences ReadTimeout
    batch = local_storage.get_unreplicated_batch("sess_loss_resp", 1, max_samples=200)
    assert batch is not None
    res1 = worker.sync_batch(batch)
    assert res1 is None  # Timed out

    # Local checkpoint did NOT advance because ACK was not received
    ckpt_local = local_storage.get_cloud_checkpoint("sess_loss_resp", 1)
    assert ckpt_local.cloud_resolved_index == -1

    # But cloud database already has the 200 samples
    c_samp, _ = cloud_storage.get_counts()
    assert c_samp == 200

    # Second attempt: re-send identical batch
    res2 = worker.sync_batch(batch)
    assert res2 is not None
    assert res2["samples_inserted"] == 0
    assert res2["duplicates_ignored"] == 200
    assert res2["contiguous_resolved_index"] == 199

    # Now local checkpoint advances cleanly to 199
    ckpt_local2 = local_storage.get_cloud_checkpoint("sess_loss_resp", 1)
    assert ckpt_local2.cloud_resolved_index == 199

    # Cloud total samples strictly still 200 (ZERO sample duplication)
    c_samp_final, _ = cloud_storage.get_counts()
    assert c_samp_final == 200

    local_storage.close()
    cloud_storage.close()


def test_sync_restart_catchup_continuity(tmp_path: Path) -> None:
    """Verify that a worker crash mid-catch-up resumes from durable checkpoint on restart."""
    local_db = tmp_path / "restart_dev.db"
    cloud_db = tmp_path / "restart_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Populate 600 samples
    populate_device_samples(local_storage, "sess_restart", 1, start_index=0, count=600)
    assert local_storage.get_replication_backlog() == 600

    client = TestClient(app, base_url="http://testserver")
    worker1 = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=200,
        client=client,
    )

    # Sync first 200 samples
    s1 = worker1.sync_stream("sess_restart", 1)
    assert s1 == 200
    assert worker1.backlog == 400

    # Simulate crash / process shutdown
    local_storage.close()

    # Reopen storage and start fresh worker
    local_storage2 = create_local_storage(local_db)
    assert local_storage2.get_replication_backlog() == 400
    ckpt_recovered = local_storage2.get_cloud_checkpoint("sess_restart", 1)
    assert ckpt_recovered.cloud_resolved_index == 199

    worker2 = CloudSyncWorker(
        storage=local_storage2,
        cloud_url="http://testserver",
        batch_size=200,
        client=client,
    )

    # Resume catch-up
    total = worker2.sync_all_pending()
    assert total == 400
    assert worker2.backlog == 0

    c_samp, _ = cloud_storage.get_counts()
    assert c_samp == 600

    local_storage2.close()
    cloud_storage.close()


def test_sync_conflict_quarantine(tmp_path: Path) -> None:
    """Verify that conflicting records raise HTTP 409 and worker halts stream advancement."""
    local_db = tmp_path / "conf_dev.db"
    cloud_db = tmp_path / "conf_cld.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Ingest sample at index 0 on cloud with values [1, 2, 3]
    cloud_batch = IngestBatch(
        session_id="sess_conf",
        sensor=1,
        first_index=0,
        count=1,
        data=[[1, 2, 3]],
        lost=[],
        session_end=False,
        crc32="",
    )
    cloud_storage.save_batch(cloud_batch)

    # Device has conflicting sample at index 0 with values [999, 999, 999]
    dev_batch = IngestBatch(
        session_id="sess_conf",
        sensor=1,
        first_index=0,
        count=1,
        data=[[999, 999, 999]],
        lost=[],
        session_end=False,
        crc32="",
    )
    local_storage.commit_batch(dev_batch)

    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=100,
        client=client,
    )

    # Attempt sync: cloud MUST reject with 409 Conflict
    res = worker.sync_batch(dev_batch)
    assert res is None
    assert worker.telemetry.conflicts_encountered == 1

    # Checkpoint on device is NOT advanced
    ckpt = local_storage.get_cloud_checkpoint("sess_conf", 1)
    assert ckpt.cloud_resolved_index == -1

    local_storage.close()
    cloud_storage.close()


def test_cli_status_auth_regression_reproduction(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test reproducing and validating the status reporting issue.

    Scenario:
    - Device DB has 5820 samples saved and stored cloud_checkpoints at index 2909 for both sensors (5820 replicated).
    - Cloud DB has 5820 samples persisted for the session.
    - Cloud receiver has CLOUD_API_KEY authentication enabled.
    - An unauthenticated status check receives HTTP 401:
      * mode remains 'offline' (not forced to online despite sample count equality).
      * samples_in_cloud reports 5820 from local checkpoints.
      * backlog reports 0.
      * stderr clearly warns about the 401 Unauthorized probe failure.
    - When authenticated with --api-key, mode transitions to 'online'.
    """
    local_db = tmp_path / "repro_device.db"
    cloud_db = tmp_path / "repro_cloud.db"

    local_storage = create_local_storage(local_db)
    cloud_storage = CloudStorageEngine(cloud_db)
    app = create_cloud_app(storage=cloud_storage)

    # Populate 2910 samples for sensor 1 and 2910 samples for sensor 2 (5820 total)
    populate_device_samples(local_storage, "sess_repro", 1, start_index=0, count=2910)
    populate_device_samples(local_storage, "sess_repro", 2, start_index=0, count=2910)

    # Replicate both sensors to cloud to achieve sample_saved == samples_in_cloud (5820)
    client = TestClient(app, base_url="http://testserver")
    worker = CloudSyncWorker(
        storage=local_storage,
        cloud_url="http://testserver",
        batch_size=500,
        client=client,
    )
    worker.sync_all_pending()
    assert worker.backlog == 0
    local_storage.close()

    # Enable API key authentication on the cloud receiver
    monkeypatch.setenv("CLOUD_API_KEY", "repro-secret-auth-key-999")
    monkeypatch.setattr("httpx.Client", lambda *args, **kwargs: TestClient(app, base_url="http://localhost:8000"))

    # 1. Unauthenticated invocation: probe returns 401 -> mode is offline, samples_in_cloud from local checkpoint
    code = main(["status", "--db", str(local_db), "--cloud-url", "http://localhost:8000"])
    assert code == 0
    captured = capsys.readouterr()
    st_unauth = json.loads(captured.out.strip())

    # Verify exact reproduction of user-observed output
    assert st_unauth["mode"] == "offline"
    assert st_unauth["samples_saved"] == 5820
    assert st_unauth["samples_in_cloud"] == 5820
    assert st_unauth["backlog"] == 0
    # Verify stderr warning was emitted
    assert "Warning: Cloud receiver at 'http://localhost:8000' returned HTTP 401 Unauthorized" in captured.err

    # 2. Authenticated invocation: probe succeeds with 200 -> mode is online
    code = main([
        "status",
        "--db", str(local_db),
        "--cloud-url", "http://localhost:8000",
        "--api-key", "repro-secret-auth-key-999",
    ])
    assert code == 0
    captured_auth = capsys.readouterr()
    st_auth = json.loads(captured_auth.out.strip())
    assert st_auth["mode"] == "online"
    assert st_auth["samples_saved"] == 5820
    assert st_auth["samples_in_cloud"] == 5820
    assert st_auth["backlog"] == 0

    cloud_storage.close()

