"""Tests for MayaCardio Cloud PostgreSQL Storage and Render Deployment Support.

Validates:
1. Connection failure and invalid URL configuration handling.
2. PostgreSQL schema creation, sessions, samples, loss_ledger, checkpoints, and window_results.
3. Atomic batch persistence, dual contiguous cursors (persisted vs resolved).
4. Idempotent re-transmissions (safe retry with identical cursors, zero duplicates).
5. Conflicting duplicates rejected with CloudIntegrityConflictError (HTTP 409).
6. Contradictory loss/sample overlaps rejected with CloudContradictoryLossConflictError (HTTP 409).
7. Application restart persistence: reconstruct_authoritative_checkpoints parity across processes.
8. FastAPI integration: /api/v1/status, /api/v1/ingest/batch, and /api/v1/export on PostgreSQL.
"""

from __future__ import annotations

import os
import time
import pytest
from fastapi.testclient import TestClient

from pipeline.cloud.server import create_cloud_app
from pipeline.cloud.storage import (
    CloudContradictoryLossConflictError,
    CloudIntegrityConflictError,
    CloudStorageEngine,
    CloudStorageError,
)
from pipeline.protocol_client import IngestBatch
from shared.protocol import LossRecord

# Default local test PostgreSQL container URL (fallback to env var)
PG_TEST_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://maya_user:maya_test_pass@127.0.0.1:5435/mayacardio_cloud",
)


def is_postgres_available(url: str) -> bool:
    """Check if test PostgreSQL instance is reachable."""
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


POSTGRES_AVAILABLE = is_postgres_available(PG_TEST_URL)


# ==============================================================================
# Configuration Validation Tests (Always Executed)
# ==============================================================================

def test_postgres_unreachable_connection_failure() -> None:
    """Verifies that an unreachable PostgreSQL URL raises a clean CloudStorageError."""
    bad_url = "postgresql://nonexistent_user:bad_pass@127.0.0.1:54999/nonexistent_db"
    with pytest.raises(CloudStorageError) as exc_info:
        CloudStorageEngine(database_url=bad_url)
    assert "Failed to connect to PostgreSQL database" in str(exc_info.value)


def test_postgres_url_normalization() -> None:
    """Verifies that legacy 'postgres://' URLs are normalized to 'postgresql://'."""
    # Test that constructor recognizes postgres:// and handles it
    legacy_url = "postgres://nonexistent_user:bad_pass@127.0.0.1:54999/nonexistent_db"
    with pytest.raises(CloudStorageError) as exc_info:
        CloudStorageEngine(database_url=legacy_url)
    # The error message should come from postgres connection attempt, not an unrecognized scheme
    assert "Failed to connect to PostgreSQL database" in str(exc_info.value)


# ==============================================================================
# PostgreSQL Functional & Durability Invariant Tests
# ==============================================================================

@pytest.mark.skipif(not POSTGRES_AVAILABLE, reason="PostgreSQL test instance is not reachable")
class TestPostgresCloudStorage:
    """Full invariant verification against real PostgreSQL database."""

    @pytest.fixture(autouse=True)
    def clean_pg_db(self) -> None:
        """Truncate test tables before each test to ensure complete isolation."""
        import psycopg2
        conn = psycopg2.connect(PG_TEST_URL)
        cur = conn.cursor()
        cur.execute("""
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        conn.commit()
        cur.close()
        conn.close()

    @pytest.fixture
    def pg_storage(self) -> CloudStorageEngine:
        storage = CloudStorageEngine(database_url=PG_TEST_URL)
        yield storage
        storage.close()

    def test_postgres_ingest_happy_path_and_cursors(self, pg_storage: CloudStorageEngine) -> None:
        """Verifies atomic ingestion and dual cursor computation on PostgreSQL."""
        batch = IngestBatch(
            session_id="pg_sess_1",
            sensor=1,
            first_index=0,
            count=3,
            data=[[10, 20, 30], [11, 21, 31], [12, 22, 32]],
            lost=[],
            session_end=False,
            crc32="",
        )
        ckpt = pg_storage.save_batch(batch)
        assert ckpt.session_id == "pg_sess_1"
        assert ckpt.sensor == 1
        assert ckpt.contiguous_persisted_index == 2
        assert ckpt.contiguous_resolved_index == 2
        assert ckpt.highest_observed_index == 2

        samp_cnt, loss_cnt = pg_storage.get_counts("pg_sess_1")
        assert samp_cnt == 3
        assert loss_cnt == 0

    def test_postgres_idempotent_duplicate_retry(self, pg_storage: CloudStorageEngine) -> None:
        """Verifies that re-transmitting an identical batch on PostgreSQL is 100% idempotent."""
        batch = IngestBatch(
            session_id="pg_sess_2",
            sensor=2,
            first_index=0,
            count=2,
            data=[[100, 200], [101, 201]],
            lost=[],
            session_end=False,
            crc32="",
        )
        ckpt1 = pg_storage.save_batch(batch)
        assert pg_storage.telemetry.samples_inserted == 2
        assert pg_storage.telemetry.duplicate_samples_ignored == 0

        # Exact retry of identical batch
        ckpt2 = pg_storage.save_batch(batch)
        assert ckpt2.contiguous_persisted_index == ckpt1.contiguous_persisted_index
        assert ckpt2.contiguous_resolved_index == ckpt1.contiguous_resolved_index
        assert pg_storage.telemetry.samples_inserted == 2
        assert pg_storage.telemetry.duplicate_samples_ignored == 2

    def test_postgres_conflicting_duplicate_rejected(self, pg_storage: CloudStorageEngine) -> None:
        """Verifies that conflicting sample values trigger CloudIntegrityConflictError and rollback."""
        batch_valid = IngestBatch(
            session_id="pg_sess_3",
            sensor=1,
            first_index=0,
            count=1,
            data=[[10, 20, 30]],
            lost=[],
            session_end=False,
            crc32="",
        )
        pg_storage.save_batch(batch_valid)

        # Batch with differing channel values at index 0
        batch_conflict = IngestBatch(
            session_id="pg_sess_3",
            sensor=1,
            first_index=0,
            count=1,
            data=[[99, 99, 99]],
            lost=[],
            session_end=False,
            crc32="",
        )
        with pytest.raises(CloudIntegrityConflictError):
            pg_storage.save_batch(batch_conflict)

        # Original sample must be intact
        s1, s2 = pg_storage.get_samples_for_session("pg_sess_3")
        assert s1[0] == (10, 20, 30)

    def test_postgres_contradictory_loss_rejected(self, pg_storage: CloudStorageEngine) -> None:
        """Verifies that incoming loss overlapping stored samples is rejected."""
        batch = IngestBatch(
            session_id="pg_sess_4",
            sensor=1,
            first_index=0,
            count=5,
            data=[[i, i, i] for i in range(5)],
            lost=[],
            session_end=False,
            crc32="",
        )
        pg_storage.save_batch(batch)

        # Incoming batch claiming loss over [2, 4]
        loss_batch = IngestBatch(
            session_id="pg_sess_4",
            sensor=1,
            first_index=10,
            count=0,
            data=[],
            lost=[LossRecord(from_index=2, to_index=4, reason="buffer_overflow")],
            session_end=False,
            crc32="",
        )
        with pytest.raises(CloudContradictoryLossConflictError):
            pg_storage.save_batch(loss_batch)

    def test_postgres_restart_checkpoint_recovery(self, pg_storage: CloudStorageEngine) -> None:
        """Verifies that restart recovery reconstructs exact checkpoints from raw PostgreSQL tables."""
        # Ingest samples [0..4]
        batch1 = IngestBatch(
            session_id="pg_sess_recover",
            sensor=1,
            first_index=0,
            count=5,
            data=[[i, i + 1, i + 2] for i in range(5)],
            lost=[],
            session_end=False,
            crc32="",
        )
        pg_storage.save_batch(batch1)

        # Ingest bounded loss [5..9] and samples [10..14]
        batch2 = IngestBatch(
            session_id="pg_sess_recover",
            sensor=1,
            first_index=10,
            count=5,
            data=[[i, i + 1, i + 2] for i in range(10, 15)],
            lost=[LossRecord(from_index=5, to_index=9, reason="network_timeout")],
            session_end=False,
            crc32="",
        )
        ckpt_pre = pg_storage.save_batch(batch2)
        assert ckpt_pre.contiguous_persisted_index == 4  # Gapped by loss at 5
        assert ckpt_pre.contiguous_resolved_index == 14  # Advanced across bounded loss

        # Simulate fresh process restart by creating a new engine instance
        restarted_storage = CloudStorageEngine(database_url=PG_TEST_URL)
        try:
            reconstructed = restarted_storage.reconstruct_authoritative_checkpoints("pg_sess_recover")
            rec_ckpt = reconstructed[("pg_sess_recover", 1)]
            assert rec_ckpt.contiguous_persisted_index == 4
            assert rec_ckpt.contiguous_resolved_index == 14
            assert rec_ckpt.highest_observed_index == 14
        finally:
            restarted_storage.close()

    def test_postgres_fastapi_server_integration(self) -> None:
        """Verifies FastAPI REST endpoints (/status, /ingest/batch, /export) against PostgreSQL."""
        app = create_cloud_app(database_url=PG_TEST_URL)
        client = TestClient(app)

        # 1. Health check
        status_resp = client.get("/api/v1/status")
        assert status_resp.status_code == 200
        assert status_resp.json()["status"] == "healthy"

        # 2. Ingest batch
        payload = {
            "session_id": "pg_api_sess",
            "sensor": 1,
            "first_index": 0,
            "count": 2,
            "data": [[10, 20, 30], [40, 50, 60]],
            "lost": [],
            "session_end": False,
        }
        ingest_resp = client.post("/api/v1/ingest/batch", json=payload)
        assert ingest_resp.status_code == 200
        data = ingest_resp.json()
        assert data["contiguous_persisted_index"] == 1
        assert data["contiguous_resolved_index"] == 1
        assert data["samples_inserted"] == 2

        # 3. Export data
        export_resp = client.get("/api/v1/export?session=pg_api_sess")
        assert export_resp.status_code == 200
        lines = [line for line in export_resp.text.strip().split("\n") if line]
        assert len(lines) == 2
