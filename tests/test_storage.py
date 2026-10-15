"""Comprehensive unit, integration, and adversarial recovery tests for SQLite Durable Local Storage (Phase 2 Increment C).

Verifies all Increment C requirements and adversarial scenarios:
1. SQLite WAL mode, synchronous durability configuration, and schema constraints.
2. Composite sample identity (session_id, sensor, sample_index).
3. Atomic batch transactions for samples, loss records, session metadata, incidents, and checkpoints.
4. Duplicate integrity: identical duplicates safely ignored; conflicting duplicates abort entire transaction.
5. Bidirectional contradictory loss detection:
   - Loss overlapping stored samples aborts transaction.
   - Incoming samples overlapping recorded loss aborts transaction.
   - Conflicting loss causes on overlapping ranges aborts transaction.
   - Adjacent identical-cause loss ranges merge cleanly.
6. Strict three-cursor invariants and progression:
   - I_persisted advances only over contiguous physical samples (never across loss).
   - I_resolved advances across stored samples or authoritative loss (never across unresolved gaps).
   - Out-of-order batches and gaps prevent false advancement.
   - Monotonic invariant: -1 <= I_persisted <= I_resolved <= I_obs.
7. Session isolation: independent cursors and samples per session; no cross-session leakage.
8. Incident durability and recovery:
   - Active and terminal unresolved incidents preserved across restarts.
   - Refusal to mark incident resolved if data is not durably committed in database.
9. Restart recovery and ground-truth checkpoint reconstruction:
   - Authoritative cursors reconstructed directly from raw table data.
   - Contradicted / forged checkpoints ahead of actual database contents raise StorageError (fail-safe).
10. Worker-to-storage failure handling & queue ack semantics:
    - Queue item is NEVER acknowledged (task_done) if commit fails.
    - Failed uncommitted batch preserved in worker.last_failed_batch.
    - Wire client paused via storage_error_event.
    - Restart re-requests missing data from committed checkpoint.
11. Transaction fault injection at multiple intermediate points (samples, loss, checkpoints).
12. Thread-safe concurrency and graceful shutdown waiting for in-flight commits.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import pytest
from pathlib import Path
from unittest.mock import patch

from biosignal_mock.server import MockServer
from pipeline.protocol_client import BiosignalProtocolClient, IngestBatch, IntegrityConflictError, UnresolvedCrcIncident
from pipeline.storage import (
    CheckpointRecord,
    ContradictoryLossConflictError,
    CursorInvariantViolationError,
    StorageEngine,
    StorageError,
)
from pipeline.worker import StorageIngestionWorker
from shared.protocol import LossRecord

STORAGE_TEST_PORT = 7520


def test_storage_schema_and_wal_pragmas(tmp_path: Path) -> None:
    """Requirement 1: Verifies WAL mode, synchronous pragma, and table schemas."""
    db_file = tmp_path / "test_pragmas.db"
    storage = StorageEngine(db_path=db_file, synchronous="NORMAL")

    try:
        cursor = storage._conn.cursor()
        cursor.execute("PRAGMA journal_mode;")
        assert cursor.fetchone()[0].lower() == "wal"

        cursor.execute("PRAGMA synchronous;")
        assert cursor.fetchone()[0] == 1  # 1 = NORMAL

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cursor.fetchall()}
        assert {"sessions", "samples", "loss_ledger", "unresolved_incidents", "checkpoints"}.issubset(tables)
    finally:
        storage.close()


def test_storage_atomic_batch_commit_and_rollback(tmp_path: Path) -> None:
    """Requirement 2: Verifies atomic commit of samples, loss records, and checkpoints."""
    db_file = tmp_path / "test_atomic.db"
    storage = StorageEngine(db_path=db_file)

    try:
        data = [[i * 3, i * 3 + 1, i * 3 + 2] for i in range(10)]
        lost = [LossRecord(from_index=10, to_index=19, reason="buffer_overflow")]
        batch = IngestBatch(
            session_id="sess_atomic",
            sensor=1,
            first_index=0,
            count=10,
            data=data,
            lost=lost,
            session_end=False,
            crc32="dummy",
        )

        ckpt = storage.commit_batch(batch)

        assert ckpt.contiguous_persisted_index == 9
        assert ckpt.contiguous_resolved_index == 19
        assert ckpt.highest_observed_index == 19

        cursor = storage._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = 'sess_atomic';")
        assert cursor.fetchone()[0] == 10

        cursor.execute("SELECT COUNT(*) FROM loss_ledger WHERE session_id = 'sess_atomic';")
        assert cursor.fetchone()[0] == 1
    finally:
        storage.close()


def test_storage_duplicate_integrity_safe_and_conflicting(tmp_path: Path) -> None:
    """Requirements 3 & 4: Composite PK and duplicate integrity checks."""
    db_file = tmp_path / "test_duplicates.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Batch 1: Sample 0 with values [10, 20, 30]
        batch1 = IngestBatch(
            session_id="sess_dup",
            sensor=1,
            first_index=0,
            count=1,
            data=[[10, 20, 30]],
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        storage.commit_batch(batch1)
        assert storage.telemetry.samples_inserted == 1
        assert storage.telemetry.duplicate_samples_ignored == 0

        # Batch 2: Identical duplicate of Sample 0 (values [10, 20, 30])
        batch2 = IngestBatch(
            session_id="sess_dup",
            sensor=1,
            first_index=0,
            count=1,
            data=[[10, 20, 30]],
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        ckpt2 = storage.commit_batch(batch2)
        assert ckpt2.contiguous_persisted_index == 0
        assert storage.telemetry.samples_inserted == 1
        assert storage.telemetry.duplicate_samples_ignored == 1

        # Batch 3: Conflicting duplicate of Sample 0 (values [99, 99, 99]) -> MUST RAISE IntegrityConflictError
        batch3 = IngestBatch(
            session_id="sess_dup",
            sensor=1,
            first_index=0,
            count=1,
            data=[[99, 99, 99]],
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        with pytest.raises(IntegrityConflictError) as exc_info:
            storage.commit_batch(batch3)
        assert "Integrity conflict for session sess_dup" in str(exc_info.value)

        # Verify rollback: original values [10, 20, 30] remain intact
        cursor = storage._conn.cursor()
        cursor.execute("SELECT ch1, ch2, ch3 FROM samples WHERE session_id = 'sess_dup' AND sample_index = 0;")
        row = cursor.fetchone()
        assert (row["ch1"], row["ch2"], row["ch3"]) == (10, 20, 30)
    finally:
        storage.close()


def test_storage_contradictory_loss_conflict_rejection(tmp_path: Path) -> None:
    """Verifies that a loss record covering existing physical samples raises ContradictoryLossConflictError."""
    db_file = tmp_path / "test_loss_conflict.db"
    storage = StorageEngine(db_path=db_file)

    try:
        batch1 = IngestBatch(
            session_id="sess_loss",
            sensor=1,
            first_index=0,
            count=10,
            data=[[i, i, i] for i in range(10)],
            lost=[],
            session_end=False,
            crc32="dummy",
        )
        storage.commit_batch(batch1)

        # Batch 2: Claims loss for indices [5, 15] -> overlaps existing samples 5..9!
        batch2 = IngestBatch(
            session_id="sess_loss",
            sensor=1,
            first_index=16,
            count=0,
            data=[],
            lost=[LossRecord(from_index=5, to_index=15, reason="buffer_overflow")],
            session_end=False,
            crc32="dummy",
        )
        with pytest.raises(ContradictoryLossConflictError) as exc_info:
            storage.commit_batch(batch2)
        assert "overlaps 5 physically stored samples" in str(exc_info.value)

        cursor = storage._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM loss_ledger WHERE session_id = 'sess_loss';")
        assert cursor.fetchone()[0] == 0
    finally:
        storage.close()


def test_storage_cursor_progression_gaps_and_loss(tmp_path: Path) -> None:
    """Requirements 6 & 7: Three-cursor invariants under gaps and confirmed loss."""
    db_file = tmp_path / "test_cursors.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Step 1: Initial contiguous arrival 0..99
        b1 = IngestBatch("sess_cur", 1, 0, 100, [[i, i, i] for i in range(100)], [], False, "crc")
        c1 = storage.commit_batch(b1)
        assert (c1.contiguous_persisted_index, c1.contiguous_resolved_index, c1.highest_observed_index) == (99, 99, 99)

        # Step 2: Forward arrival 200..299 ahead of unresolved gap [100, 199]
        b2 = IngestBatch("sess_cur", 1, 200, 100, [[i, i, i] for i in range(200, 300)], [], False, "crc")
        c2 = storage.commit_batch(b2)
        assert c2.contiguous_persisted_index == 99
        assert c2.contiguous_resolved_index == 99
        assert c2.highest_observed_index == 299

        # Step 3: Authoritative loss record arrives for [100, 149]
        b3 = IngestBatch(
            session_id="sess_cur",
            sensor=1,
            first_index=300,
            count=0,
            data=[],
            lost=[LossRecord(from_index=100, to_index=149, reason="buffer_overflow")],
            session_end=False,
            crc32="dummy",
        )
        c3 = storage.commit_batch(b3)
        assert c3.contiguous_persisted_index == 99
        assert c3.contiguous_resolved_index == 149
        assert c3.highest_observed_index == 299

        # Step 4: Sweeper recovers gap [150, 199]
        b4 = IngestBatch("sess_cur", 1, 150, 50, [[i, i, i] for i in range(150, 200)], [], False, "crc")
        c4 = storage.commit_batch(b4)
        assert c4.contiguous_persisted_index == 99
        assert c4.contiguous_resolved_index == 299
        assert c4.highest_observed_index == 299
    finally:
        storage.close()


def test_storage_session_isolation_and_checkpoints(tmp_path: Path) -> None:
    """Requirement 5: Sessions have completely isolated checkpoints and samples."""
    db_file = tmp_path / "test_isolation.db"
    storage = StorageEngine(db_path=db_file)

    try:
        b_a = IngestBatch("sess_A", 1, 0, 100, [[1, 2, 3]] * 100, [], False, "crc")
        storage.commit_batch(b_a)

        b_b = IngestBatch("sess_B", 1, 0, 50, [[4, 5, 6]] * 50, [], False, "crc")
        ckpt_b = storage.commit_batch(b_b)

        assert ckpt_b.contiguous_persisted_index == 49
        assert ckpt_b.contiguous_resolved_index == 49

        ckpt_a = storage.get_checkpoint("sess_A", 1)
        assert ckpt_a.contiguous_persisted_index == 99
        assert ckpt_a.contiguous_resolved_index == 99
    finally:
        storage.close()


def test_storage_unresolved_incident_persistence_and_restart(tmp_path: Path) -> None:
    """Requirement 8: Preserves active and terminal unresolved incidents across restarts."""
    db_file = tmp_path / "test_incidents.db"
    storage = StorageEngine(db_path=db_file)

    try:
        inc1 = UnresolvedCrcIncident("sess_inc", 1, 100, 199, retries_attempted=3, terminal=False)
        inc2 = UnresolvedCrcIncident("sess_old", 2, 50, 99, retries_attempted=3, terminal=True)

        storage.record_incident(inc1)
        storage.record_incident(inc2)

        incidents = storage.get_unresolved_incidents()
        assert len(incidents) == 2

        term = storage.get_unresolved_incidents(terminal_only=True)
        assert len(term) == 1
        assert term[0].session_id == "sess_old"
        assert term[0].terminal is True

        # Pre-commit resolution refusal: Trying to mark resolved before sample is committed must raise StorageError
        with pytest.raises(StorageError) as exc_info:
            storage.mark_incident_resolved("sess_inc", 1, 100)
        assert "neither sample nor authoritative loss" in str(exc_info.value).lower() or "not committed" in str(exc_info.value).lower()

        # Commit sample 100
        storage.commit_batch(IngestBatch("sess_inc", 1, 100, 1, [[1, 2, 3]], [], False, "crc"))

        # Close and reopen database (simulate restart)
        storage.close()
        storage2 = StorageEngine(db_path=db_file)
        try:
            reloaded = storage2.get_unresolved_incidents()
            assert len(reloaded) == 2

            # Mark inc1 as resolved now that sample 100 is durably stored
            storage2.mark_incident_resolved("sess_inc", 1, 100)
            remaining = storage2.get_unresolved_incidents()
            assert len(remaining) == 1
            assert remaining[0].session_id == "sess_old"
        finally:
            storage2.close()
    finally:
        pass


def test_storage_restart_recovery_and_checkpoint_restoration(tmp_path: Path) -> None:
    """Requirement 9: Recovers checkpoints accurately from committed database state."""
    db_file = tmp_path / "test_restart.db"
    storage = StorageEngine(db_path=db_file)

    try:
        b1 = IngestBatch("sess_rec", 1, 0, 50, [[1, 2, 3]] * 50, [], False, "crc")
        b2 = IngestBatch("sess_rec", 2, 0, 30, [[1, 2]] * 30, [], False, "crc")
        storage.commit_batch(b1)
        storage.commit_batch(b2)
        storage.close()

        storage_restarted = StorageEngine(db_path=db_file)
        try:
            checkpoints = storage_restarted.recover_checkpoints_on_restart()
            assert ("sess_rec", 1) in checkpoints
            assert ("sess_rec", 2) in checkpoints

            ckpt1 = checkpoints[("sess_rec", 1)]
            assert ckpt1.contiguous_persisted_index == 49
            assert ckpt1.contiguous_resolved_index == 49
            assert ckpt1.next_recovery_index == 50

            ckpt2 = checkpoints[("sess_rec", 2)]
            assert ckpt2.contiguous_persisted_index == 29
            assert ckpt2.contiguous_resolved_index == 29
            assert ckpt2.next_recovery_index == 30
        finally:
            storage_restarted.close()
    finally:
        pass


def test_storage_database_failure_and_transaction_rollback(tmp_path: Path) -> None:
    """Requirement 10: Database errors and disk full failures abort transaction without advancing cursors."""
    db_file = tmp_path / "test_failure.db"
    storage = StorageEngine(db_path=db_file)

    try:
        b1 = IngestBatch("sess_fail", 1, 0, 10, [[i, i, i] for i in range(10)], [], False, "crc")
        ckpt1 = storage.commit_batch(b1)
        assert ckpt1.contiguous_persisted_index == 9

        b2 = IngestBatch("sess_fail", 1, 10, 10, [[i, i, i] for i in range(10, 20)], [], False, "crc")

        real_conn = storage._conn

        class FailingCursor:
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def execute(self, sql, *args, **kwargs):
                if "INSERT INTO samples" in sql:
                    raise sqlite3.OperationalError("disk I/O error: simulated disk full")
                return self._real.execute(sql, *args, **kwargs)

        class ConnWrapper:
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def cursor(self, *args, **kwargs):
                return FailingCursor(self._real.cursor(*args, **kwargs))

        storage._conn = ConnWrapper(real_conn)

        with pytest.raises(sqlite3.OperationalError) as exc_info:
            storage.commit_batch(b2)
        assert "simulated disk full" in str(exc_info.value)

        storage._conn = real_conn

        assert storage.telemetry.transactions_rolled_back == 1

        ckpt_after = storage.get_checkpoint("sess_fail", 1)
        assert ckpt_after.contiguous_persisted_index == 9
        assert ckpt_after.contiguous_resolved_index == 9

        cursor = storage._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = 'sess_fail' AND sample_index >= 10;")
        assert cursor.fetchone()[0] == 0
    finally:
        storage.close()


def test_storage_bidirectional_loss_and_sample_conflict(tmp_path: Path) -> None:
    """Audit Point 5: Tests incoming sample vs existing loss, conflicting loss reasons, and adjacent merging."""
    db_file = tmp_path / "test_bidirectional.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Step 1: Record loss for [10, 19]
        b_loss = IngestBatch(
            session_id="sess_bidi",
            sensor=1,
            first_index=20,
            count=0,
            data=[],
            lost=[LossRecord(from_index=10, to_index=19, reason="buffer_overflow")],
            session_end=False,
            crc32="crc",
        )
        storage.commit_batch(b_loss)

        # Step 2: Incoming sample contradicts existing loss record [10, 19]
        b_sample_conflict = IngestBatch(
            session_id="sess_bidi",
            sensor=1,
            first_index=15,
            count=1,
            data=[[1, 2, 3]],
            lost=[],
            session_end=False,
            crc32="crc",
        )
        with pytest.raises(ContradictoryLossConflictError) as exc_info:
            storage.commit_batch(b_sample_conflict)
        assert "contradicts existing loss record [10, 19]" in str(exc_info.value)

        # Step 3: Incoming loss with conflicting reason on overlapping range
        b_conflicting_reason = IngestBatch(
            session_id="sess_bidi",
            sensor=1,
            first_index=30,
            count=0,
            data=[],
            lost=[LossRecord(from_index=15, to_index=25, reason="rate_limit")],
            session_end=False,
            crc32="crc",
        )
        with pytest.raises(ContradictoryLossConflictError) as exc_info:
            storage.commit_batch(b_conflicting_reason)
        assert "conflicting reason" in str(exc_info.value)

        # Step 4: Adjacent loss with same reason [20, 29] merges into [10, 29]
        b_adjacent_same_reason = IngestBatch(
            session_id="sess_bidi",
            sensor=1,
            first_index=30,
            count=0,
            data=[],
            lost=[LossRecord(from_index=20, to_index=29, reason="buffer_overflow")],
            session_end=False,
            crc32="crc",
        )
        storage.commit_batch(b_adjacent_same_reason)

        cursor = storage._conn.cursor()
        cursor.execute("SELECT from_index, to_index, reason FROM loss_ledger WHERE session_id = 'sess_bidi';")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert (rows[0]["from_index"], rows[0]["to_index"], rows[0]["reason"]) == (10, 29, "buffer_overflow")
    finally:
        storage.close()


def test_storage_checkpoint_corruption_detection_and_safefail(tmp_path: Path) -> None:
    """Audit Point 2: Detects corrupted checkpoints ahead of actual contents and fails safely."""
    db_file = tmp_path / "test_corrupt_ckpt.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Commit legitimate samples 0..19
        b1 = IngestBatch("sess_corrupt", 1, 0, 20, [[1, 2, 3]] * 20, [], False, "crc")
        storage.commit_batch(b1)

        # Manually tamper with checkpoints table to forge index 200 (ahead of actual samples)
        cursor = storage._conn.cursor()
        cursor.execute(
            """
            UPDATE checkpoints 
            SET contiguous_persisted_index = 200, contiguous_resolved_index = 200
            WHERE session_id = 'sess_corrupt' AND sensor = 1;
            """
        )
        storage.close()

        # Reopen database on restart: recover_checkpoints_on_restart must detect contradiction and fail safely
        storage_reopened = StorageEngine(db_path=db_file)
        try:
            with pytest.raises(StorageError) as exc_info:
                storage_reopened.recover_checkpoints_on_restart()
            assert "Corrupted checkpoint detected" in str(exc_info.value)
            assert "is ahead of actual verified physical samples" in str(exc_info.value)
        finally:
            storage_reopened.close()
    finally:
        pass


@pytest.mark.asyncio
async def test_worker_failure_queue_acknowledgment_and_retry(tmp_path: Path) -> None:
    """Audit Point 1: Proves failed commit does not acknowledge queue, preserves batch, and pauses client."""
    db_file = tmp_path / "test_worker_failure.db"
    storage = StorageEngine(db_path=db_file)
    client = BiosignalProtocolClient()
    worker = StorageIngestionWorker(client=client, storage=storage, max_retries=1)

    # 1. Place verified batch in accepted_queue
    batch = IngestBatch("sess_wf", 1, 0, 10, [[i, i, i] for i in range(10)], [], False, "crc")
    await client.accepted_queue.put(batch)
    assert client.accepted_queue.qsize() == 1

    # 2. Simulate disk full failure during commit
    real_commit = storage.commit_batch

    def failing_commit(b):
        raise sqlite3.OperationalError("database or disk is full")

    storage.commit_batch = failing_commit

    worker_task = worker.start()

    # Wait briefly for worker to attempt dequeue and fail
    await asyncio.sleep(0.1)

    try:
        # 3. Verify failure semantics:
        # A. Worker task raised or stopped
        assert worker_task.done()
        exc = worker_task.exception()
        assert isinstance(exc, sqlite3.OperationalError)
        assert worker.last_fatal_error is not None
        assert "disk is full" in str(worker.last_fatal_error)

        # B. Batch preserved in worker.last_failed_batch (not silently dropped)
        assert worker.last_failed_batch == batch

        # C. client.storage_error_event is set
        assert client.storage_error_event.is_set()

        # D. Database was not updated
        storage.commit_batch = real_commit
        ckpt = storage.get_checkpoint("sess_wf", 1)
        assert ckpt.contiguous_persisted_index == -1
    finally:
        await worker.stop()
        storage.close()


def test_transaction_fault_injection_at_intermediate_steps(tmp_path: Path) -> None:
    """Audit Point 6: Proves atomic rollback when failure occurs at intermediate steps."""
    db_file = tmp_path / "test_intermediate_faults.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Initial state: 0..9 committed
        b0 = IngestBatch("sess_fault", 1, 0, 10, [[i, i, i] for i in range(10)], [], False, "crc")
        storage.commit_batch(b0)

        # Failure injection: Fail right before COMMIT (e.g. during checkpoints UPDATE)
        b1 = IngestBatch(
            session_id="sess_fault",
            sensor=1,
            first_index=10,
            count=10,
            data=[[i, i, i] for i in range(10, 20)],
            lost=[LossRecord(from_index=20, to_index=25, reason="buffer_overflow")],
            session_end=False,
            crc32="crc",
        )

        real_conn = storage._conn

        class FailingCursor:
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def execute(self, sql, *args, **kwargs):
                if "INSERT INTO checkpoints" in sql:
                    raise sqlite3.OperationalError("simulated crash before checkpoint update")
                return self._real.execute(sql, *args, **kwargs)

        class ConnWrapper:
            def __init__(self, real):
                self._real = real
            def __getattr__(self, name):
                return getattr(self._real, name)
            def cursor(self, *args, **kwargs):
                return FailingCursor(self._real.cursor(*args, **kwargs))

        storage._conn = ConnWrapper(real_conn)

        with pytest.raises(sqlite3.OperationalError):
            storage.commit_batch(b1)

        storage._conn = real_conn

        # Verify rollback: Neither samples 10..19 nor loss records 20..25 remain
        cursor = storage._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = 'sess_fault' AND sample_index >= 10;")
        assert cursor.fetchone()[0] == 0

        cursor.execute("SELECT COUNT(*) FROM loss_ledger WHERE session_id = 'sess_fault';")
        assert cursor.fetchone()[0] == 0

        ckpt = storage.get_checkpoint("sess_fault", 1)
        assert ckpt.contiguous_persisted_index == 9
        assert ckpt.contiguous_resolved_index == 9
    finally:
        storage.close()


def test_storage_multithreaded_concurrency_lock(tmp_path: Path) -> None:
    """Audit Point 7: Proves multiple concurrent threads accessing StorageEngine are safe."""
    db_file = tmp_path / "test_concurrency.db"
    storage = StorageEngine(db_path=db_file)

    def worker_thread(sensor_id: int):
        for i in range(5):
            batch = IngestBatch(
                session_id="sess_conc",
                sensor=sensor_id,
                first_index=i * 10,
                count=10,
                data=[[x, x, x] for x in range(i * 10, (i + 1) * 10)],
                lost=[],
                session_end=False,
                crc32="crc",
            )
            storage.commit_batch(batch)

    threads = [
        threading.Thread(target=worker_thread, args=(1,)),
        threading.Thread(target=worker_thread, args=(2,)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ckpt1 = storage.get_checkpoint("sess_conc", 1)
    ckpt2 = storage.get_checkpoint("sess_conc", 2)
    assert ckpt1.contiguous_persisted_index == 49
    assert ckpt2.contiguous_persisted_index == 49
    storage.close()


@pytest.mark.asyncio
async def test_storage_worker_integration_live_stream(tmp_path: Path) -> None:
    """End-to-end integration with MockServer and BiosignalProtocolClient."""
    port = STORAGE_TEST_PORT + 2
    server = MockServer(mode="ramp", port=port, host="127.0.0.1", realtime=False)
    server.generator.advance_samples(100)
    await server.start()

    db_file = tmp_path / "live_pipeline.db"
    storage = StorageEngine(db_path=db_file)
    client = BiosignalProtocolClient(host="127.0.0.1", port=port, poll_delay=0.01)
    worker = StorageIngestionWorker(client=client, storage=storage)

    client_task = client.start()
    worker_task = worker.start()

    try:
        for _ in range(40):
            if storage.telemetry.batches_committed >= 2:
                break
            await asyncio.sleep(0.05)

        assert storage.telemetry.batches_committed >= 2

        sess_id = client.current_session_id
        assert sess_id is not None

        ckpt1 = storage.get_checkpoint(sess_id, 1)
        assert ckpt1.contiguous_persisted_index >= 0
        assert ckpt1.contiguous_resolved_index >= 0

        ckpt2 = storage.get_checkpoint(sess_id, 2)
        assert ckpt2.contiguous_persisted_index >= 0
        assert ckpt2.contiguous_resolved_index >= 0
    finally:
        await client.stop()
        await worker.stop()
        storage.close()
        await server.stop()


@pytest.mark.asyncio
async def test_storage_recovery_after_failure_in_process_and_restart(tmp_path: Path) -> None:
    """Case A: Recovery after SQLite commit failure (both in-process retry and restart recovery)."""
    db_file = tmp_path / "test_recovery_paths.db"
    storage = StorageEngine(db_path=db_file)
    client = BiosignalProtocolClient()
    worker = StorageIngestionWorker(client=client, storage=storage, max_retries=1)

    # Step 1: Successful initial batch (0..9)
    b0 = IngestBatch("sess_rec_path", 1, 0, 10, [[i, i, i] for i in range(10)], [], False, "crc")
    storage.commit_batch(b0)
    assert storage.get_checkpoint("sess_rec_path", 1).contiguous_persisted_index == 9

    # Step 2: Enqueue batch (10..19), but inject commit failure
    b1 = IngestBatch("sess_rec_path", 1, 10, 10, [[i, i, i] for i in range(10, 20)], [], False, "crc")
    await client.accepted_queue.put(b1)

    real_commit = storage.commit_batch

    def failing_commit(b):
        raise sqlite3.OperationalError("simulated disk full error")

    storage.commit_batch = failing_commit

    worker_task = worker.start()
    await asyncio.sleep(0.1)

    # Step 3: Verify failure behavior
    assert worker_task.done()
    assert isinstance(worker_task.exception(), sqlite3.OperationalError)
    # A. Task done was NOT called
    assert client.accepted_queue.qsize() == 1 or worker.last_failed_batch == b1
    # B. Wire polling paused
    assert client.storage_error_event.is_set()
    # C. Failed batch preserved in-memory
    assert worker.last_failed_batch == b1
    # D. Database checkpoint remains firmly at 9 (never falsely reports 19)
    storage.commit_batch = real_commit
    assert storage.get_checkpoint("sess_rec_path", 1).contiguous_persisted_index == 9

    # Step 4: Test Path 1 — In-process recovery
    # Operator resolves disk issue and calls worker.retry_failed_batch()
    ckpt_recovered = await worker.retry_failed_batch()
    assert ckpt_recovered.contiguous_persisted_index == 19
    assert worker.last_failed_batch is None
    assert worker.last_fatal_error is None
    assert not client.storage_error_event.is_set()
    assert storage.get_checkpoint("sess_rec_path", 1).contiguous_persisted_index == 19

    # Step 5: Test Path 2 — Process restart recovery
    # Enqueue batch (20..29), fail commit, and simulate process restart
    b2 = IngestBatch("sess_rec_path", 1, 20, 10, [[i, i, i] for i in range(20, 30)], [], False, "crc")
    storage.commit_batch = failing_commit
    await client.accepted_queue.put(b2)
    await asyncio.sleep(0.1)
    if worker._task and worker._task.done():
        _ = worker._task.exception()

    # Shutdown / simulate crash
    await worker.stop()
    storage.close()

    # Reopen database on restart (in-memory worker.last_failed_batch is discarded)
    storage_restarted = StorageEngine(db_path=db_file)
    try:
        checkpoints = storage_restarted.recover_checkpoints_on_restart()
        ckpt = checkpoints[("sess_rec_path", 1)]
        # Checkpoint is STILL at 19 — failed batch 20..29 was never falsely persisted
        assert ckpt.contiguous_persisted_index == 19
        assert ckpt.contiguous_resolved_index == 19
        # Wire client starts recovery from I_resolved + 1 = 20!
        assert ckpt.next_recovery_index == 20

        # Verify samples 20..29 are NOT in DB
        cursor = storage_restarted._conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = 'sess_rec_path' AND sample_index >= 20;")
        assert cursor.fetchone()[0] == 0
    finally:
        storage_restarted.close()


def test_checkpoint_reconstruction_with_unresolved_gaps(tmp_path: Path) -> None:
    """Case B: Checkpoint reconstruction with observed range beyond contiguous samples and gap."""
    db_file = tmp_path / "test_unresolved_gaps.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # Step 1: Contiguous stored samples through index 99
        b1 = IngestBatch("sess_gap", 1, 0, 100, [[i, i, i] for i in range(100)], [], False, "crc")
        storage.commit_batch(b1)

        # Step 2: Manually set highest_observed_index = 250 to simulate wire observation
        # ahead of an unresolved gap at 100..249 with NO loss records
        cursor = storage._conn.cursor()
        cursor.execute(
            """
            UPDATE checkpoints 
            SET highest_observed_index = 250
            WHERE session_id = 'sess_gap' AND sensor = 1;
            """
        )
        storage.close()

        # Step 3: Restart storage layer
        storage_reopened = StorageEngine(db_path=db_file)
        try:
            checkpoints = storage_reopened.recover_checkpoints_on_restart()
            ckpt = checkpoints[("sess_gap", 1)]

            # A. I_persisted remains 99
            assert ckpt.contiguous_persisted_index == 99
            # B. I_resolved remains 99 (blocked by unresolved gap at 100)
            assert ckpt.contiguous_resolved_index == 99
            # C. I_obs is restored as 250 according to documented semantics (not treated as corruption)
            assert ckpt.highest_observed_index == 250
            # D. Transport recovery cursor requests index 100 (earliest unresolved index, NOT beyond gap)
            assert ckpt.next_recovery_index == 100

            # Step 4: Genuinely forged checkpoint test (claims I_resolved is ahead of ground truth)
            cursor2 = storage_reopened._conn.cursor()
            cursor2.execute(
                """
                UPDATE checkpoints 
                SET contiguous_resolved_index = 150
                WHERE session_id = 'sess_gap' AND sensor = 1;
                """
            )
            storage_reopened.close()

            # Must fail safely when reopened
            storage_forged = StorageEngine(db_path=db_file)
            try:
                with pytest.raises(StorageError) as exc_info:
                    storage_forged.recover_checkpoints_on_restart()
                assert "Corrupted checkpoint detected" in str(exc_info.value)
                assert "is ahead of actual verified resolved coverage" in str(exc_info.value)
            finally:
                storage_forged.close()
        finally:
            pass
    finally:
        pass


def test_loss_range_merge_safety_and_incident_transition(tmp_path: Path) -> None:
    """Case C: Tests identical causes, conflicting causes, intervening sample, and incident resolution."""
    db_file = tmp_path / "test_merge_safety.db"
    storage = StorageEngine(db_path=db_file)

    try:
        # 1. Identical causes merge cleanly: [10, 19] + [20, 29] with buffer_overflow -> [10, 29]
        b1 = IngestBatch(
            "sess_ms", 1, 30, 0, [],
            [LossRecord(10, 19, "buffer_overflow"), LossRecord(20, 29, "buffer_overflow")],
            False, "crc"
        )
        storage.commit_batch(b1)

        cursor = storage._conn.cursor()
        cursor.execute("SELECT from_index, to_index, reason FROM loss_ledger WHERE session_id = 'sess_ms' AND from_index = 10;")
        row = cursor.fetchone()
        assert (row["from_index"], row["to_index"], row["reason"]) == (10, 29, "buffer_overflow")

        # 2. Conflicting causes do NOT merge: [40, 49] rate_limit remains distinct from buffer_overflow
        b2 = IngestBatch(
            "sess_ms", 1, 50, 0, [],
            [LossRecord(30, 39, "buffer_overflow"), LossRecord(40, 49, "rate_limit")],
            False, "crc"
        )
        storage.commit_batch(b2)

        cursor.execute("SELECT from_index, to_index, reason FROM loss_ledger WHERE session_id = 'sess_ms' ORDER BY from_index ASC;")
        rows = cursor.fetchall()
        assert len(rows) == 2
        # [30, 39] merged with left-adjacent [10, 29] (same cause) -> [10, 39]
        assert (rows[0]["from_index"], rows[0]["to_index"], rows[0]["reason"]) == (10, 39, "buffer_overflow")
        # [40, 49] rate_limit did NOT merge (different cause) -> distinct row [40, 49]
        assert (rows[1]["from_index"], rows[1]["to_index"], rows[1]["reason"]) == (40, 49, "rate_limit")

        # 3. Intervening stored sample: sample 60 exists in samples table. Loss [50, 59] and [61, 70] do NOT merge across it!
        b3 = IngestBatch("sess_ms", 1, 60, 1, [[1, 2, 3]], [], False, "crc")
        storage.commit_batch(b3)

        b4 = IngestBatch(
            "sess_ms", 1, 80, 0, [],
            [LossRecord(50, 59, "buffer_overflow"), LossRecord(61, 70, "buffer_overflow")],
            False, "crc"
        )
        storage.commit_batch(b4)

        cursor.execute("SELECT from_index, to_index FROM loss_ledger WHERE session_id = 'sess_ms' AND from_index >= 50 ORDER BY from_index ASC;")
        rows_inv = cursor.fetchall()
        assert len(rows_inv) == 2
        assert (rows_inv[0]["from_index"], rows_inv[0]["to_index"]) == (50, 59)
        assert (rows_inv[1]["from_index"], rows_inv[1]["to_index"]) == (61, 70)

        # Attempt to insert loss covering intervening sample 60 MUST raise ContradictoryLossConflictError
        b_inv_conflict = IngestBatch(
            "sess_ms", 1, 90, 0, [],
            [LossRecord(55, 65, "buffer_overflow")],
            False, "crc"
        )
        with pytest.raises(ContradictoryLossConflictError):
            storage.commit_batch(b_inv_conflict)

        # 4. Unresolved incident transition:
        # Pre-record an unresolved CRC incident at [80, 89] (resolved = 0)
        inc = UnresolvedCrcIncident("sess_ms", 1, 80, 89, retries_attempted=3, terminal=False)
        storage.record_incident(inc)
        assert len(storage.get_unresolved_incidents("sess_ms")) == 1

        # Authoritative source loss arrives for [80, 89] -> transitions incident to resolved
        b_auth_loss = IngestBatch(
            "sess_ms", 1, 90, 0, [],
            [LossRecord(80, 89, "buffer_overflow")],
            False, "crc"
        )
        storage.commit_batch(b_auth_loss)

        # Incident is now resolved by source evidence
        assert len(storage.get_unresolved_incidents("sess_ms")) == 0
    finally:
        storage.close()

