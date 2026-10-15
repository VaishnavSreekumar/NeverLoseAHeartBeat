"""MayaCardio SQLite Durable Local Storage Engine (Phase 2 Increment C).

Responsible for:
- SQLite Write-Ahead Logging (WAL) mode with explicit synchronous/durability configuration.
- Composite sample identity (session_id, sensor, sample_index) preventing silent overwrite.
- Explicit duplicate validation: identical duplicates safely ignored, conflicting values raise IntegrityConflictError.
- Strict three-cursor model and monotonic invariant: -1 <= I_persisted <= I_resolved <= I_obs.
  * I_persisted advances strictly over contiguous stored samples (never across loss).
  * I_resolved advances across stored samples or authoritative source loss (never across unresolved gaps).
  * I_obs tracks the maximum observed sample index.
- Contradictory loss detection (bidirectional):
  * Loss records overlapping existing samples raise ContradictoryLossConflictError.
  * Incoming samples overlapping existing loss records raise ContradictoryLossConflictError.
  * Conflicting loss reasons covering overlapping ranges raise ContradictoryLossConflictError.
- Adjacent loss range merging for identical causes and session streams.
- Thread-safe concurrency control via recursive mutex across executor threads.
- Safe resolution traversal preventing infinite loops on open-ended terminal intervals.
- Ground-truth authoritative checkpoint reconstruction from raw table contents.
- Startup checkpoint validation detecting corrupted or ahead-of-state records (failing safely).
- Incident resolution validation ensuring incidents cannot be marked resolved before data is durably stored.
- Atomic batch transactions: samples, loss records, incidents, and checkpoints commit or roll back together.
- Crash-consistent restart recovery from committed database state.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pipeline.protocol_client import IngestBatch, IntegrityConflictError, UnresolvedCrcIncident
from shared.protocol import LossRecord

logger = logging.getLogger("pipeline.storage")


class StorageError(Exception):
    """Base exception for storage engine failures."""
    pass


class ContradictoryLossConflictError(StorageError):
    """Raised when an incoming loss record contradicts physically persisted samples or conflicting loss."""
    pass


class CursorInvariantViolationError(StorageError):
    """Raised when cursor advancement violates the monotonic ordering invariant."""
    pass


@dataclass(frozen=True)
class CheckpointRecord:
    """Committed checkpoint state for a specific (session_id, sensor) stream."""
    session_id: str
    sensor: int
    contiguous_persisted_index: int
    contiguous_resolved_index: int
    highest_observed_index: int
    last_updated_at: float

    @property
    def next_recovery_index(self) -> int:
        """The exact index to request on wire restart: I_resolved + 1."""
        return self.contiguous_resolved_index + 1


@dataclass(frozen=True)
class CloudReplicationCheckpoint:
    """Committed cloud replication state for a specific (session_id, sensor) stream."""
    session_id: str
    sensor: int
    cloud_persisted_index: int
    cloud_resolved_index: int
    highest_observed_index: int
    last_acked_at: float


@dataclass
class StorageTelemetry:
    """Operational metrics for the storage engine."""
    batches_committed: int = 0
    samples_inserted: int = 0
    duplicate_samples_ignored: int = 0
    loss_records_inserted: int = 0
    incidents_persisted: int = 0
    transactions_rolled_back: int = 0


class StorageEngine:
    """Durable SQLite storage engine with atomic micro-batching and cursor tracking."""

    def __init__(
        self,
        db_path: str | Path = "biosignal_local.db",
        synchronous: str = "NORMAL",
        busy_timeout_ms: int = 5000,
    ) -> None:
        """Initialize the SQLite storage engine.
        
        Args:
            db_path: Path to SQLite database file or ':memory:'.
            synchronous: SQLite PRAGMA synchronous mode ('NORMAL' or 'FULL').
                - 'NORMAL': 100% durable against process crashes (SIGKILL), zero DB corruption via WAL.
                  Does not issue per-transaction fsync to disk platter/flash; dirty OS pages flushed periodically.
                - 'FULL': Issues fsync on every transaction commit. Guarantees durability to hardware write buffer,
                  subject to hardware lack of power-loss protection capacitors on commodity flash.
            busy_timeout_ms: Timeout in milliseconds waiting on database locks.
        """
        self.db_path = str(db_path)
        self.synchronous = synchronous.upper()
        self.busy_timeout_ms = busy_timeout_ms
        self.telemetry = StorageTelemetry()
        self._lock = threading.RLock()

        self._conn: sqlite3.Connection = sqlite3.connect(
            self.db_path,
            timeout=busy_timeout_ms / 1000.0,
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode; we manage explicit BEGIN IMMEDIATE / COMMIT blocks
        )
        self._conn.row_factory = sqlite3.Row

        self._init_pragmas_and_schema()

    def _init_pragmas_and_schema(self) -> None:
        """Configure performance/durability pragmas and create required tables."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                # Configure Write-Ahead Logging (WAL)
                cursor.execute("PRAGMA journal_mode = WAL;")
                mode = cursor.fetchone()[0]
                logger.info(f"SQLite journal mode: {mode.upper()}")

                # Configure Synchronous Durability Mode
                cursor.execute(f"PRAGMA synchronous = {self.synchronous};")
                cursor.execute("PRAGMA foreign_keys = ON;")
                cursor.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms};")

                # Create Tables
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        session_id TEXT PRIMARY KEY,
                        started_at REAL NOT NULL,
                        ended_at REAL,
                        clean_shutdown BOOLEAN DEFAULT 0
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS samples (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL CHECK (sensor IN (1, 2)),
                        sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
                        ch1 INTEGER NOT NULL,
                        ch2 INTEGER NOT NULL,
                        ch3 INTEGER,
                        created_at REAL NOT NULL,
                        PRIMARY KEY (session_id, sensor, sample_index)
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS loss_ledger (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL CHECK (sensor IN (1, 2)),
                        from_index INTEGER NOT NULL CHECK (from_index >= 0),
                        to_index INTEGER CHECK (to_index IS NULL OR to_index >= from_index),
                        reason TEXT NOT NULL,
                        detected_at REAL NOT NULL,
                        UNIQUE (session_id, sensor, from_index, to_index, reason)
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS unresolved_incidents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL CHECK (sensor IN (1, 2)),
                        from_index INTEGER NOT NULL CHECK (from_index >= 0),
                        to_index INTEGER CHECK (to_index IS NULL OR to_index >= from_index),
                        first_failed_at REAL NOT NULL,
                        retries_attempted INTEGER NOT NULL,
                        terminal BOOLEAN NOT NULL DEFAULT 0,
                        resolved BOOLEAN NOT NULL DEFAULT 0,
                        UNIQUE (session_id, sensor, from_index)
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL CHECK (sensor IN (1, 2)),
                        contiguous_persisted_index INTEGER NOT NULL DEFAULT -1,
                        contiguous_resolved_index INTEGER NOT NULL DEFAULT -1,
                        highest_observed_index INTEGER NOT NULL DEFAULT -1,
                        last_updated_at REAL NOT NULL,
                        PRIMARY KEY (session_id, sensor)
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cloud_checkpoints (
                        session_id TEXT NOT NULL,
                        sensor INTEGER NOT NULL CHECK (sensor IN (1, 2)),
                        cloud_persisted_index INTEGER NOT NULL DEFAULT -1,
                        cloud_resolved_index INTEGER NOT NULL DEFAULT -1,
                        highest_observed_index INTEGER NOT NULL DEFAULT -1,
                        last_acked_at REAL NOT NULL,
                        PRIMARY KEY (session_id, sensor)
                    );
                """)

                # Indexes for high-throughput contiguous cursor queries
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_samples_cursor 
                    ON samples (session_id, sensor, sample_index);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_loss_cursor 
                    ON loss_ledger (session_id, sensor, from_index, to_index);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cloud_checkpoints
                    ON cloud_checkpoints (session_id, sensor);
                """)
            finally:
                cursor.close()

    def close(self) -> None:
        """Cleanly close database connection."""
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def get_checkpoint(self, session_id: str, sensor: int) -> CheckpointRecord:
        """Fetch the durable committed checkpoint for a (session_id, sensor) pair."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT contiguous_persisted_index, contiguous_resolved_index, 
                           highest_observed_index, last_updated_at
                    FROM checkpoints
                    WHERE session_id = ? AND sensor = ?
                    """,
                    (session_id, sensor),
                )
                row = cursor.fetchone()
                if row:
                    return CheckpointRecord(
                        session_id=session_id,
                        sensor=sensor,
                        contiguous_persisted_index=row["contiguous_persisted_index"],
                        contiguous_resolved_index=row["contiguous_resolved_index"],
                        highest_observed_index=row["highest_observed_index"],
                        last_updated_at=row["last_updated_at"],
                    )
                return CheckpointRecord(
                    session_id=session_id,
                    sensor=sensor,
                    contiguous_persisted_index=-1,
                    contiguous_resolved_index=-1,
                    highest_observed_index=-1,
                    last_updated_at=0.0,
                )
            finally:
                cursor.close()

    def commit_batch(self, batch: IngestBatch) -> CheckpointRecord:
        """Commit an IngestBatch into SQLite within a single atomic transaction.
        
        Enforces:
        1. Atomic multi-table consistency (BEGIN IMMEDIATE ... COMMIT or ROLLBACK).
        2. Exact sample idempotency: identical duplicates ignored, conflicting values raise IntegrityConflictError.
        3. Bidirectional contradictory loss prevention:
           - Incoming loss overlapping stored samples raises ContradictoryLossConflictError.
           - Incoming samples overlapping recorded loss raise ContradictoryLossConflictError.
           - Incoming loss overlapping existing loss with a different reason raises ContradictoryLossConflictError.
        4. Adjacent identical-cause loss merging.
        5. Invariant assertion: -1 <= I_persisted <= I_resolved <= I_obs.
        
        Returns:
            Updated CheckpointRecord reflecting the newly committed contiguous boundary.
        """
        with self._lock:
            cursor = self._conn.cursor()
            now = time.time()
            try:
                # Begin explicit immediate transaction (locks against concurrent writers)
                cursor.execute("BEGIN IMMEDIATE;")

                # 1. Ensure Session Record Exists
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO sessions (session_id, started_at, ended_at, clean_shutdown)
                    VALUES (?, ?, NULL, 0);
                    """,
                    (batch.session_id, now),
                )

                # 2. Duplicate & Conflict Verification for Incoming Samples
                new_samples_inserted = 0
                for offset, values in enumerate(batch.data):
                    idx = batch.first_index + offset
                    ch1 = values[0]
                    ch2 = values[1]
                    ch3 = values[2] if len(values) > 2 else None

                    # Check bidirectional conflict: Does this sample index contradict recorded loss?
                    cursor.execute(
                        """
                        SELECT from_index, to_index, reason FROM loss_ledger
                        WHERE session_id = ? AND sensor = ?
                          AND from_index <= ? AND (to_index IS NULL OR to_index >= ?);
                        """,
                        (batch.session_id, batch.sensor, idx, idx),
                    )
                    loss_conflict = cursor.fetchone()
                    if loss_conflict is not None:
                        msg = (
                            f"Contradictory sample conflict on session {batch.session_id}, sensor {batch.sensor}: "
                            f"incoming sample index {idx} contradicts existing loss record "
                            f"[{loss_conflict['from_index']}, {loss_conflict['to_index']}] ({loss_conflict['reason']})"
                        )
                        logger.error(msg)
                        raise ContradictoryLossConflictError(msg)

                    # Query existing sample by composite primary key
                    cursor.execute(
                        """
                        SELECT ch1, ch2, ch3 FROM samples
                        WHERE session_id = ? AND sensor = ? AND sample_index = ?;
                        """,
                        (batch.session_id, batch.sensor, idx),
                    )
                    existing = cursor.fetchone()

                    if existing is not None:
                        # Check whether values match identically
                        existing_vals = (existing["ch1"], existing["ch2"], existing["ch3"])
                        incoming_vals = (ch1, ch2, ch3)
                        if existing_vals != incoming_vals:
                            msg = (
                                f"Integrity conflict for session {batch.session_id}, sensor {batch.sensor}, "
                                f"index {idx}: existing committed values {existing_vals} != incoming values {incoming_vals}"
                            )
                            logger.error(msg)
                            raise IntegrityConflictError(msg)
                        # Identical duplicate: safely skip insertion
                        self.telemetry.duplicate_samples_ignored += 1
                    else:
                        # Insert new sample
                        cursor.execute(
                            """
                            INSERT INTO samples (session_id, sensor, sample_index, ch1, ch2, ch3, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?);
                            """,
                            (batch.session_id, batch.sensor, idx, ch1, ch2, ch3, now),
                        )
                        new_samples_inserted += 1

                self.telemetry.samples_inserted += new_samples_inserted

                # 3. Contradictory Loss Check & Loss Ledger Insertion
                for loss in batch.lost:
                    # A. Contradictory loss check: Loss record must NOT cover existing persisted samples
                    if loss.to_index is not None:
                        cursor.execute(
                            """
                            SELECT COUNT(*) AS cnt FROM samples
                            WHERE session_id = ? AND sensor = ? 
                              AND sample_index >= ? AND sample_index <= ?;
                            """,
                            (batch.session_id, batch.sensor, loss.from_index, loss.to_index),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT COUNT(*) AS cnt FROM samples
                            WHERE session_id = ? AND sensor = ? AND sample_index >= ?;
                            """,
                            (batch.session_id, batch.sensor, loss.from_index),
                        )
                    overlap_count = cursor.fetchone()["cnt"]
                    if overlap_count > 0:
                        msg = (
                            f"Contradictory loss record on session {batch.session_id}, sensor {batch.sensor}: "
                            f"loss range [{loss.from_index}, {loss.to_index}] ({loss.reason}) overlaps "
                            f"{overlap_count} physically stored samples."
                        )
                        logger.error(msg)
                        raise ContradictoryLossConflictError(msg)

                    # B. Check for contradictory overlapping loss records with different reasons
                    effective_to = loss.to_index if loss.to_index is not None else 999999999
                    cursor.execute(
                        """
                        SELECT id, from_index, to_index, reason FROM loss_ledger
                        WHERE session_id = ? AND sensor = ?
                          AND from_index <= ? AND (to_index IS NULL OR to_index >= ?);
                        """,
                        (batch.session_id, batch.sensor, effective_to, loss.from_index),
                    )
                    overlapping_losses = cursor.fetchall()
                    for ex in overlapping_losses:
                        if ex["reason"] != loss.reason:
                            msg = (
                                f"Contradictory loss record on session {batch.session_id}, sensor {batch.sensor}: "
                                f"incoming loss [{loss.from_index}, {loss.to_index}] ({loss.reason}) overlaps "
                                f"existing loss [{ex['from_index']}, {ex['to_index']}] with conflicting reason '{ex['reason']}'"
                            )
                            logger.error(msg)
                            raise ContradictoryLossConflictError(msg)

                    # C. Adjacent loss merging for identical session and reason
                    merged = False
                    if loss.to_index is not None:
                        # Left adjacent: existing.to_index + 1 == loss.from_index
                        cursor.execute(
                            """
                            SELECT id, from_index, to_index FROM loss_ledger
                            WHERE session_id = ? AND sensor = ? AND reason = ? AND to_index = ?;
                            """,
                            (batch.session_id, batch.sensor, loss.reason, loss.from_index - 1),
                        )
                        left_adj = cursor.fetchone()

                        # Right adjacent: loss.to_index + 1 == existing.from_index
                        cursor.execute(
                            """
                            SELECT id, from_index, to_index FROM loss_ledger
                            WHERE session_id = ? AND sensor = ? AND reason = ? AND from_index = ?;
                            """,
                            (batch.session_id, batch.sensor, loss.reason, loss.to_index + 1),
                        )
                        right_adj = cursor.fetchone()

                        if left_adj and right_adj:
                            cursor.execute(
                                "UPDATE loss_ledger SET to_index = ? WHERE id = ?;",
                                (right_adj["to_index"], left_adj["id"]),
                            )
                            cursor.execute("DELETE FROM loss_ledger WHERE id = ?;", (right_adj["id"],))
                            merged = True
                        elif left_adj:
                            cursor.execute(
                                "UPDATE loss_ledger SET to_index = MAX(to_index, ?) WHERE id = ?;",
                                (loss.to_index, left_adj["id"]),
                            )
                            merged = True
                        elif right_adj:
                            cursor.execute(
                                "UPDATE loss_ledger SET from_index = MIN(from_index, ?) WHERE id = ?;",
                                (loss.from_index, right_adj["id"]),
                            )
                            merged = True

                    if not merged:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO loss_ledger (session_id, sensor, from_index, to_index, reason, detected_at)
                            VALUES (?, ?, ?, ?, ?, ?);
                            """,
                            (batch.session_id, batch.sensor, loss.from_index, loss.to_index, loss.reason, now),
                        )
                    self.telemetry.loss_records_inserted += 1

                    # Transition matching unresolved incidents to resolved via authoritative source evidence
                    if loss.to_index is not None:
                        cursor.execute(
                            """
                            UPDATE unresolved_incidents SET resolved = 1
                            WHERE session_id = ? AND sensor = ? 
                              AND from_index >= ? AND to_index <= ?
                              AND resolved = 0;
                            """,
                            (batch.session_id, batch.sensor, loss.from_index, loss.to_index),
                        )

                # 4. Handle Clean Session End
                if batch.session_end:
                    cursor.execute(
                        """
                        UPDATE sessions SET ended_at = ?, clean_shutdown = 1
                        WHERE session_id = ?;
                        """,
                        (now, batch.session_id),
                    )

                # 5. Compute Monotonic Cursor Progressions
                # Read current checkpoint
                cursor.execute(
                    """
                    SELECT contiguous_persisted_index, contiguous_resolved_index, highest_observed_index
                    FROM checkpoints
                    WHERE session_id = ? AND sensor = ?;
                    """,
                    (batch.session_id, batch.sensor),
                )
                ckpt_row = cursor.fetchone()
                if ckpt_row:
                    curr_persisted = ckpt_row["contiguous_persisted_index"]
                    curr_resolved = ckpt_row["contiguous_resolved_index"]
                    curr_obs = ckpt_row["highest_observed_index"]
                else:
                    curr_persisted = -1
                    curr_resolved = -1
                    curr_obs = -1

                # A. Update highest_observed_index
                new_obs = curr_obs
                if batch.data:
                    batch_max = batch.first_index + len(batch.data) - 1
                    new_obs = max(new_obs, batch_max)
                for loss in batch.lost:
                    if loss.to_index is not None:
                        new_obs = max(new_obs, loss.to_index)
                    else:
                        new_obs = max(new_obs, loss.from_index)

                # B. Advance contiguous_persisted_index (ONLY physically stored samples)
                new_persisted = curr_persisted
                cursor.execute(
                    """
                    SELECT sample_index FROM samples
                    WHERE session_id = ? AND sensor = ? AND sample_index > ?
                    ORDER BY sample_index ASC;
                    """,
                    (batch.session_id, batch.sensor, curr_persisted),
                )
                for row in cursor.fetchall():
                    s_idx = row["sample_index"]
                    if s_idx == new_persisted + 1:
                        new_persisted = s_idx
                    else:
                        # Encountered a gap in physical samples
                        break

                # C. Advance contiguous_resolved_index (stored samples OR authoritative loss)
                new_resolved = curr_resolved
                while True:
                    candidate = new_resolved + 1
                    # Check if sample exists in samples
                    cursor.execute(
                        """
                        SELECT 1 FROM samples
                        WHERE session_id = ? AND sensor = ? AND sample_index = ?;
                        """,
                        (batch.session_id, batch.sensor, candidate),
                    )
                    if cursor.fetchone():
                        new_resolved = candidate
                        continue

                    # Check if candidate is covered by authoritative loss in loss_ledger
                    cursor.execute(
                        """
                        SELECT to_index FROM loss_ledger
                        WHERE session_id = ? AND sensor = ? 
                          AND from_index <= ? AND (to_index IS NULL OR to_index >= ?)
                        ORDER BY from_index ASC LIMIT 1;
                        """,
                        (batch.session_id, batch.sensor, candidate, candidate),
                    )
                    loss_match = cursor.fetchone()
                    if loss_match:
                        to_idx = loss_match["to_index"]
                        if to_idx is not None and to_idx >= candidate:
                            new_resolved = to_idx
                            continue
                        else:
                            # Open-ended loss (terminal uncertainty) cannot resolve forward without bounded proof
                            break

                    # Unresolved gap or future unreceived sample encountered
                    break

                # D. Assert Monotonic Ordering Invariant: -1 <= I_persisted <= I_resolved <= I_obs
                effective_obs = max(new_obs, new_resolved)
                if not (-1 <= new_persisted <= new_resolved <= effective_obs):
                    msg = (
                        f"Cursor invariant violation on session {batch.session_id}, sensor {batch.sensor}: "
                        f"I_persisted={new_persisted}, I_resolved={new_resolved}, I_obs={effective_obs}"
                    )
                    logger.error(msg)
                    raise CursorInvariantViolationError(msg)

                new_obs = effective_obs

                # Update checkpoints table
                cursor.execute(
                    """
                    INSERT INTO checkpoints (session_id, sensor, contiguous_persisted_index, 
                                            contiguous_resolved_index, highest_observed_index, last_updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, sensor) DO UPDATE SET
                        contiguous_persisted_index = excluded.contiguous_persisted_index,
                        contiguous_resolved_index = excluded.contiguous_resolved_index,
                        highest_observed_index = excluded.highest_observed_index,
                        last_updated_at = excluded.last_updated_at;
                    """,
                    (batch.session_id, batch.sensor, new_persisted, new_resolved, new_obs, now),
                )

                # Commit Transaction
                cursor.execute("COMMIT;")
                self.telemetry.batches_committed += 1

                return CheckpointRecord(
                    session_id=batch.session_id,
                    sensor=batch.sensor,
                    contiguous_persisted_index=new_persisted,
                    contiguous_resolved_index=new_resolved,
                    highest_observed_index=new_obs,
                    last_updated_at=now,
                )
            except Exception as exc:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass
                self.telemetry.transactions_rolled_back += 1
                logger.debug(f"Transaction rolled back due to: {exc}")
                raise
            finally:
                cursor.close()

    def record_incident(self, incident: UnresolvedCrcIncident) -> None:
        """Durable persistence of an active or terminal unresolved CRC incident."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE;")
                cursor.execute(
                    """
                    INSERT INTO unresolved_incidents (
                        session_id, sensor, from_index, to_index, 
                        first_failed_at, retries_attempted, terminal, resolved
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    ON CONFLICT(session_id, sensor, from_index) DO UPDATE SET
                        to_index = excluded.to_index,
                        retries_attempted = excluded.retries_attempted,
                        terminal = excluded.terminal;
                    """,
                    (
                        incident.session_id,
                        incident.sensor,
                        incident.from_index,
                        incident.to_index,
                        incident.first_failed_at,
                        incident.retries_attempted,
                        1 if incident.terminal else 0,
                    ),
                )
                cursor.execute("COMMIT;")
                self.telemetry.incidents_persisted += 1
            except Exception:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                cursor.close()

    def mark_incident_resolved(self, session_id: str, sensor: int, from_index: int) -> None:
        """Mark an in-process incident as resolved upon verified recovery in database.
        
        Refuses to mark resolved if neither physical samples nor authoritative loss
        is actually committed in the database for the given index.
        """
        with self._lock:
            cursor = self._conn.cursor()
            try:
                # Verify that the index is actually stored in samples OR loss_ledger
                cursor.execute(
                    "SELECT 1 FROM samples WHERE session_id = ? AND sensor = ? AND sample_index = ?;",
                    (session_id, sensor, from_index),
                )
                has_sample = cursor.fetchone() is not None

                if not has_sample:
                    cursor.execute(
                        """
                        SELECT 1 FROM loss_ledger 
                        WHERE session_id = ? AND sensor = ? AND from_index <= ? AND (to_index IS NULL OR to_index >= ?);
                        """,
                        (session_id, sensor, from_index, from_index),
                    )
                    has_loss = cursor.fetchone() is not None
                else:
                    has_loss = False

                if not (has_sample or has_loss):
                    raise StorageError(
                        f"Cannot mark incident resolved for session {session_id}, sensor {sensor}, index {from_index}: "
                        "recovered data is not committed in database samples or loss_ledger."
                    )

                cursor.execute("BEGIN IMMEDIATE;")
                cursor.execute(
                    """
                    UPDATE unresolved_incidents SET resolved = 1
                    WHERE session_id = ? AND sensor = ? AND from_index = ?;
                    """,
                    (session_id, sensor, from_index),
                )
                cursor.execute("COMMIT;")
            except Exception:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                cursor.close()

    def get_unresolved_incidents(
        self,
        session_id: Optional[str] = None,
        terminal_only: bool = False,
    ) -> list[UnresolvedCrcIncident]:
        """Fetch unrecovered incidents from database, preserved across restarts."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                query = """
                    SELECT session_id, sensor, from_index, to_index, first_failed_at, retries_attempted, terminal
                    FROM unresolved_incidents
                    WHERE resolved = 0
                """
                params: list[Any] = []
                if session_id:
                    query += " AND session_id = ?"
                    params.append(session_id)
                if terminal_only:
                    query += " AND terminal = 1"
                query += " ORDER BY session_id, sensor, from_index ASC;"

                cursor.execute(query, params)
                results: list[UnresolvedCrcIncident] = []
                for row in cursor.fetchall():
                    results.append(
                        UnresolvedCrcIncident(
                            session_id=row["session_id"],
                            sensor=row["sensor"],
                            from_index=row["from_index"],
                            to_index=row["to_index"],
                            first_failed_at=row["first_failed_at"],
                            retries_attempted=row["retries_attempted"],
                            terminal=bool(row["terminal"]),
                        )
                    )
                return results
            finally:
                cursor.close()

    def reconstruct_authoritative_checkpoints(
        self,
        session_id: Optional[str] = None,
    ) -> dict[tuple[str, int], CheckpointRecord]:
        """Reconstruct authoritative checkpoints directly from committed samples and loss records."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                # Find all distinct (session_id, sensor) pairs across samples and loss_ledger
                query = """
                    SELECT DISTINCT session_id, sensor FROM samples
                    UNION
                    SELECT DISTINCT session_id, sensor FROM loss_ledger
                """
                params: list[Any] = []
                if session_id:
                    query = f"SELECT session_id, sensor FROM ({query}) WHERE session_id = ?"
                    params.append(session_id)
                query += " ORDER BY session_id, sensor;"

                cursor.execute(query, params)
                streams = cursor.fetchall()
                reconstructed: dict[tuple[str, int], CheckpointRecord] = {}

                for row in streams:
                    s_id = row["session_id"]
                    sensor = row["sensor"]

                    # 1. Authoritative contiguous persisted index (strictly stored samples)
                    persisted = -1
                    cursor.execute(
                        """
                        SELECT sample_index FROM samples
                        WHERE session_id = ? AND sensor = ?
                        ORDER BY sample_index ASC;
                        """,
                        (s_id, sensor),
                    )
                    sample_rows = cursor.fetchall()
                    sample_indices = set()
                    for s_row in sample_rows:
                        s_idx = s_row["sample_index"]
                        sample_indices.add(s_idx)
                        if s_idx == persisted + 1:
                            persisted = s_idx

                    # Fetch bounded loss intervals for this stream
                    cursor.execute(
                        """
                        SELECT from_index, to_index FROM loss_ledger
                        WHERE session_id = ? AND sensor = ? AND to_index IS NOT NULL
                        ORDER BY from_index ASC;
                        """,
                        (s_id, sensor),
                    )
                    loss_intervals = [
                        (l_row["from_index"], l_row["to_index"])
                        for l_row in cursor.fetchall()
                    ]

                    # 2. Authoritative contiguous resolved index (samples + authoritative loss)
                    # By monotonic invariant, resolved >= persisted; samples 0..persisted are guaranteed present.
                    resolved = persisted
                    while True:
                        cand = resolved + 1
                        if cand in sample_indices:
                            resolved = cand
                            continue

                        matched = False
                        for f_idx, t_idx in loss_intervals:
                            if f_idx <= cand <= t_idx:
                                resolved = t_idx
                                matched = True
                                break
                        if matched:
                            continue
                        break

                    # 3. Highest observed index
                    cursor.execute(
                        "SELECT MAX(sample_index) AS m_samp FROM samples WHERE session_id = ? AND sensor = ?;",
                        (s_id, sensor),
                    )
                    m_samp = cursor.fetchone()["m_samp"]
                    cursor.execute(
                        "SELECT MAX(to_index) AS m_loss FROM loss_ledger WHERE session_id = ? AND sensor = ?;",
                        (s_id, sensor),
                    )
                    m_loss = cursor.fetchone()["m_loss"]
                    obs = max(
                        -1,
                        m_samp if m_samp is not None else -1,
                        m_loss if m_loss is not None else -1,
                        resolved,
                    )

                    reconstructed[(s_id, sensor)] = CheckpointRecord(
                        session_id=s_id,
                        sensor=sensor,
                        contiguous_persisted_index=persisted,
                        contiguous_resolved_index=resolved,
                        highest_observed_index=obs,
                        last_updated_at=time.time(),
                    )

                return reconstructed
            finally:
                cursor.close()

    def recover_checkpoints_on_restart(self) -> dict[tuple[str, int], CheckpointRecord]:
        """Re-verify and restore checkpoints on startup from committed SQLite state.
        
        Validates database integrity and ensures that recorded checkpoints do NOT
        contradict the actual contiguous physical samples and authoritative loss records.
        If a checkpoint in the database is ahead of the contiguous verified contents,
        raises StorageError to fail safely.
        """
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("PRAGMA integrity_check;")
                integrity = cursor.fetchone()[0]
                if integrity != "ok":
                    raise StorageError(f"SQLite database integrity check failed on startup: {integrity}")

                # Compute authoritative checkpoints from raw table data
                authoritative = self.reconstruct_authoritative_checkpoints()

                # Read recorded checkpoints table
                cursor.execute("""
                    SELECT session_id, sensor, contiguous_persisted_index, 
                           contiguous_resolved_index, highest_observed_index, last_updated_at
                    FROM checkpoints;
                """)
                recorded_rows = cursor.fetchall()
                for row in recorded_rows:
                    key = (row["session_id"], row["sensor"])
                    auth_ckpt = authoritative.get(key)
                    true_persisted = auth_ckpt.contiguous_persisted_index if auth_ckpt else -1
                    true_resolved = auth_ckpt.contiguous_resolved_index if auth_ckpt else -1

                    # Check for contradiction: recorded checkpoint ahead of verified state
                    if row["contiguous_persisted_index"] > true_persisted:
                        raise StorageError(
                            f"Corrupted checkpoint detected for session {row['session_id']}, sensor {row['sensor']}: "
                            f"recorded contiguous_persisted_index={row['contiguous_persisted_index']} is ahead of "
                            f"actual verified physical samples (reconstructed={true_persisted})."
                        )
                    if row["contiguous_resolved_index"] > true_resolved:
                        raise StorageError(
                            f"Corrupted checkpoint detected for session {row['session_id']}, sensor {row['sensor']}: "
                            f"recorded contiguous_resolved_index={row['contiguous_resolved_index']} is ahead of "
                            f"actual verified resolved coverage (reconstructed={true_resolved})."
                        )

                    # Preserve highest observed index from checkpoint without treating observed gap as corruption
                    restored_obs = max(row["highest_observed_index"], auth_ckpt.highest_observed_index if auth_ckpt else -1)
                    authoritative[key] = CheckpointRecord(
                        session_id=row["session_id"],
                        sensor=row["sensor"],
                        contiguous_persisted_index=true_persisted,
                        contiguous_resolved_index=true_resolved,
                        highest_observed_index=restored_obs,
                        last_updated_at=row["last_updated_at"],
                    )

                return authoritative
            finally:
                cursor.close()

    def get_cloud_checkpoint(self, session_id: str, sensor: int) -> CloudReplicationCheckpoint:
        """Fetch the durable committed cloud replication checkpoint for (session_id, sensor)."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT cloud_persisted_index, cloud_resolved_index,
                           highest_observed_index, last_acked_at
                    FROM cloud_checkpoints
                    WHERE session_id = ? AND sensor = ?;
                    """,
                    (session_id, sensor),
                )
                row = cursor.fetchone()
                if row:
                    return CloudReplicationCheckpoint(
                        session_id=session_id,
                        sensor=sensor,
                        cloud_persisted_index=row["cloud_persisted_index"],
                        cloud_resolved_index=row["cloud_resolved_index"],
                        highest_observed_index=row["highest_observed_index"],
                        last_acked_at=row["last_acked_at"],
                    )
                return CloudReplicationCheckpoint(
                    session_id=session_id,
                    sensor=sensor,
                    cloud_persisted_index=-1,
                    cloud_resolved_index=-1,
                    highest_observed_index=-1,
                    last_acked_at=0.0,
                )
            finally:
                cursor.close()

    def update_cloud_checkpoint(
        self,
        session_id: str,
        sensor: int,
        cloud_persisted_index: int,
        cloud_resolved_index: int,
        highest_observed_index: int,
        last_acked_at: Optional[float] = None,
        acked_at: Optional[float] = None,
    ) -> CloudReplicationCheckpoint:
        """Atomically upsert the verified cloud acknowledgement cursors."""
        effective_acked_at = last_acked_at if last_acked_at is not None else (acked_at if acked_at is not None else time.time())
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE;")
                cursor.execute(
                    """
                    INSERT INTO cloud_checkpoints (
                        session_id, sensor, cloud_persisted_index,
                        cloud_resolved_index, highest_observed_index, last_acked_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, sensor) DO UPDATE SET
                        cloud_persisted_index = excluded.cloud_persisted_index,
                        cloud_resolved_index = excluded.cloud_resolved_index,
                        highest_observed_index = excluded.highest_observed_index,
                        last_acked_at = excluded.last_acked_at;
                    """,
                    (
                        session_id,
                        sensor,
                        cloud_persisted_index,
                        cloud_resolved_index,
                        highest_observed_index,
                        effective_acked_at,
                    ),
                )
                cursor.execute("COMMIT;")
                return CloudReplicationCheckpoint(
                    session_id=session_id,
                    sensor=sensor,
                    cloud_persisted_index=cloud_persisted_index,
                    cloud_resolved_index=cloud_resolved_index,
                    highest_observed_index=highest_observed_index,
                    last_acked_at=last_acked_at,
                )
            except Exception:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass
                raise
            finally:
                cursor.close()

    def get_all_cloud_checkpoints(self) -> list[CloudReplicationCheckpoint]:
        """Fetch all stored cloud replication checkpoints."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("""
                    SELECT session_id, sensor, cloud_persisted_index,
                           cloud_resolved_index, highest_observed_index, last_acked_at
                    FROM cloud_checkpoints
                    ORDER BY session_id, sensor;
                """)
                return [
                    CloudReplicationCheckpoint(
                        session_id=row["session_id"],
                        sensor=row["sensor"],
                        cloud_persisted_index=row["cloud_persisted_index"],
                        cloud_resolved_index=row["cloud_resolved_index"],
                        highest_observed_index=row["highest_observed_index"],
                        last_acked_at=row["last_acked_at"],
                    )
                    for row in cursor.fetchall()
                ]
            finally:
                cursor.close()

    def get_active_sessions(self) -> list[str]:
        """Return list of distinct session IDs recorded in local storage."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute("""
                    SELECT session_id FROM sessions
                    UNION
                    SELECT DISTINCT session_id FROM samples
                    UNION
                    SELECT DISTINCT session_id FROM loss_ledger
                    ORDER BY session_id;
                """)
                return [row["session_id"] for row in cursor.fetchall()]
            finally:
                cursor.close()

    def get_replication_backlog(self, session_id: Optional[str] = None) -> int:
        """Count total local samples not yet acknowledged by cloud."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                query = "SELECT DISTINCT session_id, sensor FROM checkpoints"
                params: list[Any] = []
                if session_id:
                    query += " WHERE session_id = ?"
                    params.append(session_id)
                cursor.execute(query, params)
                streams = cursor.fetchall()
                total_backlog = 0
                for s in streams:
                    s_id = s["session_id"]
                    sens = s["sensor"]
                    cloud_ckpt = self.get_cloud_checkpoint(s_id, sens)
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS cnt FROM samples
                        WHERE session_id = ? AND sensor = ? AND sample_index > ?;
                        """,
                        (s_id, sens, cloud_ckpt.cloud_resolved_index),
                    )
                    row = cursor.fetchone()
                    total_backlog += row["cnt"] if row else 0
                return total_backlog
            finally:
                cursor.close()

    def get_unreplicated_batch(
        self,
        session_id: str,
        sensor: int,
        max_samples: int = 400,
    ) -> Optional[IngestBatch]:
        """Construct the next bounded IngestBatch to send to the cloud receiver.

        Enforces:
        - Strict contiguous traversal starting from cloud_resolved_index + 1.
        - Immediate forwarding of authoritative loss records before subsequent samples.
        - Correct channel layout (3 for sensor 1, 2 for sensor 2).
        - Clean session_end detection upon full stream reconciliation.
        """
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cloud_ckpt = self.get_cloud_checkpoint(session_id, sensor)
                local_ckpt = self.get_checkpoint(session_id, sensor)

                if local_ckpt.highest_observed_index < 0:
                    return None

                next_idx = cloud_ckpt.cloud_resolved_index + 1

                # If cloud already caught up to local resolved cursor
                if next_idx > local_ckpt.contiguous_resolved_index:
                    return None

                # Check if next_idx is covered by an authoritative loss record
                cursor.execute(
                    """
                    SELECT from_index, to_index, reason FROM loss_ledger
                    WHERE session_id = ? AND sensor = ?
                      AND from_index <= ? AND (to_index IS NULL OR to_index >= ?)
                    ORDER BY from_index ASC LIMIT 1;
                    """,
                    (session_id, sensor, next_idx, next_idx),
                )
                loss_row = cursor.fetchone()
                if loss_row:
                    lr = LossRecord(
                        from_index=loss_row["from_index"],
                        to_index=loss_row["to_index"],
                        reason=loss_row["reason"],
                    )
                    return IngestBatch(
                        session_id=session_id,
                        sensor=sensor,
                        first_index=lr.from_index,
                        count=0,
                        data=[],
                        lost=[lr],
                        session_end=False,
                        crc32="",
                    )

                # Otherwise, fetch contiguous samples starting at next_idx
                cursor.execute(
                    """
                    SELECT sample_index, ch1, ch2, ch3 FROM samples
                    WHERE session_id = ? AND sensor = ? AND sample_index >= ?
                    ORDER BY sample_index ASC LIMIT ?;
                    """,
                    (session_id, sensor, next_idx, max_samples),
                )
                sample_rows = cursor.fetchall()
                if not sample_rows:
                    return None

                # Check if a loss record starts within this range to prevent crossing loss boundaries
                cursor.execute(
                    """
                    SELECT MIN(from_index) AS min_loss FROM loss_ledger
                    WHERE session_id = ? AND sensor = ? AND from_index >= ?;
                    """,
                    (session_id, sensor, next_idx),
                )
                min_loss_row = cursor.fetchone()
                min_loss_idx = min_loss_row["min_loss"] if min_loss_row and min_loss_row["min_loss"] is not None else float("inf")

                contiguous_samples: list[list[int]] = []
                expected_idx = next_idx
                for s in sample_rows:
                    s_idx = s["sample_index"]
                    if s_idx != expected_idx or s_idx >= min_loss_idx:
                        break
                    if sensor == 1:
                        contiguous_samples.append([s["ch1"], s["ch2"], s["ch3"]])
                    else:
                        contiguous_samples.append([s["ch1"], s["ch2"]])
                    expected_idx += 1

                if not contiguous_samples:
                    return None

                # Check if session clean shutdown recorded and this batch completes stream
                session_end = False
                if expected_idx - 1 >= local_ckpt.contiguous_resolved_index:
                    cursor.execute(
                        "SELECT clean_shutdown FROM sessions WHERE session_id = ?;",
                        (session_id,),
                    )
                    s_row = cursor.fetchone()
                    if s_row and s_row["clean_shutdown"]:
                        session_end = True

                return IngestBatch(
                    session_id=session_id,
                    sensor=sensor,
                    first_index=next_idx,
                    count=len(contiguous_samples),
                    data=contiguous_samples,
                    lost=[],
                    session_end=session_end,
                    crc32="",
                )
            finally:
                cursor.close()
