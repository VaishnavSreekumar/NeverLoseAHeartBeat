"""MayaCardio Cloud Durable Storage Engine (Phase 4A).

Responsible for:
- SQLite Write-Ahead Logging (WAL) mode with explicit synchronous durability configuration.
- Composite primary key (session_id, sensor, sample_index) preventing silent overwrite.
- Explicit duplicate validation:
  * Identical duplicates are safely ignored (idempotent network retries).
  * Conflicting channel values for the same index raise CloudIntegrityConflictError.
- Contradictory loss detection (bidirectional):
  * Incoming samples overlapping existing loss records raise CloudContradictoryLossConflictError.
  * Incoming loss records overlapping physically persisted samples raise CloudContradictoryLossConflictError.
  * Overlapping loss records with different reasons raise CloudContradictoryLossConflictError.
- Adjacent loss record merging for identical reasons and streams.
- Explicit dual contiguous cursor tracking:
  * contiguous_persisted_index (P): highest index such that every index 0..P exists
    as a physically stored sample in SQLite (strictly samples, never loss).
  * contiguous_resolved_index (R): highest index such that every index 0..R is accounted
    for as either a stored sample or an authoritative bounded loss record (never across gaps).
  * highest_observed_index (O): highest sample or loss index observed across all batches.
- Cursor relationship and out-of-order semantics:
  * When a stream begins with physical samples and has no loss: P == R.
  * When authoritative loss exists before or between samples (e.g. loss at [0, 49] and
    samples at [50, 99]), P stops at the first missing sample (P = -1), while R advances
    across the bounded loss to 99. Here P < R.
  * Out-of-order batches (e.g. [100, 199] arriving before [0, 99]) are durably persisted
    in the samples table, but neither P nor R advances past the gap (-1 <= P, R <= gap_start - 1).
    Data is NEVER acknowledged as resolved merely because a later batch was stored.
  * Open-ended loss records (to_index IS NULL) represent terminal uncertainty and halt
    forward advancement of R at from_index - 1.
- Crash-consistent restart recovery via authoritative checkpoint reconstruction.
- Atomic batch transactions (BEGIN IMMEDIATE ... COMMIT / ROLLBACK).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Optional, Sequence

from pipeline.processing import LossRange, WindowResult
from pipeline.protocol_client import IngestBatch
from shared.protocol import LossRecord

logger = logging.getLogger("pipeline.cloud.storage")


class CloudStorageError(Exception):
    """Base exception for cloud storage failures."""
    pass


class CloudIntegrityConflictError(CloudStorageError):
    """Raised when an incoming sample has identical index but conflicting channel values."""
    pass


class CloudContradictoryLossConflictError(CloudStorageError):
    """Raised when incoming samples contradict existing loss or incoming loss contradicts samples."""
    pass


@dataclass(frozen=True)
class CloudCheckpointRecord:
    """Committed checkpoint state for a cloud (session_id, sensor) stream."""
    session_id: str
    sensor: int
    contiguous_persisted_index: int
    contiguous_resolved_index: int
    highest_observed_index: int
    last_updated_at: float

    @property
    def next_recovery_index(self) -> int:
        """The exact index to request on wire restart: contiguous_resolved_index + 1."""
        return self.contiguous_resolved_index + 1


@dataclass
class CloudStorageTelemetry:
    """Operational metrics for the cloud storage engine."""
    batches_committed: int = 0
    samples_inserted: int = 0
    duplicate_samples_ignored: int = 0
    loss_records_inserted: int = 0
    transactions_rolled_back: int = 0


class CloudStorageEngine:
    """Durable SQLite storage engine for the cloud receiver."""

    def __init__(
        self,
        db_path: str | Path = "biosignal_cloud.db",
        synchronous: Optional[str] = None,
        busy_timeout_ms: int = 5000,
    ) -> None:
        """Initialize the cloud storage engine.

        Durability Tradeoffs:
        - PRAGMA synchronous = NORMAL (default):
          Provides 100% durability against application or container process crashes
          (SIGKILL, fatal exceptions, sudden process exit). The SQLite WAL ensures the
          database file is never corrupted and all committed transactions survive.
          Does not issue an fsync on every transaction commit, relying on OS dirty-page
          writeback.
        - PRAGMA synchronous = FULL:
          Issues an fsync on every transaction commit. Guarantees durability against physical
          host power loss (assuming non-volatile hardware write caches). Tradeoff is
          higher I/O latency per micro-batch.

        Args:
            db_path: Path to SQLite database file or ':memory:'.
            synchronous: SQLite PRAGMA synchronous mode ('NORMAL' or 'FULL').
                Defaults to CLOUD_SQLITE_SYNCHRONOUS env var, or 'NORMAL'.
            busy_timeout_ms: Timeout in milliseconds waiting on database locks.
        """
        self.db_path = str(db_path)
        if synchronous is None:
            synchronous = os.getenv("CLOUD_SQLITE_SYNCHRONOUS", "NORMAL")
        self.synchronous = synchronous.upper()
        self.busy_timeout_ms = busy_timeout_ms
        self.telemetry = CloudStorageTelemetry()
        self._lock = threading.RLock()

        self._conn: sqlite3.Connection = sqlite3.connect(
            self.db_path,
            timeout=busy_timeout_ms / 1000.0,
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode; explicit BEGIN IMMEDIATE blocks
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
                    CREATE TABLE IF NOT EXISTS window_results (
                        session_id TEXT NOT NULL,
                        window INTEGER NOT NULL,
                        mode TEXT NOT NULL,
                        complete BOOLEAN NOT NULL,
                        missing_1 INTEGER NOT NULL,
                        missing_2 INTEGER NOT NULL,
                        magnitude REAL,
                        heart_rate REAL,
                        result_json TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        PRIMARY KEY (session_id, window)
                    );
                """)

                # High-throughput query indexes
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cloud_samples_cursor 
                    ON samples (session_id, sensor, sample_index);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cloud_loss_cursor
                    ON loss_ledger (session_id, sensor, from_index, to_index);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cloud_window_results
                    ON window_results (session_id, window);
                """)
            finally:
                cursor.close()

    def close(self) -> None:
        """Close the database connection safely."""
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def save_batch(self, batch: IngestBatch) -> CloudCheckpointRecord:
        """Atomically persist an ingestion batch into cloud storage.

        Enforces:
        - Atomic all-or-nothing transactions.
        - Identical duplicates ignored safely.
        - Conflicting sample values raise CloudIntegrityConflictError.
        - Bidirectional contradictory loss detection raises CloudContradictoryLossConflictError.
        - Dual contiguous cursor calculation (persisted vs resolved).
        """
        with self._lock:
            now = time.time()
            cursor = self._conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE;")

                # 1. Register Session Record
                cursor.execute(
                    """
                    INSERT INTO sessions (session_id, started_at)
                    VALUES (?, ?)
                    ON CONFLICT(session_id) DO NOTHING;
                    """,
                    (batch.session_id, now),
                )

                # 2. Validate and Insert Physical Samples
                batch_samples_inserted = 0
                batch_duplicates_ignored = 0
                batch_losses_inserted = 0

                for offset, values in enumerate(batch.data):
                    idx = batch.first_index + offset
                    ch1 = values[0]
                    ch2 = values[1]
                    ch3 = values[2] if len(values) > 2 else None

                    # A. Check against contradictory loss records
                    cursor.execute(
                        """
                        SELECT from_index, to_index, reason FROM loss_ledger
                        WHERE session_id = ? AND sensor = ?
                          AND from_index <= ? AND (to_index IS NULL OR to_index >= ?)
                        LIMIT 1;
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
                        raise CloudContradictoryLossConflictError(msg)

                    # B. Check existing sample for duplicates or conflicts
                    cursor.execute(
                        """
                        SELECT ch1, ch2, ch3 FROM samples
                        WHERE session_id = ? AND sensor = ? AND sample_index = ?;
                        """,
                        (batch.session_id, batch.sensor, idx),
                    )
                    existing = cursor.fetchone()

                    if existing is not None:
                        existing_vals = (existing["ch1"], existing["ch2"], existing["ch3"])
                        incoming_vals = (ch1, ch2, ch3)
                        if existing_vals != incoming_vals:
                            msg = (
                                f"Integrity conflict for session {batch.session_id}, sensor {batch.sensor}, "
                                f"index {idx}: existing values {existing_vals} != incoming {incoming_vals}"
                            )
                            logger.error(msg)
                            raise CloudIntegrityConflictError(msg)
                        batch_duplicates_ignored += 1
                    else:
                        cursor.execute(
                            """
                            INSERT INTO samples (session_id, sensor, sample_index, ch1, ch2, ch3, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?);
                            """,
                            (batch.session_id, batch.sensor, idx, ch1, ch2, ch3, now),
                        )
                        batch_samples_inserted += 1

                # 3. Validate and Insert Loss Records
                for loss in batch.lost:
                    # A. Contradictory check: loss record must NOT cover existing persisted samples
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
                        raise CloudContradictoryLossConflictError(msg)

                    # B. Check for conflicting loss reason on overlapping range
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
                                f"Contradictory loss reason conflict on session {batch.session_id}, sensor {batch.sensor}: "
                                f"incoming [{loss.from_index}, {loss.to_index}] reason '{loss.reason}' contradicts "
                                f"existing [{ex['from_index']}, {ex['to_index']}] reason '{ex['reason']}'"
                            )
                            logger.error(msg)
                            raise CloudContradictoryLossConflictError(msg)

                    # C. Open-ended loss closure and adjacent interval merging
                    merged = False
                    if loss.to_index is not None:
                        # Check for exact open-ended loss record for same session, sensor, from_index, and reason
                        cursor.execute(
                            """
                            SELECT id FROM loss_ledger
                            WHERE session_id = ? AND sensor = ? AND reason = ? AND from_index = ? AND to_index IS NULL;
                            """,
                            (batch.session_id, batch.sensor, loss.reason, loss.from_index),
                        )
                        open_match = cursor.fetchone()
                        if open_match:
                            # Authoritative bounded record closes this exact open-ended record
                            closed_id = open_match["id"]
                            cursor.execute(
                                "UPDATE loss_ledger SET to_index = ? WHERE id = ?;",
                                (loss.to_index, closed_id),
                            )
                            merged = True

                            # Check if closed record merges with right-adjacent record:
                            cursor.execute(
                                """
                                SELECT id, from_index, to_index FROM loss_ledger
                                WHERE session_id = ? AND sensor = ? AND reason = ? AND from_index = ?;
                                """,
                                (batch.session_id, batch.sensor, loss.reason, loss.to_index + 1),
                            )
                            right_adj = cursor.fetchone()
                            if right_adj:
                                cursor.execute(
                                    "UPDATE loss_ledger SET to_index = ? WHERE id = ?;",
                                    (right_adj["to_index"], closed_id),
                                )
                                cursor.execute("DELETE FROM loss_ledger WHERE id = ?;", (right_adj["id"],))

                            # Check if closed record merges with left-adjacent record:
                            cursor.execute(
                                """
                                SELECT id, from_index, to_index FROM loss_ledger
                                WHERE session_id = ? AND sensor = ? AND reason = ? AND to_index = ?;
                                """,
                                (batch.session_id, batch.sensor, loss.reason, loss.from_index - 1),
                            )
                            left_adj = cursor.fetchone()
                            if left_adj:
                                cursor.execute(
                                    "UPDATE loss_ledger SET to_index = (SELECT to_index FROM loss_ledger WHERE id = ?) WHERE id = ?;",
                                    (closed_id, left_adj["id"]),
                                )
                                cursor.execute("DELETE FROM loss_ledger WHERE id = ?;", (closed_id,))
                        else:
                            # Standard adjacent loss merging
                            cursor.execute(
                                """
                                SELECT id, from_index, to_index FROM loss_ledger
                                WHERE session_id = ? AND sensor = ? AND reason = ? AND to_index = ?;
                                """,
                                (batch.session_id, batch.sensor, loss.reason, loss.from_index - 1),
                            )
                            left_adj = cursor.fetchone()

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
                    batch_losses_inserted += 1

                # 4. Clean session end flag
                if batch.session_end:
                    cursor.execute(
                        """
                        UPDATE sessions SET ended_at = ?, clean_shutdown = 1
                        WHERE session_id = ?;
                        """,
                        (now, batch.session_id),
                    )

                # 5. Dual Contiguous Cursor Calculation
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

                # A. highest_observed_index
                new_obs = curr_obs
                if batch.data:
                    batch_max = batch.first_index + len(batch.data) - 1
                    new_obs = max(new_obs, batch_max)
                for loss in batch.lost:
                    if loss.to_index is not None:
                        new_obs = max(new_obs, loss.to_index)
                    else:
                        new_obs = max(new_obs, loss.from_index)

                # B. contiguous_persisted_index (P)
                # Strictly physical samples contiguous from 0. NEVER advances across loss or gaps.
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
                        break

                # C. contiguous_resolved_index (R)
                # Advances across stored samples OR authoritative bounded loss.
                # Halts at unresolved gaps or open-ended loss.
                # Optimization: samples 0..new_persisted are guaranteed physically present in SQLite.
                # Therefore, resolution is guaranteed up to new_persisted.
                new_resolved = max(curr_resolved, new_persisted)
                while True:
                    candidate = new_resolved + 1
                    # Check physical sample
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

                    # Check authoritative bounded loss
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
                            # Open-ended loss (terminal uncertainty) halts resolution
                            break

                    # Unresolved gap or unreceived sample
                    break

                effective_obs = max(new_obs, new_resolved)

                # 6. Update Checkpoints Table
                cursor.execute(
                    """
                    INSERT INTO checkpoints (
                        session_id, sensor, contiguous_persisted_index,
                        contiguous_resolved_index, highest_observed_index, last_updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, sensor) DO UPDATE SET
                        contiguous_persisted_index = excluded.contiguous_persisted_index,
                        contiguous_resolved_index = excluded.contiguous_resolved_index,
                        highest_observed_index = excluded.highest_observed_index,
                        last_updated_at = excluded.last_updated_at;
                    """,
                    (batch.session_id, batch.sensor, new_persisted, new_resolved, effective_obs, now),
                )

                cursor.execute("COMMIT;")
                self.telemetry.batches_committed += 1
                self.telemetry.samples_inserted += batch_samples_inserted
                self.telemetry.duplicate_samples_ignored += batch_duplicates_ignored
                self.telemetry.loss_records_inserted += batch_losses_inserted

                return CloudCheckpointRecord(
                    session_id=batch.session_id,
                    sensor=batch.sensor,
                    contiguous_persisted_index=new_persisted,
                    contiguous_resolved_index=new_resolved,
                    highest_observed_index=effective_obs,
                    last_updated_at=now,
                )
            except Exception as exc:
                try:
                    cursor.execute("ROLLBACK;")
                except Exception:
                    pass
                self.telemetry.transactions_rolled_back += 1
                logger.debug(f"Cloud transaction rolled back: {exc}")
                raise
            finally:
                cursor.close()

    def get_checkpoint(self, session_id: str, sensor: int) -> Optional[CloudCheckpointRecord]:
        """Fetch committed checkpoint for a stream."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT contiguous_persisted_index, contiguous_resolved_index, 
                           highest_observed_index, last_updated_at
                    FROM checkpoints
                    WHERE session_id = ? AND sensor = ?;
                    """,
                    (session_id, sensor),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return CloudCheckpointRecord(
                    session_id=session_id,
                    sensor=sensor,
                    contiguous_persisted_index=row["contiguous_persisted_index"],
                    contiguous_resolved_index=row["contiguous_resolved_index"],
                    highest_observed_index=row["highest_observed_index"],
                    last_updated_at=row["last_updated_at"],
                )
            finally:
                cursor.close()

    def get_all_checkpoints(self, session_id: Optional[str] = None) -> list[CloudCheckpointRecord]:
        """Fetch all committed checkpoints, optionally filtered by session."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                query = """
                    SELECT session_id, sensor, contiguous_persisted_index,
                           contiguous_resolved_index, highest_observed_index, last_updated_at
                    FROM checkpoints
                """
                params: list[Any] = []
                if session_id:
                    query += " WHERE session_id = ?"
                    params.append(session_id)
                query += " ORDER BY session_id, sensor;"
                cursor.execute(query, params)
                return [
                    CloudCheckpointRecord(
                        session_id=r["session_id"],
                        sensor=r["sensor"],
                        contiguous_persisted_index=r["contiguous_persisted_index"],
                        contiguous_resolved_index=r["contiguous_resolved_index"],
                        highest_observed_index=r["highest_observed_index"],
                        last_updated_at=r["last_updated_at"],
                    )
                    for r in cursor.fetchall()
                ]
            finally:
                cursor.close()

    def reconstruct_authoritative_checkpoints(
        self, session_id: Optional[str] = None
    ) -> dict[tuple[str, int], CloudCheckpointRecord]:
        """Reconstruct authoritative checkpoints directly from committed raw table contents.

        Used to recover exact state after a server crash/restart.
        """
        with self._lock:
            cursor = self._conn.cursor()
            try:
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
                reconstructed: dict[tuple[str, int], CloudCheckpointRecord] = {}

                for row in streams:
                    s_id = row["session_id"]
                    sensor = row["sensor"]

                    # 1. Contiguous persisted index (strictly stored samples from 0)
                    persisted = -1
                    cursor.execute(
                        """
                        SELECT sample_index FROM samples
                        WHERE session_id = ? AND sensor = ?
                        ORDER BY sample_index ASC;
                        """,
                        (s_id, sensor),
                    )
                    for s_row in cursor.fetchall():
                        if s_row["sample_index"] == persisted + 1:
                            persisted = s_row["sample_index"]
                        elif s_row["sample_index"] > persisted + 1:
                            break

                    # 2. Contiguous resolved index (samples + authoritative loss)
                    # Samples 0..persisted are guaranteed present, so resolved is at least persisted
                    resolved = max(-1, persisted)
                    while True:
                        cand = resolved + 1
                        cursor.execute(
                            "SELECT 1 FROM samples WHERE session_id = ? AND sensor = ? AND sample_index = ?;",
                            (s_id, sensor, cand),
                        )
                        if cursor.fetchone():
                            resolved = cand
                            continue

                        cursor.execute(
                            """
                            SELECT to_index FROM loss_ledger
                            WHERE session_id = ? AND sensor = ? AND from_index <= ? 
                              AND (to_index IS NULL OR to_index >= ?)
                            ORDER BY from_index ASC LIMIT 1;
                            """,
                            (s_id, sensor, cand, cand),
                        )
                        l_row = cursor.fetchone()
                        if l_row and l_row["to_index"] is not None and l_row["to_index"] >= cand:
                            resolved = l_row["to_index"]
                            continue
                        break

                    # 3. Highest observed index
                    cursor.execute(
                        "SELECT MAX(sample_index) AS m_samp FROM samples WHERE session_id = ? AND sensor = ?;",
                        (s_id, sensor),
                    )
                    m_samp = cursor.fetchone()["m_samp"]
                    cursor.execute(
                        "SELECT MAX(to_index) AS m_loss_to, MAX(from_index) AS m_loss_from FROM loss_ledger WHERE session_id = ? AND sensor = ?;",
                        (s_id, sensor),
                    )
                    l_res = cursor.fetchone()
                    m_loss_to = l_res["m_loss_to"]
                    m_loss_from = l_res["m_loss_from"]

                    obs = max(
                        -1,
                        m_samp if m_samp is not None else -1,
                        m_loss_to if m_loss_to is not None else -1,
                        m_loss_from if m_loss_from is not None else -1,
                        resolved,
                    )

                    reconstructed[(s_id, sensor)] = CloudCheckpointRecord(
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

    def get_samples_for_session(
        self, session_id: str
    ) -> tuple[dict[int, tuple[int, ...]], dict[int, tuple[int, ...]]]:
        """Fetch all samples for a session separated into sensor 1 and sensor 2 maps."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT sensor, sample_index, ch1, ch2, ch3
                    FROM samples
                    WHERE session_id = ?
                    ORDER BY sample_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                s1: dict[int, tuple[int, ...]] = {}
                s2: dict[int, tuple[int, ...]] = {}
                for r in cursor.fetchall():
                    sensor = r["sensor"]
                    idx = r["sample_index"]
                    if sensor == 1:
                        s1[idx] = (r["ch1"], r["ch2"], r["ch3"])
                    else:
                        s2[idx] = (r["ch1"], r["ch2"])
                return s1, s2
            finally:
                cursor.close()

    def get_loss_records_for_session(self, session_id: str) -> list[LossRange]:
        """Fetch all loss records for a session as LossRange objects."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT sensor, from_index, to_index, reason
                    FROM loss_ledger
                    WHERE session_id = ?
                    ORDER BY from_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                return [
                    LossRange(
                        sensor=r["sensor"],
                        from_index=r["from_index"],
                        to_index=r["to_index"],
                        reason=r["reason"],
                    )
                    for r in cursor.fetchall()
                ]
            finally:
                cursor.close()

    def is_session_ended(self, session_id: str) -> bool:
        """Check if session has cleanly finished."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    "SELECT clean_shutdown FROM sessions WHERE session_id = ?;",
                    (session_id,),
                )
                row = cursor.fetchone()
                return bool(row["clean_shutdown"]) if row else False
            finally:
                cursor.close()

    def get_active_sessions(self) -> list[str]:
        """List distinct session IDs currently present in storage."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT session_id FROM sessions
                    UNION
                    SELECT DISTINCT session_id FROM samples
                    UNION
                    SELECT DISTINCT session_id FROM loss_ledger
                    ORDER BY session_id;
                    """
                )
                return [r["session_id"] for r in cursor.fetchall()]
            finally:
                cursor.close()

    def session_exists(self, session_id: str) -> bool:
        """Check whether a session exists in sessions, samples, or loss_ledger."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT 1 FROM sessions WHERE session_id = ?
                    UNION
                    SELECT 1 FROM samples WHERE session_id = ?
                    UNION
                    SELECT 1 FROM loss_ledger WHERE session_id = ?
                    LIMIT 1;
                    """,
                    (session_id, session_id, session_id),
                )
                return cursor.fetchone() is not None
            finally:
                cursor.close()

    def get_counts(self, session_id: Optional[str] = None) -> tuple[int, int]:
        """Return total (samples_count, loss_records_count), optionally filtered by session_id."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                if session_id:
                    cursor.execute("SELECT COUNT(*) AS c FROM samples WHERE session_id = ?;", (session_id,))
                    samp_cnt = cursor.fetchone()["c"]
                    cursor.execute("SELECT COUNT(*) AS c FROM loss_ledger WHERE session_id = ?;", (session_id,))
                    loss_cnt = cursor.fetchone()["c"]
                else:
                    cursor.execute("SELECT COUNT(*) AS c FROM samples;")
                    samp_cnt = cursor.fetchone()["c"]
                    cursor.execute("SELECT COUNT(*) AS c FROM loss_ledger;")
                    loss_cnt = cursor.fetchone()["c"]
                return samp_cnt, loss_cnt
            finally:
                cursor.close()

    def save_window_result(self, result: WindowResult) -> None:
        """Persist a computed Section 3 window result."""
        self.save_window_results([result])

    def save_window_results(self, results: list[WindowResult]) -> None:
        """Persist a list of computed Section 3 window results in a single atomic transaction."""
        if not results:
            return
        with self._lock:
            cursor = self._conn.cursor()
            now = time.time()
            try:
                cursor.execute("BEGIN IMMEDIATE;")
                cursor.executemany(
                    """
                    INSERT INTO window_results (
                        session_id, window, mode, complete, missing_1, missing_2,
                        magnitude, heart_rate, result_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, window) DO UPDATE SET
                        mode = excluded.mode,
                        complete = excluded.complete,
                        missing_1 = excluded.missing_1,
                        missing_2 = excluded.missing_2,
                        magnitude = excluded.magnitude,
                        heart_rate = excluded.heart_rate,
                        result_json = excluded.result_json,
                        created_at = excluded.created_at;
                    """,
                    [
                        (
                            res.session_id or "",
                            res.window,
                            res.mode,
                            1 if res.complete else 0,
                            res.missing_1,
                            res.missing_2,
                            res.magnitude,
                            res.heart_rate,
                            json.dumps(res.to_dict()),
                            now,
                        )
                        for res in results
                    ],
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

    def get_window_results(self, session_id: str) -> list[WindowResult]:
        """Fetch all computed window results for a session ordered by window."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT result_json FROM window_results
                    WHERE session_id = ?
                    ORDER BY window ASC;
                    """,
                    (session_id,),
                )
                results: list[WindowResult] = []
                for row in cursor.fetchall():
                    d = json.loads(row["result_json"])
                    results.append(
                        WindowResult(
                            window=d["window"],
                            mode=d["mode"],
                            missing_1=d["missing_1"],
                            missing_2=d["missing_2"],
                            complete=d["complete"],
                            mean={1: d["mean"]["1"], 2: d["mean"]["2"]},
                            min={1: d["min"]["1"], 2: d["min"]["2"]},
                            max={1: d["max"]["1"], 2: d["max"]["2"]},
                            magnitude=d["magnitude"],
                            heart_rate=d["heart_rate"],
                            session_id=d.get("session_id"),
                        )
                    )
                return results
            finally:
                cursor.close()

    def export_ndjson_records(self, session_id: str) -> list[dict[str, Any]]:
        """Generate official Section 4 NDJSON export records for a session."""
        records: list[dict[str, Any]] = []
        with self._lock:
            cursor = self._conn.cursor()
            try:
                # 1. Samples ordered by sample_index ASC, sensor ASC
                cursor.execute(
                    """
                    SELECT sensor, sample_index, ch1, ch2, ch3
                    FROM samples
                    WHERE session_id = ?
                    ORDER BY sample_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                for r in cursor.fetchall():
                    sensor = r["sensor"]
                    if sensor == 1:
                        values = [r["ch1"], r["ch2"], r["ch3"]]
                    else:
                        values = [r["ch1"], r["ch2"]]
                    records.append({
                        "session_id": session_id,
                        "sensor": sensor,
                        "index": r["sample_index"],
                        "values": values,
                    })

                # 2. Loss records ordered by from_index ASC, sensor ASC
                cursor.execute(
                    """
                    SELECT sensor, from_index, to_index, reason
                    FROM loss_ledger
                    WHERE session_id = ?
                    ORDER BY from_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                for r in cursor.fetchall():
                    records.append({
                        "session_id": session_id,
                        "sensor": r["sensor"],
                        "from_index": r["from_index"],
                        "to_index": r["to_index"],
                        "reason": r["reason"],
                    })
                return records
            finally:
                cursor.close()
