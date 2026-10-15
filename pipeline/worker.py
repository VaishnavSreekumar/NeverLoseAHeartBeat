"""Background storage worker bridging BiosignalProtocolClient and StorageEngine.

Responsible for:
- Draining validated batches from BiosignalProtocolClient.accepted_queue.
- Executing atomic commits into StorageEngine via thread executor without blocking the event loop.
- Providing durable recovery cursor hook to client (from_index = I_resolved + 1).
- Queue acknowledgment integrity: task_done() is ONLY called when a batch is durably committed.
- Failure handling: preserves failed unpersisted batches in last_failed_batch and last_fatal_error.
- Transient retry with exponential backoff for database locks.
- Graceful shutdown allowing in-flight commits to complete before worker exit.
- Incident synchronization across quiet intervals and shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Optional

from pipeline.protocol_client import BiosignalProtocolClient, IngestBatch
from pipeline.storage import CheckpointRecord, StorageEngine

logger = logging.getLogger("pipeline.worker")


class StorageIngestionWorker:
    """Async background worker draining client batches and persisting to SQLite."""

    def __init__(
        self,
        client: BiosignalProtocolClient,
        storage: StorageEngine,
        max_retries: int = 3,
        retry_delay: float = 0.05,
    ) -> None:
        self.client = client
        self.storage = storage
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._running = False
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()

        # State tracking for failure analysis & auditing
        self.last_failed_batch: Optional[IngestBatch] = None
        self.last_fatal_error: Optional[Exception] = None

        # Wire client's recovery cursor provider to query SQLite checkpoints
        self.client.recovery_cursor_provider = self.get_recovery_cursor

    def get_recovery_cursor(self, session_id: str, sensor: int) -> int:
        """Provide current contiguous resolved cursor from storage: I_resolved."""
        ckpt = self.storage.get_checkpoint(session_id, sensor)
        return ckpt.contiguous_resolved_index

    async def _process_batch_with_retry(self, batch: IngestBatch) -> CheckpointRecord:
        """Commit batch to SQLite in threadpool with bounded retry for transient locks."""
        loop = asyncio.get_running_loop()
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                return await loop.run_in_executor(None, self.storage.commit_batch, batch)
            except sqlite3.OperationalError as exc:
                last_exc = exc
                err_msg = str(exc).lower()
                # Fast fail on non-transient disk I/O, disk full, or readonly errors
                if "disk" in err_msg or "full" in err_msg or "readonly" in err_msg:
                    raise
                # Transient lock/busy: brief sleep and retry
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
            except Exception:
                # Fatal integrity, contradictory loss, or cursor errors fail immediately
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Unexpected retry loop termination")

    async def _sync_incidents(self) -> None:
        """Persist newly captured terminal or unresolved incidents into SQLite."""
        loop = asyncio.get_running_loop()
        for inc in list(self.client.terminal_unresolved_incidents):
            await loop.run_in_executor(None, self.storage.record_incident, inc)
        for inc in list(self.client.unresolved_crc_incidents):
            await loop.run_in_executor(None, self.storage.record_incident, inc)

    async def run(self) -> None:
        """Worker loop processing batches from accepted_queue with strict ack semantics."""
        self._running = True
        logger.info("StorageIngestionWorker started.")

        try:
            while not self._stop_event.is_set():
                try:
                    # Bounded wait for next batch
                    batch = await asyncio.wait_for(
                        self.client.accepted_queue.get(),
                        timeout=0.2,
                    )
                except asyncio.TimeoutError:
                    # Periodically sync incidents during quiet intervals
                    await self._sync_incidents()
                    continue

                try:
                    ckpt = await self._process_batch_with_retry(batch)
                    # ONLY acknowledge queue when transaction is durably committed!
                    self.client.accepted_queue.task_done()
                    logger.debug(
                        f"Committed batch for sensor {batch.sensor} at index {batch.first_index}. "
                        f"Cursors: I_persisted={ckpt.contiguous_persisted_index}, "
                        f"I_resolved={ckpt.contiguous_resolved_index}, I_obs={ckpt.highest_observed_index}"
                    )
                except Exception as exc:
                    self.last_failed_batch = batch
                    self.last_fatal_error = exc
                    logger.error(
                        f"Storage commit failed for batch {batch.sensor}@{batch.first_index}: {exc}. "
                        "Preserving unpersisted batch and halting worker."
                    )
                    # Signal client to pause/stop polling to prevent queue overflow and missing data
                    if hasattr(self.client, "storage_error_event"):
                        self.client.storage_error_event.set()
                    raise

                # Sync any newly archived incidents
                await self._sync_incidents()

        except asyncio.CancelledError:
            pass
        finally:
            # Flush remaining batches in queue on shutdown
            await self._flush_remaining()
            self._running = False
            logger.info("StorageIngestionWorker stopped.")

    async def _flush_remaining(self) -> None:
        """Flush any residual batches queued in memory before shutdown."""
        while not self.client.accepted_queue.empty():
            try:
                batch = self.client.accepted_queue.get_nowait()
                await self._process_batch_with_retry(batch)
                self.client.accepted_queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception as exc:
                self.last_failed_batch = batch
                self.last_fatal_error = exc
                logger.error(f"Error flushing residual batch during shutdown: {exc}")
                break
        try:
            await self._sync_incidents()
        except Exception:
            pass

    def start(self) -> asyncio.Task[None]:
        """Start worker background task."""
        self._stop_event.clear()
        self._task = asyncio.create_task(self.run())
        return self._task

    async def stop(self, timeout: float = 5.0) -> None:
        """Signal worker stop, wait for in-flight commits, and cleanly flush."""
        self._stop_event.set()
        if self._task is not None and not self._task.done():
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None

    async def retry_failed_batch(self) -> CheckpointRecord:
        """Retry persisting the failed uncommitted batch in-process after storage recovery.
        
        Operator / Application Recovery Sequence:
        1. Operator/Supervisor resolves underlying storage issue (e.g. frees disk space).
        2. Application invokes `await worker.retry_failed_batch()`.
        3. If commit succeeds:
           - Queue item is acknowledged (task_done()).
           - last_failed_batch and last_fatal_error are cleared.
           - client.storage_error_event is cleared, resuming wire polling.
           - Worker ingestion loop is restarted if it had stopped.
        
        Alternative (Restart Recovery):
        If process restarts instead of calling retry_failed_batch(), in-memory state
        is cleared. On restart, the client reads the unadvanced checkpoint from SQLite
        and re-requests missing data from I_resolved + 1 over the wire.
        """
        if self.last_failed_batch is None:
            raise RuntimeError("No failed batch pending recovery.")

        ckpt = await self._process_batch_with_retry(self.last_failed_batch)
        self.client.accepted_queue.task_done()

        self.last_failed_batch = None
        self.last_fatal_error = None

        if hasattr(self.client, "storage_error_event"):
            self.client.storage_error_event.clear()

        # Restart worker loop if it exited
        if self._task is None or self._task.done():
            self.start()

        return ckpt
