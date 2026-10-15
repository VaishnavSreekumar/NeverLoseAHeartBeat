"""MayaCardio Cloud Synchronization Worker & Mode State Machine (Phase 4B).

Responsible for:
1. Continuous background synchronization of persisted device biosignals to the cloud receiver.
2. Bounded micro-batching with strict channel preservation and authoritative loss inclusion.
3. Durable replication checkpoint tracking: local `cloud_checkpoints` updated strictly upon HTTP 200.
4. Idempotent delivery: network drops and duplicate re-transmissions cause zero data corruption or duplication.
5. Online/offline state machine with automatic transitions:
   - ONLINE -> OFFLINE if cloud receiver communication fails continuously for >10 seconds.
   - OFFLINE -> ONLINE within 10 seconds of cloud receiver restoration.
6. Thread-safe background execution and synchronous drain helpers (`sync_all_pending()`).
7. Real-time telemetry tracking: replicated samples, loss records, conflicts, transitions, and latency.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from pipeline.protocol_client import IngestBatch
from pipeline.storage import CloudReplicationCheckpoint, StorageEngine
from shared.protocol import LossRecord

logger = logging.getLogger("pipeline.sync")


@dataclass
class SyncTelemetry:
    """Operational telemetry for cloud synchronization worker."""
    batches_sent: int = 0
    samples_replicated: int = 0
    duplicates_reported: int = 0
    loss_records_replicated: int = 0
    conflicts_encountered: int = 0
    network_errors: int = 0
    transition_count: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    last_sync_time: float = 0.0
    last_successful_contact: float = 0.0


class CloudSyncWorker:
    """Background worker managing cloud ingestion synchronization and mode state transitions."""

    def __init__(
        self,
        storage: StorageEngine,
        cloud_url: str = "http://localhost:8000",
        batch_size: int = 400,
        poll_interval: float = 0.05,
        idle_interval: float = 0.5,
        health_interval: float = 1.0,
        http_timeout: float = 3.0,
        offline_threshold_s: float = 10.0,
        client: Optional[httpx.Client] = None,
    ) -> None:
        self.storage = storage
        self.cloud_url = cloud_url.rstrip("/")
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.idle_interval = idle_interval
        self.health_interval = health_interval
        self.http_timeout = http_timeout
        self.offline_threshold_s = offline_threshold_s

        self._client = client or httpx.Client(timeout=self.http_timeout)
        self._owns_client = client is None

        self._lock = threading.Lock()
        self._running = False
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        # Mode State Machine: starts online by default, probe confirms
        self._mode: str = "online"
        self._consecutive_failure_start: Optional[float] = None
        self._last_health_check_time: float = 0.0

        self.telemetry = SyncTelemetry()

    @property
    def mode(self) -> str:
        """Current operational mode: 'online' or 'offline'."""
        with self._lock:
            return self._mode

    @property
    def backlog(self) -> int:
        """Current local un-replicated sample backlog."""
        return self.storage.get_replication_backlog()

    @property
    def is_running(self) -> bool:
        """Check if background worker thread is active."""
        return self._running

    def _set_mode(self, new_mode: str, reason: str = "") -> None:
        """Update operational mode and record transition."""
        if self._mode != new_mode:
            prev = self._mode
            self._mode = new_mode
            self.telemetry.transition_count += 1
            logger.info(f"Mode transitioned: {prev} -> {new_mode}. Reason: {reason}")

    def check_cloud_health(self) -> bool:
        """Probe cloud receiver /api/v1/status to update health and state machine."""
        now = time.time()
        self._last_health_check_time = now
        try:
            resp = self._client.get(f"{self.cloud_url}/api/v1/status")
            if resp.status_code == 200:
                with self._lock:
                    self._consecutive_failure_start = None
                    self.telemetry.last_successful_contact = now
                    if self._mode != "online":
                        self._set_mode("online", "Cloud receiver health probe responded HTTP 200")
                return True
            else:
                self._record_network_failure(f"Cloud health probe returned HTTP {resp.status_code}")
                return False
        except Exception as exc:
            self._record_network_failure(f"Cloud health probe connection failed: {exc}")
            return False

    def _record_network_failure(self, reason: str) -> None:
        """Record network error and evaluate threshold for offline transition."""
        now = time.time()
        with self._lock:
            self.telemetry.network_errors += 1
            if self._consecutive_failure_start is None:
                self._consecutive_failure_start = now

            duration = now - self._consecutive_failure_start
            if duration >= self.offline_threshold_s and self._mode == "online":
                self._set_mode(
                    "offline",
                    f"Continuous network failures for {duration:.2f}s (threshold {self.offline_threshold_s}s): {reason}"
                )

    def _record_sync_success(self) -> None:
        """Record successful communication and advance online state."""
        now = time.time()
        with self._lock:
            self._consecutive_failure_start = None
            self.telemetry.last_successful_contact = now
            self.telemetry.last_sync_time = now
            if self._mode != "online":
                self._set_mode("online", "Successful data synchronization with cloud receiver")

    def sync_batch(self, batch: IngestBatch) -> Optional[dict[str, Any]]:
        """Send a single IngestBatch to the cloud receiver and update local cloud checkpoints.

        Guarantees:
        - Atomically advances local cloud_checkpoints ONLY on verified HTTP 200.
        - Idempotent against network drops: duplicate submissions are safely acknowledged.
        - Rejection of conflicting records raises clear warnings without inventing data.
        """
        payload = {
            "session_id": batch.session_id,
            "sensor": batch.sensor,
            "first_index": batch.first_index,
            "count": len(batch.data),
            "data": batch.data,
            "lost": [
                {
                    "from_index": lr.from_index,
                    "to_index": lr.to_index,
                    "reason": lr.reason,
                }
                for lr in batch.lost
            ],
            "session_end": batch.session_end,
        }

        try:
            resp = self._client.post(
                f"{self.cloud_url}/api/v1/ingest/batch",
                json=payload,
            )
        except Exception as exc:
            self._record_network_failure(f"HTTP POST failed for batch {batch.sensor}@{batch.first_index}: {exc}")
            return None

        if resp.status_code == 200:
            data = resp.json()
            # Atomically update local durable cloud checkpoint
            self.storage.update_cloud_checkpoint(
                session_id=batch.session_id,
                sensor=batch.sensor,
                cloud_persisted_index=data["contiguous_persisted_index"],
                cloud_resolved_index=data["contiguous_resolved_index"],
                highest_observed_index=data["highest_observed_index"],
            )

            # Update telemetry
            req_content = getattr(resp.request, "content", b"") or b""
            resp_content = getattr(resp, "content", b"") or b""
            with self._lock:
                self.telemetry.batches_sent += 1
                self.telemetry.samples_replicated += data.get("samples_inserted", 0)
                self.telemetry.duplicates_reported += data.get("duplicates_ignored", 0)
                self.telemetry.bytes_sent += len(req_content)
                self.telemetry.bytes_received += len(resp_content)
                if batch.lost:
                    self.telemetry.loss_records_replicated += len(batch.lost)

            self._record_sync_success()
            return data

        elif resp.status_code == 409:
            with self._lock:
                self.telemetry.conflicts_encountered += 1
            logger.error(
                f"Cloud receiver reported HTTP 409 Conflict for batch {batch.sensor}@{batch.first_index}: {resp.text}. "
                "Quarantining batch advancement to prevent data corruption."
            )
            return None
        else:
            self._record_network_failure(f"Cloud receiver returned HTTP {resp.status_code}: {resp.text}")
            return None

    def sync_stream(self, session_id: str, sensor: int) -> int:
        """Attempt to synchronize the next pending micro-batch for a specific stream.

        Returns:
            Number of samples successfully replicated in this step (0 if caught up or error).
        """
        batch = self.storage.get_unreplicated_batch(
            session_id=session_id,
            sensor=sensor,
            max_samples=self.batch_size,
        )
        if batch is None:
            return 0

        res = self.sync_batch(batch)
        if res is not None:
            return res.get("samples_inserted", 0)
        return 0

    def sync_once(self) -> int:
        """Execute one replication pass across all sessions and sensors.

        Returns:
            Total samples replicated across all streams in this pass.
        """
        sessions = self.storage.get_active_sessions()
        total_samples = 0
        for session_id in sessions:
            for sensor in (1, 2):
                total_samples += self.sync_stream(session_id, sensor)
        return total_samples

    def sync_all_pending(self, max_iterations: int = 100000) -> int:
        """Synchronously drain all pending un-replicated records across all sessions.

        Useful for accelerated catch-up testing and verification benchmarks.

        Returns:
            Total samples replicated.
        """
        total_replicated = 0
        iterations = 0
        while iterations < max_iterations:
            batches_before = self.telemetry.batches_sent
            replicated = self.sync_once()
            total_replicated += replicated
            iterations += 1

            # Check if any unreplicated records remain across all active streams
            active_sessions = self.storage.get_active_sessions()
            has_pending = False
            for s_id in active_sessions:
                for sens in (1, 2):
                    if self.storage.get_unreplicated_batch(s_id, sens, max_samples=1) is not None:
                        has_pending = True
                        break
                if has_pending:
                    break

            if not has_pending:
                break

            # If no samples replicated and no batches successfully sent in this loop, network is down or stalled
            if replicated == 0 and self.telemetry.batches_sent == batches_before:
                break

        return total_replicated

    def _sync_loop(self) -> None:
        """Background thread loop managing periodic sync, health probes, and state transitions."""
        logger.info(f"CloudSyncWorker started. Target cloud URL: {self.cloud_url}")
        while not self._stop_event.is_set():
            now = time.time()

            # If offline, perform periodic health checks to detect network recovery
            if self._mode == "offline":
                if now - self._last_health_check_time >= self.health_interval:
                    self.check_cloud_health()

                # If still offline, sleep and retry health probe
                if self._mode == "offline":
                    self._stop_event.wait(self.health_interval)
                    continue

            # In online mode, attempt sync
            try:
                samples_synced = self.sync_once()
                if samples_synced > 0:
                    # More data may be waiting; brief poll interval
                    self._stop_event.wait(self.poll_interval)
                else:
                    # Idle: wait idle interval or until interrupted
                    self._stop_event.wait(self.idle_interval)
            except Exception as exc:
                self._record_network_failure(f"Unhandled sync loop exception: {exc}")
                self._stop_event.wait(self.idle_interval)

        logger.info("CloudSyncWorker loop exited.")

    def start(self) -> None:
        """Start the background sync worker thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._sync_loop,
                name="CloudSyncWorkerThread",
                daemon=True,
            )
            self._worker_thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully stop background sync worker thread."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._worker_thread is not None:
            self._worker_thread.join(timeout=timeout)
            self._worker_thread = None

        if self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass

    def __enter__(self) -> "CloudSyncWorker":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()
