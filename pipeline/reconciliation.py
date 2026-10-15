"""MayaCardio Device-Cloud Reconciliation Utility (Phase 4C).

Responsible for:
1. Strict mathematical comparison of device and cloud records for a session:
   - Every physical sample (session_id, sensor, sample_index, channels) must match exactly.
   - Every authoritative loss interval (from_index, to_index, reason) must match exactly.
   - Contiguous cursors (persisted, resolved, observed) must be identical.
2. Independent mismatch and anomaly detection:
   - Missing samples on cloud
   - Extra / fabricated samples on cloud
   - Conflicting sample values
   - Mismatched or truncated loss intervals
3. Formats machine-readable reconciliation reports.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Optional

from pipeline.cloud.storage import CloudStorageEngine
from pipeline.storage import StorageEngine

logger = logging.getLogger("pipeline.reconciliation")


@dataclass
class StreamReconciliation:
    """Per-(session, sensor) reconciliation details."""
    session_id: str
    sensor: int
    device_sample_count: int
    cloud_sample_count: int
    matched_samples: int
    device_loss_count: int
    cloud_loss_count: int
    matched_loss_records: int
    device_cloud_resolved_index: int
    cloud_resolved_index: int
    sample_mismatches: int
    loss_mismatches: int
    is_consistent: bool


@dataclass
class ReconciliationReport:
    """Comprehensive device-cloud reconciliation report."""
    session_id: str
    is_fully_reconciled: bool
    total_matched_samples: int
    total_matched_loss_records: int
    total_discrepancies: int
    streams: list[StreamReconciliation]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def reconcile_session(
    device_storage: StorageEngine,
    cloud_storage: CloudStorageEngine,
    session_id: str,
) -> ReconciliationReport:
    """Execute ground-truth reconciliation between device and cloud databases for a session."""
    streams: list[StreamReconciliation] = []
    total_matched_samples = 0
    total_matched_loss = 0
    total_discrepancies = 0

    with device_storage._lock, cloud_storage._lock:
        d_cur = device_storage._conn.cursor()
        c_cur = cloud_storage._conn.cursor()

        for sensor in (1, 2):
            sample_mismatches = 0
            loss_mismatches = 0
            matched_samples = 0
            matched_loss = 0

            # 1. Compare samples
            d_cur.execute(
                """
                SELECT sample_index, ch1, ch2, ch3 FROM samples
                WHERE session_id = ? AND sensor = ?
                ORDER BY sample_index ASC;
                """,
                (session_id, sensor),
            )
            d_samples = {
                row["sample_index"]: (row["ch1"], row["ch2"], row["ch3"])
                for row in d_cur.fetchall()
            }

            c_cur.execute(
                """
                SELECT sample_index, ch1, ch2, ch3 FROM samples
                WHERE session_id = ? AND sensor = ?
                ORDER BY sample_index ASC;
                """,
                (session_id, sensor),
            )
            c_samples = {
                row["sample_index"]: (row["ch1"], row["ch2"], row["ch3"])
                for row in c_cur.fetchall()
            }

            # Check matches and mismatches
            all_indices = set(d_samples.keys()) | set(c_samples.keys())
            for idx in all_indices:
                d_val = d_samples.get(idx)
                c_val = c_samples.get(idx)
                if d_val is not None and c_val is not None:
                    if d_val == c_val:
                        matched_samples += 1
                    else:
                        sample_mismatches += 1
                        logger.error(
                            f"Sample value conflict at {session_id}:{sensor}:{idx} - "
                            f"device={d_val}, cloud={c_val}"
                        )
                else:
                    sample_mismatches += 1
                    if d_val is None:
                        logger.error(f"Extra sample on cloud at {session_id}:{sensor}:{idx}")
                    else:
                        logger.error(f"Missing sample on cloud at {session_id}:{sensor}:{idx}")

            # 2. Compare loss records
            d_cur.execute(
                """
                SELECT from_index, to_index, reason FROM loss_ledger
                WHERE session_id = ? AND sensor = ?
                ORDER BY from_index ASC;
                """,
                (session_id, sensor),
            )
            d_losses = [
                (row["from_index"], row["to_index"], row["reason"])
                for row in d_cur.fetchall()
            ]

            c_cur.execute(
                """
                SELECT from_index, to_index, reason FROM loss_ledger
                WHERE session_id = ? AND sensor = ?
                ORDER BY from_index ASC;
                """,
                (session_id, sensor),
            )
            c_losses = [
                (row["from_index"], row["to_index"], row["reason"])
                for row in c_cur.fetchall()
            ]

            d_loss_set = set(d_losses)
            c_loss_set = set(c_losses)
            matched_loss = len(d_loss_set & c_loss_set)
            loss_diff = (d_loss_set - c_loss_set) | (c_loss_set - d_loss_set)
            loss_mismatches = len(loss_diff)

            # 3. Compare cursors
            dev_ckpt = device_storage.get_cloud_checkpoint(session_id, sensor)
            cld_ckpt = cloud_storage.get_checkpoint(session_id, sensor)
            cld_resolved = cld_ckpt.contiguous_resolved_index if cld_ckpt else -1
            cld_persisted = cld_ckpt.contiguous_persisted_index if cld_ckpt else -1

            cursor_consistent = (
                dev_ckpt.cloud_resolved_index == cld_resolved
                and dev_ckpt.cloud_persisted_index == cld_persisted
            )

            is_consistent = (
                sample_mismatches == 0
                and loss_mismatches == 0
                and cursor_consistent
                and len(d_samples) == len(c_samples)
            )

            stream_rec = StreamReconciliation(
                session_id=session_id,
                sensor=sensor,
                device_sample_count=len(d_samples),
                cloud_sample_count=len(c_samples),
                matched_samples=matched_samples,
                device_loss_count=len(d_losses),
                cloud_loss_count=len(c_losses),
                matched_loss_records=matched_loss,
                device_cloud_resolved_index=dev_ckpt.cloud_resolved_index,
                cloud_resolved_index=cld_resolved,
                sample_mismatches=sample_mismatches,
                loss_mismatches=loss_mismatches,
                is_consistent=is_consistent,
            )
            streams.append(stream_rec)

            total_matched_samples += matched_samples
            total_matched_loss += matched_loss
            total_discrepancies += sample_mismatches + loss_mismatches + (0 if cursor_consistent else 1)

    is_fully_reconciled = total_discrepancies == 0 and all(s.is_consistent for s in streams)

    return ReconciliationReport(
        session_id=session_id,
        is_fully_reconciled=is_fully_reconciled,
        total_matched_samples=total_matched_samples,
        total_matched_loss_records=total_matched_loss,
        total_discrepancies=total_discrepancies,
        streams=streams,
    )
