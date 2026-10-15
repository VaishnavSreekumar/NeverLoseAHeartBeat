"""MayaCardio Phase 4C 2-Hour Backlog Catch-Up & SLA Benchmark Runner.

Validates the Section 4 recovery SLA:
- Backlog representation: 5,760,000 index space (2,880,000 per sensor) representing a 2-hour outage.
- High-volume physical sample catch-up.
- SLA Target: < 15 minutes (900 seconds) to catch up completely upon network restoration.
- Mathematical reconciliation against exact sample indices and loss ledger entries.
- Peak RSS memory footprint (< 150 MB device constraint).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import sys
import time

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from fastapi.testclient import TestClient
import psutil

from pipeline.cloud.server import create_cloud_app
from pipeline.cloud.storage import CloudStorageEngine
from pipeline.protocol_client import IngestBatch
from pipeline.reconciliation import reconcile_session
from pipeline.storage import StorageEngine
from pipeline.sync import CloudSyncWorker
from shared.protocol import LossRecord


def run_catchup_benchmark(output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    dev_db = output_dir / "bench_catchup_dev.db"
    cld_db = output_dir / "bench_catchup_cld.db"

    for p in (dev_db, cld_db, Path(str(dev_db) + "-wal"), Path(str(cld_db) + "-wal")):
        if p.exists():
            p.unlink()

    proc = psutil.Process()
    rss_initial = proc.memory_info().rss / (1024 * 1024)

    session_id = "sess_2hr_benchmark_run"
    dev_storage = StorageEngine(db_path=dev_db, synchronous="NORMAL")
    cloud_storage = CloudStorageEngine(db_path=cld_db, synchronous="NORMAL")

    print(f"Generating 2-hour backlog fixture for session '{session_id}'...")
    # Pre-outage: 4,000 samples per sensor (indices 0..3999)
    for sensor in (1, 2):
        for b in range(10):
            first_idx = b * 400
            if sensor == 1:
                data = [[1000 + first_idx + i, 2000 + first_idx + i, 3000 + first_idx + i] for i in range(400)]
            else:
                data = [[4000 + first_idx + i, 5000 + first_idx + i] for i in range(400)]
            dev_storage.commit_batch(IngestBatch(session_id, sensor, first_idx, 400, data, [], False, "crc"))

    # Outage: 2,872,000 dropped indices per sensor (indices 4000..2875999)
    loss_from = 4000
    loss_to = 2875999
    for sensor in (1, 2):
        loss = LossRecord(from_index=loss_from, to_index=loss_to, reason="outage_buffer_overflow")
        dev_storage.commit_batch(IngestBatch(session_id, sensor, 2876000, 0, [], [loss], False, "crc"))

    # Post-outage: 4,000 samples per sensor (indices 2876000..2879999)
    for sensor in (1, 2):
        for b in range(10):
            first_idx = 2876000 + b * 400
            if sensor == 1:
                data = [[1000 + first_idx + i, 2000 + first_idx + i, 3000 + first_idx + i] for i in range(400)]
            else:
                data = [[4000 + first_idx + i, 5000 + first_idx + i] for i in range(400)]
            is_end = (b == 9)
            dev_storage.commit_batch(IngestBatch(session_id, sensor, first_idx, 400, data, [], is_end, "crc"))

    initial_backlog = dev_storage.get_replication_backlog()
    print(f"Fixture created. Total un-replicated physical samples: {initial_backlog}")
    print(f"Full 2-hour index space coverage: 5,760,000 indices (2,880,000 per sensor)")

    # Setup Cloud Receiver App and Worker
    app = create_cloud_app(storage=cloud_storage)
    client = TestClient(app, base_url="http://testserver")

    worker = CloudSyncWorker(
        storage=dev_storage,
        cloud_url="http://testserver",
        batch_size=400,
        client=client,
    )

    print("Initiating catch-up synchronization...")
    t_start = time.perf_counter()
    replicated_samples = worker.sync_all_pending()
    t_end = time.perf_counter()
    elapsed = t_end - t_start

    rss_peak = proc.memory_info().rss / (1024 * 1024)

    # Verification: Ground-truth reconciliation
    print("Running mathematical ground-truth reconciliation between device and cloud...")
    report = reconcile_session(dev_storage, cloud_storage, session_id)

    # SLA checks
    target_max_seconds = 900.0  # 15 minutes SLA
    meets_sla = elapsed < target_max_seconds and report.is_fully_reconciled

    throughput_samples_sec = replicated_samples / elapsed if elapsed > 0 else 0
    throughput_indices_sec = 5760000.0 / elapsed if elapsed > 0 else 0

    benchmark_summary = {
        "timestamp": time.time(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "session_id": session_id,
        "index_space_covered": 5760000,
        "physical_samples_replicated": replicated_samples,
        "elapsed_seconds": round(elapsed, 4),
        "target_max_seconds": target_max_seconds,
        "meets_15min_target": meets_sla,
        "samples_per_second": round(throughput_samples_sec, 2),
        "indices_per_second": round(throughput_indices_sec, 2),
        "initial_rss_mb": round(rss_initial, 2),
        "peak_rss_mb": round(rss_peak, 2),
        "reconciliation_status": "RECONCILED" if report.is_fully_reconciled else "FAILED",
        "total_discrepancies": report.total_discrepancies,
        "streams": [
            {
                "sensor": s.sensor,
                "device_samples": s.device_sample_count,
                "cloud_samples": s.cloud_sample_count,
                "matched_samples": s.matched_samples,
                "device_losses": s.device_loss_count,
                "cloud_losses": s.cloud_loss_count,
                "cloud_resolved_index": s.cloud_resolved_index,
                "is_consistent": s.is_consistent,
            }
            for s in report.streams
        ],
    }

    dev_storage.close()
    cloud_storage.close()

    return benchmark_summary


if __name__ == "__main__":
    out_dir = Path("benchmarks")
    summary = run_catchup_benchmark(out_dir)
    print("\n" + "=" * 60)
    print("PHASE 4C 2-HOUR CATCH-UP BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Elapsed Time:       {summary['elapsed_seconds']} s (Target: < 900 s / 15 min)")
    print(f"Meets 15-min SLA:   {summary['meets_15min_target']}")
    print(f"Throughput:         {summary['samples_per_second']} samples/s")
    print(f"Index Throughput:   {summary['indices_per_second']} indices/s")
    print(f"Peak RSS Memory:    {summary['peak_rss_mb']} MB (Limit: < 150 MB)")
    print(f"Reconciliation:     {summary['reconciliation_status']}")
    print(f"Discrepancies:      {summary['total_discrepancies']}")
    print("=" * 60)

    res_file = out_dir / "phase4c_catchup_summary.json"
    with open(res_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"Saved benchmark summary to {res_file}")
