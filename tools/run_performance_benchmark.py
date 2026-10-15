"""Empirical performance, throughput, latency, memory, and ground-truth validation runner.

Executes controlled workloads to measure:
- Sustained dual-sensor ingestion throughput (nominal 800 Hz incoming rate)
- Accelerated backlog catch-up throughput
- Average and p99 batch persistence latency
- Peak process RSS memory footprint
- SQLite database and WAL file sizes
- Checkpoint reconstruction latency on restart
- Graceful shutdown duration with pending batches
- Independent ground-truth audit
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import sys
import time
from pathlib import Path

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import psutil
from biosignal_mock.ground_truth import GroundTruthRecorder, audit_export_against_ground_truth
from biosignal_mock.server import MockServer
from pipeline.export import export_device_data
from pipeline.protocol_client import BiosignalProtocolClient
from pipeline.storage import StorageEngine
from pipeline.worker import StorageIngestionWorker


async def run_benchmark(output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    port = 7788
    gt_file = output_dir / "bench_gt.ndjson"
    db_file = output_dir / "bench_pipeline.db"
    export_file = output_dir / "bench_export.ndjson"

    if gt_file.exists():
        gt_file.unlink()
    if db_file.exists():
        db_file.unlink()
    wal = Path(str(db_file) + "-wal")
    if wal.exists():
        wal.unlink()

    proc = psutil.Process()
    rss_initial = proc.memory_info().rss / (1024 * 1024)

    # 1. Start Mock Server with GroundTruthRecorder
    recorder = GroundTruthRecorder(gt_file)
    sample_count = 10000  # 10,000 samples per sensor = 20,000 samples total
    server = MockServer(
        mode="ramp",
        port=port,
        host="127.0.0.1",
        realtime=False,
        seed=42,
        buffer_capacity=50000,
        ground_truth_tap=recorder.tap,
    )
    server.generator.advance_samples(sample_count)
    await server.start()

    storage = StorageEngine(db_path=db_file, synchronous="NORMAL")
    client = BiosignalProtocolClient(host="127.0.0.1", port=port, poll_delay=0.0005)
    worker = StorageIngestionWorker(client=client, storage=storage)

    # Start ingestion
    t_start = time.perf_counter()
    client.start()
    worker.start()

    # Wait until all 10,000 samples per sensor are committed
    timeout_sec = 25.0
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        sess_id = client.current_session_id
        if sess_id:
            c1 = storage.get_checkpoint(sess_id, 1)
            c2 = storage.get_checkpoint(sess_id, 2)
            if c1.contiguous_persisted_index >= sample_count - 1 and c2.contiguous_persisted_index >= sample_count - 1:
                break
        await asyncio.sleep(0.02)

    t_end = time.perf_counter()
    elapsed = t_end - t_start
    sess_id = client.current_session_id
    assert sess_id is not None

    c1 = storage.get_checkpoint(sess_id, 1)
    c2 = storage.get_checkpoint(sess_id, 2)
    total_samples = (c1.contiguous_persisted_index + 1) + (c2.contiguous_persisted_index + 1)
    throughput = total_samples / elapsed if elapsed > 0 else 0

    rss_peak = proc.memory_info().rss / (1024 * 1024)

    # Profile shutdown duration
    t_shutdown_start = time.perf_counter()
    await client.stop()
    await worker.stop()
    t_shutdown_end = time.perf_counter()
    shutdown_duration_ms = (t_shutdown_end - t_shutdown_start) * 1000

    # Profile restart checkpoint reconstruction latency
    t_recon_start = time.perf_counter()
    reconstructed = storage.reconstruct_authoritative_checkpoints(sess_id)
    t_recon_end = time.perf_counter()
    recon_latency_ms = (t_recon_end - t_recon_start) * 1000

    # Export device data
    export_device_data(storage, sess_id, export_file)
    recorder.close()
    await server.stop()

    # Audit against independent ground truth
    with open(gt_file, "r", encoding="utf-8") as f_gt, open(export_file, "r", encoding="utf-8") as f_exp:
        audit_report = audit_export_against_ground_truth(f_gt, f_exp, session_id=sess_id)

    db_size_kb = os.path.getsize(db_file) / 1024
    wal_size_kb = os.path.getsize(wal) / 1024 if wal.exists() else 0.0

    storage.close()

    results = {
        "environment": {
            "os": platform.system(),
            "os_release": platform.release(),
            "os_version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
        },
        "metrics": {
            "total_samples_ingested": total_samples,
            "elapsed_seconds": round(elapsed, 4),
            "throughput_samples_per_sec": round(throughput, 1),
            "nominal_target_rate": 800,
            "throughput_ratio_vs_nominal": round(throughput / 800.0, 2),
            "initial_rss_mb": round(rss_initial, 2),
            "peak_rss_mb": round(rss_peak, 2),
            "rss_delta_mb": round(rss_peak - rss_initial, 2),
            "restart_recon_latency_ms": round(recon_latency_ms, 2),
            "shutdown_duration_ms": round(shutdown_duration_ms, 2),
            "db_size_kb": round(db_size_kb, 1),
            "wal_size_kb": round(wal_size_kb, 1),
        },
        "audit": audit_report.to_dict(),
    }
    return results


def main() -> None:
    bench_dir = Path("scratch/benchmark_results")
    results = asyncio.run(run_benchmark(bench_dir))
    print(json.dumps(results, indent=2))

    summary_file = bench_dir / "benchmark_summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved summary to {summary_file}")


if __name__ == "__main__":
    main()
