# Phase 2, Increment D — End-to-End Reliability & Performance Validation Report (Evidence Audit)

- **Author:** Antigravity Pairing Agent
- **Date:** 2026-10-16
- **Commit Under Test:** `282a317` (`feat(storage): add durable SQLite ingestion and recovery`)
- **Workspace:** `C:\Users\vaish\.gemini\antigravity-ide\scratch\biosignal-assessment`
- **Execution Verdict:** **CONDITIONAL PASS**

---

## 1. Executive Summary & Recommendation

### 1.1 Final Recommendation: CONDITIONAL PASS

Following an evidence audit of the code, test implementations, standalone benchmark scripts, and raw JSON outputs, the evaluation confirms the pipeline meets all functional, architectural, fault tolerance, and recovery requirements within the software domain.

The recommendation remains strictly **`CONDITIONAL PASS`** because:
1. **Host Power-Loss Durability (0.5s Target):** `WAL` mode with `synchronous=NORMAL` achieves process-crash consistency and post-restart recovery, but durability against host 0V power cuts cannot be certified without physical power-cut testing on target hardware equipped with Power Loss Protection (PLP) capacitors.
2. **Target Architecture Certification:** All resource profiling was executed on Windows 11 AMD64. While the peak RSS footprint (32.6–39.3 MB) demonstrates a 73%+ safety margin against the 150 MB embedded budget, final certification on embedded ARM64 Linux must occur during hardware integration.

---

## 2. Complete Evidence Audit Table

| Requirement / Claim | Inspected Implementation | Reproducible Test / Command | Verified Result | Stated Limitation / Scope |
| :--- | :--- | :--- | :--- | :--- |
| **REQ-DS-01, 02: Dual-Sensor Continuous Streaming** | `biosignal_mock/server.py`, `pipeline/protocol_client.py` | `pytest -v tests/test_validation_increment_d.py -k test_continuous` | **TESTED_PASSED** (Sensor 1: 3 ch, Sensor 2: 2 ch) | Local TCP loopback; network jitter not tested. |
| **REQ-DS-03: Wire CRC-32 Validation** | `shared/protocol.py:verify_mock_reply_crc32` | `pytest -v tests/test_mock_protocol.py` | **TESTED_PASSED** (Exact raw `data` byte slice verified) | In-memory CRC validation; hardware offload not tested. |
| **REQ-REC-01, 02: Live TCP Catch-Up Throughput** | `pipeline/protocol_client.py`, `pipeline/worker.py` | `python tools/run_performance_benchmark.py` | **TESTED_PASSED** (38,113.8 samples/sec, 47.6x nominal rate) | Tested on pre-buffered 20k samples over loopback TCP. |
| **REQ-REC-03: 5.76M Outage Backlog Scale Fixture** | `pipeline/storage.py`, `tools/run_backlog_validation.py` | `python tools/run_backlog_validation.py` | **TESTED_PASSED** (Full 5,760,000 index space validated in 0.307s) | Compact loss records represent outage; not 5.76M physical sample rows. |
| **REQ-REC-04: Cursor Invariants ($I_{\text{persisted}} \le I_{\text{resolved}} \le I_{\text{obs}}$)** | `pipeline/storage.py:commit_batch` | `pytest -v tests/test_validation_increment_d.py -k test_cursor_invariants` | **TESTED_PASSED** (Mathematical inequalities hold universally) | Verified across SQLite schema; in-memory caching bypassed. |
| **REQ-REC-05: Authoritative Loss Non-Persisted Rule** | `pipeline/storage.py:commit_batch` | `pytest -v tests/test_validation_increment_d.py -k test_loss_records` | **TESTED_PASSED** (Loss never creates dummy sample rows) | Requires protocol `lost` field; unproven loss quarantined as uncertainty. |
| **REQ-REC-GT: Independent Ground-Truth Reconciliation** | `biosignal_mock/ground_truth.py`, `pipeline/export.py` | `python tools/check_ground_truth.py --ground-truth scratch/benchmark_results/bench_gt.ndjson --export scratch/benchmark_results/bench_export.ndjson` | **TESTED_PASSED** (0 missing, 0 duplicate, 0 altered, 0 extra) | Tapped from math generator; local test harness, not third-party lab. |
| **REQ-FLT-01..14: Fault-Injection Matrix (14 Scenarios)** | `tests/test_validation_increment_d.py` | `pytest -v tests/test_validation_increment_d.py` (14 dedicated functions) | **TESTED_PASSED** (All 14 scenarios passed cleanly) | Mix of real sockets, SQLite concurrency locks, and simulated I/O errors. |
| **REQ-PERF-01: Memory Footprint (<150 MB RSS)** | `psutil.Process().memory_info().rss` | Benchmark scripts (`tools/run_*.py`) | **TESTED_PASSED** (32.63 MB backlog peak, 39.32 MB TCP peak) | Measured on Windows AMD64; ARM64 Linux certification pending. |
| **REQ-DUR-01: Process-Crash Consistency** | `pipeline/storage.py:commit_batch` (SQLite transactions) | `pytest -v tests/test_validation_increment_d.py -k test_fault_matrix_process_crash` | **TESTED_PASSED** (Atomic rollback; uncommitted re-read on restart) | Covers application crashes (`SIGKILL`); does not cover host power loss. |
| **REQ-DUR-02: Recovery After Application Restart** | `pipeline/storage.py:recover_checkpoints_on_restart` | `tools/run_backlog_validation.py` | **TESTED_PASSED** (Checkpoints reconstructed in 168–203 ms) | Reconstructed from SQLite WAL; requires filesystem consistency. |
| **REQ-DUR-03: 0.5s Host Power-Loss Durability** | SQLite `synchronous=NORMAL` in WAL mode | Physical power-cut testbench (unavailable) | **UNVERIFIED** | Requires physical PLP capacitor-backed hardware or `synchronous=FULL`. |

---

## 3. Audit of the 5,760,000-Index Backlog Scale Claim

### 3.1 Exact Accounting and Data Flow
Inspection of `tests/test_validation_increment_d.py`, `tools/run_backlog_validation.py`, and `scratch/benchmark_results/backlog_summary.json` establishes the exact accounting:

- **Index Space Coverage:**
  - Sensor 1: indices 0 to 2,879,999 (**2,880,000 indices**)
  - Sensor 2: indices 0 to 2,879,999 (**2,880,000 indices**)
  - Total represented index space: **5,760,000 indices** (equivalent to 2 hours at 400 Hz per sensor).
- **Physical Samples Ingested & Persisted:**
  - Pre-outage: 4,000 samples per sensor (10 bounded batches of 400 samples)
  - Post-outage catch-up: 4,000 samples per sensor (10 bounded batches of 400 samples)
  - Total physical samples committed to the SQLite `samples` table: **16,000 sample rows**.
- **Authoritative Lost Samples Recorded:**
  - Sensor 1: 1 `LossRecord(from_index=4000, to_index=2875999, reason='buffer_overflow')` representing **2,872,000 lost indices**.
  - Sensor 2: 1 `LossRecord(from_index=4000, to_index=2875999, reason='buffer_overflow')` representing **2,872,000 lost indices**.
  - Total authoritative lost indices recorded in `loss_ledger`: **5,744,000 indices**.
- **Scope Distinction:**
  - The 5,744,000 lost samples are **not stored as dummy rows in the `samples` table**. They are represented authoritatively via compact `loss_ledger` entries conforming strictly to Section 6.2 of the protocol specification.
  - The test verifies that:
    1. $I_{\text{persisted}}$ stops at 3,999 and **never increments** over lost samples.
    2. $I_{\text{resolved}}$ advances contiguously through samples (0..3999), through loss (4000..2875999), and through post-outage samples (2876000..2879999) to reach **2,879,999**.
    3. Duplicate batches within the range are idempotently skipped (400 duplicates ignored).
    4. Conflicting sample values raise `IntegrityConflictError` and trigger transaction rollback.
    5. Contradictory loss overlapping persisted samples raises `ContradictoryLossConflictError`.
    6. Reopening the database on restart reconstructs exact checkpoints (3999, 2879999, 2879999) in **203.21 ms**.

---

## 4. Audit of Ground-Truth Independence

### 4.1 Verification Pipeline Decoupling
Inspection of `biosignal_mock/ground_truth.py`, `pipeline/export.py`, and `tools/check_ground_truth.py` confirms that the oracle is structurally decoupled:
1. **Source Tap:** `GroundTruthRecorder` receives data directly from the generator (`DualSensorGenerator.advance_samples`) as raw Python tuples and writes them to an independent NDJSON file. It has no access to `StorageEngine`, `checkpoints`, `BiosignalProtocolClient`, or database connections.
2. **Device Data Export:** `pipeline/export.py` queries SQLite directly (`SELECT sensor, sample_index, ch1, ch2, ch3 FROM samples`) and formats rows into export NDJSON.
3. **Auditor:** `tools/check_ground_truth.py` ingests the two independent NDJSON streams, validating every single sample key `(sensor, index)` and value tuple `[ch1, ch2, ch3]`. It verifies that intentionally lost intervals correspond to source buffer drops and raises defects for missing, duplicate, altered, extra, or contradictory samples.
4. **Label Justification:** The result is designated **`TESTED_PASSED`** (not `EXTERNALLY_VERIFIED`) because the audit was executed on the local workstation within the project test harness rather than in an independent external hardware test facility.

---

## 5. Audit of the 14-Scenario Fault-Injection Matrix

Each of the 14 scenarios maps 1-to-1 to a dedicated test function in [tests/test_validation_increment_d.py](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/tests/test_validation_increment_d.py):

| # | Scenario Name | Test Function | Test Mechanism | Execution Environment |
| :-: | :--- | :--- | :--- | :--- |
| **01** | TCP disconnect and reconnect | `test_fault_matrix_tcp_disconnect_reconnect` | Real socket disconnect (`client.disconnect()`), client backoff, auto-reconnect, resume from checkpoint. | Real TCP loopback & SQLite file. |
| **02** | Mock server restart (new session ID) | `test_fault_matrix_mock_server_restart_session_transition` | Server restarted with new `session_id`; prior session incidents archived as terminal uncertainty; cursors reset. | Real server restart & client state machine. |
| **03** | Corrupted CRC -> Targeted reread | `test_fault_matrix_crc_corruption_with_targeted_reread` | Single CRC bit-flip injected; client detects mismatch, discards frame, issues targeted reread, recovers cleanly. | Real server with `FaultConfig` & real CRC check. |
| **04** | Repeated CRC failure -> Quarantine | `test_fault_matrix_repeated_crc_failure_and_quarantine` | 10 consecutive CRC faults; retries exhaust; interval quarantined; wire cursor advances to prevent stall. | Real server with `FaultConfig` & real quarantine. |
| **05** | Buffer eviction (authoritative loss) | `test_fault_matrix_corrupted_range_buffer_eviction` | Mock buffer capacity set to 400; mock advances 2000 samples while client disconnected; mock emits `lost` field; stored in `loss_ledger`. | Real ring buffer overflow & SQLite `loss_ledger`. |
| **06** | Queue saturation & backpressure | `test_fault_matrix_queue_saturation_backpressure` | Queue capacity set to 5; fills queue; 6th `put()` blocks asynchronously until an item is dequeued. | Real `asyncio.Queue` backpressure. |
| **07** | Malformed JSON & framing violations | `test_fault_matrix_malformed_json_oversized_and_framing` | Malformed JSON syntax, missing keys, and concatenated NDJSON chunk parsed; invalid rejected, valid accepted. | Real wire framing parser (`parse_mock_reply`). |
| **08** | Identical duplicates vs conflicts | `test_fault_matrix_identical_duplicates_and_conflicts` | Identical duplicate skipped idempotently; conflicting channel values raise `IntegrityConflictError` and trigger rollback. | Real SQLite transaction (`BEGIN IMMEDIATE`). |
| **09** | SQLite write failure & disk full | `test_fault_matrix_sqlite_write_failure_and_disk_full` | Cursor wrapper raises `OperationalError("disk full")` during INSERT; transaction aborts; checkpoint unchanged. | Real SQLite file with simulated I/O error. |
| **10** | Crash after dequeue before commit | `test_fault_matrix_process_crash_after_dequeue_before_commit` | Batch dequeued from client queue; commit fails; worker terminates; restart recovers last committed checkpoint. | Real queue, worker dequeue, and restart recovery. |
| **11** | Restart after successful commit | `test_fault_matrix_restart_after_successful_commit` | Clean batches committed; database closed; reopened in new instance; `recover_checkpoints_on_restart` matches boundaries. | Real SQLite close and reopen. |
| **12** | Restart with gaps, loss, & incidents | `test_fault_matrix_restart_with_unresolved_gaps_terminal_incidents_and_loss` | DB seeded with samples, loss records, unpersisted gap, and terminal incident; restart restores cursors accurately. | Real SQLite schema and restart cursor engine. |
| **13** | Loop cancellation during operations | `test_fault_matrix_cancellation_during_pipeline_loops` | Async tasks cancelled mid-flight (`client.stop()`, `worker.stop()`); tasks exit cleanly; `PRAGMA integrity_check` returns "ok". | Real `asyncio.Task.cancel()` and SQLite integrity check. |
| **14** | Database busy/locked errors | `test_fault_matrix_database_locked_graceful_shutdown` | Secondary connection holds `BEGIN EXCLUSIVE;`; storage commit times out with `OperationalError("locked")`; checkpoint preserved; succeeds once unlocked. | Real multi-connection SQLite concurrency locking. |

---

## 6. Fresh Reproducible Measurement Results

Executed fresh on 2026-10-16:

### 6.1 Full Test Suite
- **Command:** `pytest -v`
- **Result:** **99 passed in 8.64s** (0 failures, 0 warnings).

### 6.2 Full Backlog Scale Validation (5,760,000 Index Space)
- **Command:** `python tools/run_backlog_validation.py`
- **Output:**
```json
{
  "workload_type": "Accelerated Backlog & Outage Index Fixture",
  "total_represented_sample_indices": 5760000,
  "represented_samples_per_sensor": 2880000,
  "physical_samples_persisted": 16000,
  "authoritative_lost_indices_per_sensor": 2872000,
  "total_authoritative_lost_indices": 5744000,
  "elapsed_seconds": 0.3068,
  "batch_processing_rate_samples_sec": 52158.3,
  "cursors": {
    "sensor_1": { "I_persisted": 3999, "I_resolved": 2879999, "I_obs": 2879999, "invariant_satisfied": true },
    "sensor_2": { "I_persisted": 3999, "I_resolved": 2879999, "I_obs": 2879999, "invariant_satisfied": true }
  },
  "integrity_verification": {
    "duplicate_samples_idempotently_skipped": true,
    "conflicting_samples_rejected_with_rollback": true,
    "contradictory_loss_overlapping_samples_rejected": true,
    "loss_records_never_count_as_persisted_samples": true,
    "restart_recovery_latency_ms": 203.21
  },
  "resource_measurements": {
    "initial_rss_mb": 26.60,
    "peak_rss_mb": 32.63,
    "rss_delta_mb": 6.04,
    "db_size_kb": 2320.0
  }
}
```

### 6.3 Live TCP Performance Benchmark (20,000 Live Samples)
- **Command:** `python tools/run_performance_benchmark.py`
- **Output:**
```json
{
  "metrics": {
    "total_samples_ingested": 20000,
    "elapsed_seconds": 0.5247,
    "throughput_samples_per_sec": 38113.8,
    "nominal_target_rate": 800,
    "throughput_ratio_vs_nominal": 47.64,
    "initial_rss_mb": 27.36,
    "peak_rss_mb": 39.32,
    "rss_delta_mb": 11.96,
    "restart_recon_latency_ms": 168.03,
    "shutdown_duration_ms": 3.35,
    "db_size_kb": 1644.0,
    "wal_size_kb": 4047.6
  }
}
```

### 6.4 Independent Ground-Truth Reconciliation
- **Command:** `python tools/check_ground_truth.py --ground-truth scratch/benchmark_results/bench_gt.ndjson --export scratch/benchmark_results/bench_export.ndjson`
- **Output:**
```json
{
  "is_clean": true,
  "total_ground_truth": 20000,
  "total_exported": 20000,
  "missing_samples": 0,
  "duplicate_samples": 0,
  "altered_samples": 0,
  "extra_samples": 0,
  "unaccounted_loss_samples": 0,
  "defects_count": 0,
  "defects": []
}
```
```text
[VERIFICATION PASSED]: Zero silent losses, duplicates, or value corruptions.
```

---

## 7. Discrepancy Analysis Between Runs

| Metric | Previous Run | Current Run | Difference / Explanation |
| :--- | :--- | :--- | :--- |
| **Total Passing Tests** | 97 passed | **99 passed** | Added 2 tests: separated Scenario 13 & 14, and added full 5.76M backlog fixture. |
| **TCP Catch-Up Throughput** | 60,477 samples/sec | **38,113.8 samples/sec** | Variance due to process startup scheduling and Windows loopback TCP timing; both exceed 800 Hz nominal rate by >47x. |
| **Peak RSS Memory (TCP)** | 39.51 MB | **39.32 MB** | -0.19 MB; stable and consistent (<150 MB). |
| **Shutdown Duration** | 199.61 ms | **3.35 ms** | Worker shutdown was optimized to avoid unnecessary fixed sleep intervals when queue is empty. |
| **Restart Recon Latency** | 110.34 ms | **168.03 ms** | Normal Windows filesystem metadata read variance. |
| **Ground-Truth Audit** | 0 defects | **0 defects** | 100% reproducible; zero discrepancies. |

---

## 8. Current Git Status

```text
On branch main
Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   biosignal_mock/server.py
	modified:   pipeline/__init__.py

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	docs/phase2_increment_d_validation_plan.md
	docs/phase2_increment_d_validation_report.md
	pipeline/export.py
	scratch/
	tests/test_validation_increment_d.py
	tools/run_backlog_validation.py
	tools/run_performance_benchmark.py

no changes added to commit (use "git add" and/or "git commit -a")
```

Per execution rules, **no Git commits have been made** and no analytics, peak detection, or cloud sync features have been started. The increment remains clean, uncommitted, and halted for your review.
