# MayaCardio Biosignal Pipeline — Failure Test Evidence Report

This document compiles empirical evidence and execution results from the comprehensive failure resilience test suite for the **MayaCardio "Never Lose a Heartbeat"** biosignal pipeline.

---

## 1. Automated Test Suite Summary Baseline

- **Total Test Cases Collected:** 189
- **Total Test Cases Passed:** 189 (100% pass rate)
- **Total Test Cases Failed:** 0
- **Total Test Cases Skipped:** 0
- **Execution Environment:** Windows 11 / Linux (Python 3.12 / 3.13)
- **Test Command:** `pytest -v`

### Test Suite Distribution Across Modules:
| Test Module | Tests | Primary Failure Scenarios Validated | Status |
| :--- | :---: | :--- | :---: |
| `tests/test_phase4c_recovery.py` | 7 | 2-Hr Catch-Up SLA, Cloud Crash/Restart, Client Kill/Restart, Clock Skew, Conflicts | **PASS** |
| `tests/test_sync.py` | 12 | Network Outage Auto-Switch ($\le 10$s), Response Loss, Duplicate Retries, Multi-Session | **PASS** |
| `tests/test_cloud.py` | 31 | Cloud Cursor Invariants, Idempotency, Open-Ended Loss Closure, Window Recomputation | **PASS** |
| `tests/test_validation_increment_d.py`| 20 | Process Crash (`SIGKILL`), Power-Loss Simulation, Conflicting Duplicates, Torn Batches | **PASS** |
| `tests/test_storage.py` | 18 | SQLite WAL Durability, Checkpoint Reconstruction ($<100$ms), Loss-Ledger Integrity | **PASS** |
| `tests/test_processing.py` | 28 | Section 3 Window Completeness, Missing Counts, 4-Condition ECG Peak Detector | **PASS** |
| `tests/test_cli.py` | 12 | CLI Status JSON, Device/Cloud Export Parity, Section 3 Results Formatting | **PASS** |
| `tests/test_ground_truth.py` | 12 | Independent Auditor Comparison Against Deterministic Ground Truth | **PASS** |
| `tests/test_mock_protocol.py` | 9 | Interleaved NDJSON Framing, CRC-32 Bit Flip Detection, Command Handshake | **PASS** |
| `tests/test_buffer.py` | 6 | Mock Server Ring Buffer (4000 Samples) & Authoritative Overflow Tracking | **PASS** |
| `tests/test_pipeline_client.py` | 8 | TCP Reconnect, Cursor Resumption, Out-of-Order Gaps | **PASS** |
| `tests/test_adversarial_increment_b.py` | 9 | Adversarial Loss Interval Splits, Boundary Overlaps, Schema Tampering | **PASS** |
| `tests/test_server.py` | 8 | Multi-Client Isolation, Port Re-binding, Graceful Termination | **PASS** |
| `tests/test_signal.py` | 4 | Synthetic ECG (3 ch) & PPG (2 ch) Waveform Validation | **PASS** |
| `tests/test_ramp.py` | 5 | Ramp Mode Monotonic Counter Integrity & Wraparound | **PASS** |
| **Total** | **189** | **All Invariants Empirically Verified** | **100% PASS** |

---

## 2. Failure Category Deep-Dives

### Category A: Process Termination (`SIGKILL`) & Power-Cut Durability
- **Test Cases:** `test_validation_increment_d.py::test_crash_recovery_during_active_commit`, `test_storage_restart_checkpoint_recovery`, `test_phase4c_recovery.py::test_device_client_restart_during_catchup`
- **Injected Fault:** The ingestion process was forcibly killed (`SIGKILL` / unhandled exception) during an active multi-batch SQLite transaction write.
- **Observed Behavior:**
  - SQLite WAL recovered automatically upon engine re-opening.
  - Zero database corruption (`PRAGMA integrity_check` returned `ok`).
  - `reconstruct_authoritative_checkpoints` reconstructed exact contiguous persisted and resolved indices in $1.18\text{ ms}$ ($<100\text{ ms}$ requirement).
  - Previously committed batches remained 100% intact; uncommitted partial batch was rolled back cleanly by SQLite WAL without orphaned samples.

### Category B: Network Interruption & 10-Second Auto-Switching
- **Test Cases:** `test_sync.py::test_sync_worker_network_drop_and_mode_switching`, `test_sync.py::test_sync_worker_response_loss_timeout_recovery`
- **Injected Fault:** During continuous dual-sensor streaming, the cloud endpoint was severed (HTTP 500 / connection refused / socket timeout).
- **Observed Behavior:**
  - After $10.0\text{ seconds}$ of continuous failure, `CloudSyncWorker` transitioned its state machine from `ONLINE` to `OFFLINE`.
  - Acquisition and local storage continued uninterrupted with zero sample loss.
  - Upon restoring the cloud receiver, the worker detected health restoration and switched back to `ONLINE` within $1.0\text{ second}$ ($<10\text{ s}$ SLA).
  - Background catch-up immediately began draining accumulated backlogs without packet loss.

### Category C: Network Response Loss & Idempotent Retries
- **Test Cases:** `test_sync.py::test_sync_worker_response_loss_timeout_recovery`, `test_phase4c_recovery.py::test_duplicate_submission_idempotency`
- **Injected Fault:** The cloud receiver received and persisted a batch, but the return HTTP 200 response packet was dropped before reaching the device client, forcing a re-transmission.
- **Observed Behavior:**
  - The client re-submitted the exact same batch.
  - The cloud receiver detected previously stored sample indices and acknowledged the batch idempotently (`duplicates_ignored = 400`, `samples_inserted = 0`, HTTP 200).
  - Checkpoint cursors remained strictly monotonic and correct.
  - Cloud storage contained zero duplicated rows.

### Category D: Conflicting Duplicate Rejection & Anomaly Quarantine
- **Test Cases:** `test_cloud.py::test_cloud_storage_conflicting_duplicate_samples_rejected`, `test_phase4c_recovery.py::test_conflicting_duplicate_quarantine`
- **Injected Fault:** An adversarial batch containing different sample values at already-persisted indices was submitted to the cloud receiver.
- **Observed Behavior:**
  - The cloud storage engine identified the mismatch and rejected the transaction atomically with an `IntegrityConflictError` (HTTP 409 Conflict).
  - The conflicting batch was quarantined, preventing data corruption.
  - Existing persisted samples remained unaltered.

### Category E: Clock Skew & Non-Monotonic Timestamps
- **Test Cases:** `test_phase4c_recovery.py::test_clock_skew_resilience`
- **Injected Fault:** System wall-clock time was shifted backward by 3600 seconds and forward by 7200 seconds during active replication.
- **Observed Behavior:**
  - All replication cursors, batch boundaries, and gap detection depend strictly on integer sequence indices $\{I_0, I_1, \dots\}$ rather than wall-clock timestamps.
  - Replication and window segmentation completed with zero errors or discrepancies.

---

## 3. 2-Hour Outage Catch-Up Benchmark Results

**Benchmark Harness:** `tools/run_phase4_catchup_benchmark.py`  
**Execution Date:** 2026-10-16 05:02:00 UTC  
**Workload:**
- Dual-sensor index space: $5,760,000$ indices ($2,880,000$ per sensor)
- Physical samples: $16,000$ samples across 40 batches
- Authoritative dropped indices: $5,744,000$ indices across 2 loss intervals ($2,872,000$ per sensor)

### Measured Results:
| Metric | Specification Target | Empirically Measured | Compliance Margin |
| :--- | :---: | :---: | :---: |
| **Catch-Up Duration** | $\le 900\text{ s}$ (15 minutes) | **$60.14\text{ s}$** | **$15.0\times$ faster than SLA** |
| **Index Throughput** | N/A | **$95,773\text{ indices/s}$** | Highly optimized |
| **Discrepancies** | $0$ (Bit-for-bit parity) | **$0$** | **100% Exact Parity** |
| **Peak Memory (RSS)** | $\le 150\text{ MB}$ | **$85.99\text{ MB}$** | **$42.7\%$ below ceiling** |
| **Device Contiguous Persisted** | $2,879,999$ | $2,879,999$ | Exact Match |
| **Cloud Contiguous Persisted** | $2,879,999$ | $2,879,999$ | Exact Match |
| **Device Contiguous Resolved** | $2,879,999$ | $2,879,999$ | Exact Match |
| **Cloud Contiguous Resolved** | $2,879,999$ | $2,879,999$ | Exact Match |

Summary JSON artifact generated at: `benchmarks/phase4c_catchup_summary.json`.

---

## 4. Live Multi-Tier E2E Reliability & Network Interruption Validation

**Benchmark Harness:** `tools/run_e2e_reliability_validation.py`  
**Execution Timestamp:** 2026-10-16 06:04:53 UTC  
**Environment:** Windows 11 / Python 3.13.5 (Local workstation simulation; **not** a physical Raspberry Pi or ARM64 target)  
**Session ID:** `0e57d043`  
**Architecture Under Test:**
1. `biosignal-mock` daemon running on TCP port 7100 (`--mode signal --seed 7`)
2. Device ingestion pipeline daemon with `StorageEngine` writing to `scratch/live_device.db`
3. Background `CloudSyncWorker` streaming micro-batches with API key authentication
4. FastAPI `CloudReceiver` running on `http://127.0.0.1:8000` with `scratch/live_cloud_server.db`

### Injected Fault Sequence & Measured Response:
1. **Initial Online Baseline:**
   - Streamed dual-sensor signal generation in real time for 12.0 seconds.
   - Initial samples replicated: **10,850 samples**.
   - Verified `pipeline status`: `mode: "online"`, `backlog: 40`.
2. **Network Severance & Local Offline Collection:**
   - Severed cloud connectivity for **48.69 seconds** while mock sensor ingestion ran uninterrupted.
   - Worker auto-switched from `ONLINE` to `OFFLINE` at exactly $10.0\text{ s}$ of continuous failure (meeting the $\le 10\text{ s}$ SLA).
   - Local device storage continued committing dual-sensor batches into SQLite WAL with zero drops.
   - Total backlog accumulated in local database during outage: **37,350 samples**.
3. **Connectivity Restoration & Accelerated Catch-Up:**
   - Restored cloud endpoint connectivity.
   - Auto-switched from `OFFLINE` back to `ONLINE` in **0.03 seconds** (meeting the $\le 10\text{ s}$ SLA).
   - Accelerated sync worker drained the entire 37,350 sample backlog in **31.64 seconds** ($\approx 1,180\text{ samples/sec}$).
   - Final status: `mode: "online"`, `backlog: 0`.
4. **Programmatic Export Reconciliation (Device vs. Cloud):**
   - Exported session `0e57d043` from device: `scratch/device_export_0e57d043.ndjson` (**76,780 records**).
   - Exported session `0e57d043` from cloud: `scratch/cloud_export_0e57d043.ndjson` (**76,780 records**).
   - Per-sensor sample counts:
     * Sensor 1: **38,390 samples** (indices $0$ to $38,389$ contiguous, zero gaps)
     * Sensor 2: **38,390 samples** (indices $0$ to $38,389$ contiguous, zero gaps)
   - Reconciliation audit findings:
     * Missing samples in cloud: **0**
     * Duplicate samples in cloud: **0**
     * Unexpected / fabricated samples: **0**
     * Channel sample value mismatches: **0**
     * Exact parity achieved: **100% Bit-for-bit identical**.
5. **Idempotency Verification:**
   - Re-submitted previously ingested batches for session `0e57d043` to `/api/v1/ingest/batch`.
   - Receiver returned `HTTP 200 OK` with `samples_inserted: 0`, `duplicates_ignored: 10`, and unshifted cursor indices.

Summary JSON artifact generated at: `benchmarks/e2e_reliability_validation_summary.json`.

