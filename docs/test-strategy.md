# Comprehensive Verification & Test Strategy

**Project:** MayaCardio "Never Lose a Heartbeat" Biosignal Pipeline  
**Document Version:** 1.0.0  
**Phase:** Phase 0 — Engineering Foundation  
**Target Deadline:** October 16, 2026, 2:00 PM IST  

---

## 1. Verification Principles & Strategy Overview

In a reliability-critical medical pipeline, automated testing is the only defensible proof of correctness. Every requirement in the Traceability Matrix (`docs/requirements-traceability.md`) maps directly to an automated verification test.

### Testing Hierarchy
1. **Tier 1: Unit & Protocol Tests (Platform-Independent):** Fast, in-memory tests covering NDJSON protocol framing, CRC-32 validation, ramp/signal math, and window statistics.
2. **Tier 2: Component Integration Tests (Platform-Independent):** Tests covering SQLite transactions, interleaved sensor polling, buffer queueing, and mode transitions.
3. **Tier 3: Network & Cloud End-to-End Tests (Platform-Independent):** Tests streaming, idempotent ingestion, cloud processing SLA, and backlog catch-up.
4. **Tier 4: OS-Level Fault & Platform-Specific Tests (Linux / ARM64):** Tests simulating SIGKILL crashes, power-loss resets, `iptables` network cuts, loopback disk-full conditions, and cgroups resource limits.

---

## 2. Platform Compatibility Matrix

| Test Suite / Scope | Windows (Local Host) | Linux / WSL2 | ARM64 Linux (Target) |
| :--- | :---: | :---: | :---: |
| **Protocol framing & CRC-32 checks** | Compatible | Compatible | Compatible |
| **Ramp mode mathematical generation** | Compatible | Compatible | Compatible |
| **Signal mode ECG & PPG generation** | Compatible | Compatible | Compatible |
| **Window stats & Section 3 formulas** | Compatible | Compatible | Compatible |
| **Heart rate 4-rule peak detector** | Compatible | Compatible | Compatible |
| **SQLite WAL transaction batching** | Compatible | Compatible | Compatible |
| **Mock buffer overflow & lost accounting**| Compatible | Compatible | Compatible |
| **Corrupt CRC re-read & duplicates** | Compatible | Compatible | Compatible |
| **Online/offline analytics parity** | Compatible | Compatible | Compatible |
| **CLI command format validation** | Compatible | Compatible | Compatible |
| **Ground-truth verification tool** | Compatible | Compatible | Compatible |
| **Process kill recovery (`SIGKILL`)** | Substituted (taskkill) | Native (`kill -9`) | Native (`kill -9`) |
| **Sudden power-cut loss bound** | Emulated (unbuffered kill)| VM / QEMU reset | Hardware cut / QEMU |
| **Network cut & restore (<10s switch)**| Socket disconnect emu | Native (`iptables`) | Native (`iptables`) |
| **Disk-full graceful handling** | Emulated quota | Loopback mount (`dd`) | Loopback mount (`dd`) |
| **System clock tamper invariance** | Mock time monkeypatch | Native (`date -s`) | Native (`date -s`) |
| **Resource benchmarks (4 CPU, 2 GB)** | Host profiling | cgroups v2 check | Raspberry Pi 4 native |

---

## 3. Requirement-to-Test Mapping Matrix

| Test ID | Test Name | Target Requirements | Verification Method | Acceptance Condition |
| :--- | :--- | :--- | :--- | :--- |
| **TST-01** | `test_protocol_framing_and_crc` | REQ-A-07, REQ-A-09, REQ-A-10 | Unit test with valid and corrupted NDJSON frames | CRC matches `zlib.crc32(raw_bytes)`; malformed frames rejected. |
| **TST-02** | `test_ramp_mode_deterministic` | REQ-A-05, REQ-A-06 | Generate $10^5$ samples with seed 7; assert formulas | Bit-for-bit match with $(3i+c) \bmod 8388608$ and $(2i+c) \bmod 8388608$. |
| **TST-03** | `test_signal_mode_synthetics` | REQ-A-03, REQ-A-04 | 10-minute signal generation run; detect peaks | Channel 2 HR matches 60, 90, 72 bpm cycle; Ch1=0.6*Ch2; Ch3=1.3*Ch2. |
| **TST-04** | `test_interleaved_sensor_polling_under_faults` | REQ-A-01, REQ-B-01, REQ-B-02 | Inject 5s sensor stall, 10% corrupt CRC, duplicate frames, and queue saturation on single TCP socket | Client alternates queries without deadlock; bounded queue enforces backpressure without RAM growth; neither sensor's buffer exceeds capacity; zero unforced overflow. |
| **TST-05** | `test_buffer_overflow_loss_logging` | REQ-A-11, REQ-B-08, REQ-H-05 | Pause client read for 12 seconds; resume read | Mock returns `lost` range; loss recorded in `loss_ledger`; $I_{\text{resolved}}$ bridges confirmed gap and advances past lost indices; zero silent loss. Strict loss-merging rules verified (never merge across distinct reasons, never merge with persisted samples). |
| **TST-06** | `test_crc_corruption_and_reread` | REQ-B-04, REQ-A-12 | Inject corrupted CRC responses; test 1–3 retries and exceeding retry bound | Pipeline rejects corrupt frames, requests `from_index` targeting $I_{\text{resolved}} + 1$; on exceeding 3 retries, logs quarantined entry in `unresolved_crc_incidents` without advancing $I_{\text{resolved}}$ or writing to `loss_ledger`; verifies transition to confirmed loss occurs ONLY upon mock buffer eviction or session restart; zero corrupt values enter SQLite. |
| **TST-06B** | `test_crc_failure_forward_progress_and_in_process_recovery` | REQ-B-04, REQ-B-06 | Inject CRC corruption on batch [1000, 1099]; fail 3 retries; ingest subsequent batch [1100, 1199]; re-poll [1000, 1099] via in-process sweeper and recover | Intermediate state: $I_{\text{obs}}=1199$, $I_{\text{persisted}}=999$, $I_{\text{resolved}}=999$, 0 rows in `loss_ledger`. After recovery: $I_{\text{persisted}}=1199$, $I_{\text{resolved}}=1199$, $I_{\text{obs}}=1199$; zero false loss recorded; contiguous cursors advance seamlessly; window analytics unbroken [0, 1199]. |
| **TST-07** | `test_duplicate_response_rejection_and_integrity` | REQ-B-05, REQ-E-03 | Inject identical duplicate replies, conflicting duplicates (same index, different values), and overlapping loss-sample records | Identical duplicates safely ignored with incremented telemetry; conflicting sample duplicates raise detectable `IntegrityConflictError`; loss range overlapping persisted sample raises `ContradictoryLossConflictError`. |
| **TST-08** | `test_session_restart_unknown_end` | REQ-E-04, REQ-A-12 | Mock restarts during run; new session ID generated | Prior session checkpoints and samples preserved untouched; session change establishes transition, not measured loss; open-ended loss recorded with `to_index: null` and reason `session_restart`, preserving terminal uncertainty; new session starts cleanly at index 0. |
| **TST-09** | `test_sqlite_batch_durability_and_power_loss` | REQ-B-03, REQ-B-06, REQ-B-07 | Abrupt unbuffered crash (`SIGKILL`) / power cut simulation during active batch commit | `WAL` + `synchronous=NORMAL` guarantees 100% process-crash survival and clean WAL rollback; power-loss bound tested as unverified design target (not promised guarantee); empirical loss gap $(i_{\text{cut}} - i_{\text{committed}})/400$ measured; gap logged in `loss_ledger`. |
| **TST-10** | `test_section3_window_metrics` | REQ-F-01..05, REQ-F-14 | Synthetic 1s windows with known statistics | Exact match for mean, min, max, magnitude, missing counts; 1-decimal round. |
| **TST-11** | `test_heart_rate_4_rules` | REQ-F-06..13 | Edge cases: $w<4$, missing samples, refractory spacing | Null on missing sample in 5s; exact bpm match on synthetic peaks. |
| **TST-12** | `test_online_offline_code_parity` | REQ-C-05, REQ-D-02, REQ-D-03 | Execute same dataset through online and offline paths | Outputs are mathematically identical with respective `mode` tags. |
| **TST-13** | `test_mode_switch_timing` | REQ-D-04, REQ-D-05, REQ-D-06 | Drop network, restore network; measure transition times | Offline mode active $\le 10$s after drop; online active $\le 10$s after restore. |
| **TST-14** | `test_cloud_catchup_benchmark` | REQ-E-01, REQ-E-02, REQ-C-04 | Representative benchmark: 5.76M samples uploaded to candidate cloud endpoint | Sync completes in $< 15$ minutes ($\ge 6,400$ samples/s); cloud database storage consumption remains $<\text{Free-Tier Quota}$ (500 MB). |
| **TST-15** | `test_clock_change_invariance` | REQ-H-06 | Shift system clock $\pm 2$ hours during active session | 0 skipped samples, 0 window misalignment, 0 assertion errors. |
| **TST-16** | `test_disk_full_resilience` | REQ-H-07 | Mount 50MB loopback disk; exhaust storage | Pipeline logs disk pressure, halts writes cleanly, passes DB integrity check. |
| **TST-17** | `test_arm64_resource_limits` | REQ-H-01, REQ-H-02, REQ-H-03 | 1-hour run in 4-core, 2 GB cgroup | Mean CPU $< 25\%$, Peak RSS $< 150$ MB, disk and network logged. |

---

## 4. Verification Subsystem: Independent Candidate Ground-Truth Recorder & Auditor

To guarantee zero silent data loss before submission, we design an independent ground-truth recording and auditing subsystem (`tools/check_ground_truth.py`).

> [!IMPORTANT]
> The Candidate Ground-Truth Recorder and Checker are strictly independent verification components developed by the candidate to guarantee compliance with Criteria 1, 2, and 7. They do NOT rely on pipeline-generated truth, pipeline databases, or pipeline loss ledgers.

### 4.1 Independent Candidate Ground-Truth Recorder
1. **Source Tap:** Embedded directly inside the mock generator loop.
2. **Recorded Events:**
   - Every generated sample: `{"session_id": S, "sensor": X, "index": i, "values": [...], "ts": float}`.
   - Every session reset / restart event: `{"event": "session_restart", "old_session_id": S1, "new_session_id": S2}`.
   - Every ring-buffer overflow event: `{"event": "buffer_drop", "sensor": X, "from_index": i1, "to_index": i2}`.
3. **Storage:** Written directly to an independent, append-only log file (`ground_truth.ndjson`).

### 4.2 Independent Candidate Ground-Truth Checker (`tools/check_ground_truth.py`)
1. **Input A (Independent Ground Truth):** Reconstructs or reads `ground_truth.ndjson`.
2. **Input B (Pipeline Export):** Reads NDJSON from `pipeline export --session <ID> --from [device|cloud]`.
3. **Auditing Verification Algorithm:**
   - **Missing Samples Audit:** Every sample emitted by the generator must appear in the export, UNLESS the mock generator logged an explicit buffer overflow. Any unaccounted sample is flagged as a **SILENT LOSS DEFECT**.
   - **Duplicate Samples Audit:** The export is audited for repeated `(session_id, sensor, sample_index)` keys. Any duplicate is flagged as a **DUPLICATION DEFECT**.
   - **Altered Values Audit:** Emitted channel values are compared bit-for-bit against ground-truth integer arrays. Any divergence is flagged as a **CORRUPTION DEFECT**.
   - **Loss Ledger Reconciliation:** Exported loss records are reconciled against the mock's actual drop events. Any invented loss or discrepancy is flagged.
   - **Window Analytics Parity:** Section 3 summary statistics and ECG peak detection rules are independently recomputed and compared against `pipeline results`.

---

## 5. Linux & ARM64 Specific Test Procedures

### 5.1 Sudden Power-Loss Verification (Empirical Measurement Procedure)
- **Test Environment:** ARM64 Linux VM, Raspberry Pi with hardware power relay, or kernel crash emulation (`echo c > /proc/sysrq-trigger`).
- **Procedure:**
  1. Start `pipeline` ingesting both sensors at 800 Hz against deterministic ramp data.
  2. Ground-truth recorder logs timestamp and index $i_{\text{cut}}$ at the exact millisecond of power severance.
  3. Execute abrupt, unannounced power severance (physical power disconnect or kernel sysrq crash).
  4. Boot system, launch pipeline recovery audit.
  5. Execute `PRAGMA integrity_check;` to verify zero SQLite database or WAL page corruption.
  6. Query `SELECT MAX(sample_index) FROM samples WHERE sensor = 1;` to determine $i_{\text{committed}}$.
  7. Compute empirical loss: $\Delta t = (i_{\text{cut}} - i_{\text{committed}}) / 400.0\text{ seconds}$.
  8. **Acceptance Condition:** Database opens cleanly; measured loss $\Delta t \le \text{Target Bound}$ (0.50s); post-boot recovery detects the gap and logs an entry in `loss_ledger` with reason `power_loss_interruption`.

### 5.2 Network Severance & Automatic Switching Verification
- **Test Environment:** Linux system with `iptables` / `tc`.
- **Procedure:**
  1. Start pipeline in online streaming mode.
  2. Block outbound traffic to cloud endpoint:
     ```bash
     iptables -A OUTPUT -p tcp --dport 443 -j DROP
     ```
  3. Measure elapsed time until pipeline transitions to offline mode (must be $\le 10$ seconds).
  4. Maintain block for 15 minutes while local collection and offline processing continue.
  5. Remove network block:
     ```bash
     iptables -D OUTPUT -p tcp --dport 443 -j DROP
     ```
  6. Measure elapsed time until pipeline transitions back to online mode (must be $\le 10$ seconds).
  7. Verify cloud backlog sync commences and completes without duplicate samples.

### 5.3 1-Hour Resource Profiling Benchmark
- **Test Environment:** Linux ARM64 restricted to 4 CPU cores and 2 GB memory:
  ```bash
  systemd-run --scope -p CPUQuota=400% -p MemoryMax=2G python3 -m pipeline.cli start
  ```
- **Measurement Script:** `tools/benchmark_resources.py` records every 5 seconds:
  - CPU % via `/proc/stat` and `/proc/<pid>/stat`.
  - Resident Set Size (RSS) memory via `/proc/<pid>/status`.
  - Disk bytes written via `/proc/<pid>/io`.
  - Network bytes transmitted via `/proc/net/dev`.
- **Acceptance:** Average CPU $< 20\%$, Peak RSS $< 150\text{ MB}$, disk/network figures documented for submission.

### 5.4 In-Process CRC Failure, Forward Arrival & Gap Recovery Verification Procedure
- **Objective:** Verify that when CRC retries fail on an interval, the pipeline advances on the wire to maintain forward ingestion without stalling, quarantines the gap without recording false loss, holds contiguous cursors at the gap boundary, and successfully sweeps and recovers the gap during the same process lifetime.
- **Procedure:**
  1. Initialize `biosignal_mock` in deterministic ramp mode with seed 7.
  2. Start `pipeline` ingesting both sensors at 400 Hz. Ingest clean samples up to index 999 ($I_{\text{persisted}} = 999, I_{\text{resolved}} = 999, I_{\text{obs}} = 999$).
  3. Configure mock transport fault injection to return corrupted CRC-32 on Sensor 1 batch $[1000, 1099]$.
  4. Pipeline detects CRC error, issues up to 3 immediate re-reads (`from_index: 1000`), all returning corrupted CRC.
  5. Transport worker logs $[1000, 1099]$ to in-memory `unresolved_crc_incidents` and advances wire polling cursor $C_{\text{poll}} = 1100$.
  6. Disable fault injection. Mock delivers clean batch $[1100, 1199]$.
  7. Pipeline commits $[1100, 1199]$ to `samples`.
  8. **Assert Intermediate State (Gap Present):**
     - Query SQLite: `SELECT MAX(sample_index) FROM samples WHERE sensor = 1;` $\rightarrow 1199$ ($I_{\text{obs}} = 1199$).
     - Query SQLite checkpoints: `contiguous_persisted_index` $\rightarrow 999$, `contiguous_resolved_index` $\rightarrow 999$.
     - Query SQLite: `SELECT COUNT(*) FROM loss_ledger WHERE sensor = 1 AND from_index >= 1000;` $\rightarrow \mathbf{0}$.
     - **Verification Invariant:** Zero false loss is recorded; contiguous cursors did NOT advance across the hole.
  9. Trigger in-process reconciliation sweeper (quiescent poll or 1.0s interval). Client issues targeted re-read: `{"cmd": "read", "sensor": 1, "from_index": 1000}`.
  10. Mock returns clean batch $[1000, 1099]$ with valid CRC-32.
  11. Storage commits $[1000, 1099]$ to `samples` in a single atomic transaction.
  12. **Assert Final State (Gap Recovered):**
      - `unresolved_crc_incidents` is empty.
      - Query SQLite checkpoints: `contiguous_persisted_index` $\rightarrow \mathbf{1199}$, `contiguous_resolved_index` $\rightarrow \mathbf{1199}$, `highest_observed_index` $\rightarrow \mathbf{1199}$.
      - Query SQLite: `SELECT COUNT(*) FROM loss_ledger WHERE sensor = 1 AND from_index >= 1000;` $\rightarrow \mathbf{0}$.
      - Run `pipeline export`: NDJSON contains contiguous samples $0 \dots 1199$ with zero missing entries and zero duplicate records.
      - Check Section 3 window analytics: Windows $w=0, 1, 2$ ($[0, 399], [400, 799], [800, 1199]$) all compute with `missing = 0`.
