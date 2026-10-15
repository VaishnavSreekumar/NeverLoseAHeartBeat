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
| **TST-04** | `test_interleaved_sensor_polling_under_faults` | REQ-A-01, REQ-B-01, REQ-B-02 | Inject 5s sensor stall, 10% corrupt CRC, and duplicate frames on single TCP socket | Client alternates queries without deadlock; neither sensor's buffer exceeds 70% capacity (7s lag); zero unforced overflow. |
| **TST-05** | `test_buffer_overflow_loss_logging` | REQ-A-11, REQ-B-08, REQ-H-05 | Pause client read for 12 seconds; resume read | Mock returns `lost` range; loss recorded in `loss_ledger`; zero silent loss. |
| **TST-06** | `test_crc_corruption_and_reread` | REQ-B-04, REQ-A-12 | Inject 10% corrupted CRC responses in mock | Pipeline rejects corrupt frames, requests `from_index`, recovers valid data. |
| **TST-07** | `test_duplicate_response_rejection` | REQ-B-05, REQ-E-03 | Inject duplicate replies from mock | Local SQLite maintains unique sample count; zero primary key collisions. |
| **TST-08** | `test_session_restart_unknown_end` | REQ-E-04, REQ-A-12 | Mock restarts during run; new session ID generated | Loss recorded with `to_index: null` and reason `session_restart`. |
| **TST-09** | `test_sqlite_batch_durability_and_power_loss` | REQ-B-03, REQ-B-06, REQ-B-07 | Abrupt unbuffered crash / power cut during active ingestion | `PRAGMA integrity_check` clean; loss gap $(i_{\text{cut}} - i_{\text{committed}})/400 \le \text{Target Bound}$ (0.5s); gap logged in `loss_ledger`. |
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
