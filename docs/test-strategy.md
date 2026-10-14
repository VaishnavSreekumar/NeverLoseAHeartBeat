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
| **TST-04** | `test_interleaved_sensor_polling` | REQ-A-01, REQ-B-01, REQ-B-02 | Mock with 2 sensors; run client ingestion for 60s | Both sensors ingested at 400 Hz average; zero buffer overflows. |
| **TST-05** | `test_buffer_overflow_loss_logging` | REQ-A-11, REQ-B-08, REQ-H-05 | Pause client read for 12 seconds; resume read | Mock returns `lost` range; loss recorded in `loss_ledger`; zero silent loss. |
| **TST-06** | `test_crc_corruption_and_reread` | REQ-B-04, REQ-A-12 | Inject 10% corrupted CRC responses in mock | Pipeline rejects corrupt frames, requests `from_index`, recovers valid data. |
| **TST-07** | `test_duplicate_response_rejection` | REQ-B-05, REQ-E-03 | Inject duplicate replies from mock | Local SQLite maintains unique sample count; zero primary key collisions. |
| **TST-08** | `test_session_restart_unknown_end` | REQ-E-04, REQ-A-12 | Mock restarts during run; new session ID generated | Loss recorded with `to_index: null` and reason `session_restart`. |
| **TST-09** | `test_sqlite_batch_durability` | REQ-B-03, REQ-B-06, REQ-B-07 | Commit batching test; assert WAL mode and sync | Bounded commit window ($\le 250$ ms); transactions atomic on crash. |
| **TST-10** | `test_section3_window_metrics` | REQ-F-01..05, REQ-F-14 | Synthetic 1s windows with known statistics | Exact match for mean, min, max, magnitude, missing counts; 1-decimal round. |
| **TST-11** | `test_heart_rate_4_rules` | REQ-F-06..13 | Edge cases: $w<4$, missing samples, refractory spacing | Null on missing sample in 5s; exact bpm match on synthetic peaks. |
| **TST-12** | `test_online_offline_code_parity` | REQ-C-05, REQ-D-02, REQ-D-03 | Execute same dataset through online and offline paths | Outputs are mathematically identical with respective `mode` tags. |
| **TST-13** | `test_mode_switch_timing` | REQ-D-04, REQ-D-05, REQ-D-06 | Drop network, restore network; measure transition times | Offline mode active $\le 10$s after drop; online active $\le 10$s after restore. |
| **TST-14** | `test_cloud_catchup_throughput` | REQ-E-01, REQ-E-02 | Simulate 2-hour backlog (5.76M samples); run sync | Catch-up completes in $< 15$ minutes ($\ge 6,400$ samples/sec). |
| **TST-15** | `test_clock_change_invariance` | REQ-H-06 | Shift system clock $\pm 2$ hours during active session | 0 skipped samples, 0 window misalignment, 0 assertion errors. |
| **TST-16** | `test_disk_full_resilience` | REQ-H-07 | Mount 50MB loopback disk; exhaust storage | Pipeline logs disk pressure, halts writes cleanly, passes DB integrity check. |
| **TST-17** | `test_arm64_resource_limits` | REQ-H-01, REQ-H-02, REQ-H-03 | 1-hour run in 4-core, 2 GB cgroup | Mean CPU $< 25\%$, Peak RSS $< 150$ MB, disk and network logged. |

---

## 4. Verification Tool Design: Candidate Ground-Truth Checker

To guarantee zero silent data loss before submission, we design an independent ground-truth recording and auditing tool (`tools/check_ground_truth.py`).

> [!NOTE]
> This tool is developed as an internal candidate engineering test harness to ensure compliance with MayaCardio Criterion 1, 2, and 7. It is not represented as an official MayaCardio artifact.

### Architecture of `check_ground_truth.py`
1. **Input A (Ground Truth):**
   - For `--mode ramp`: Recomputes expected sample values deterministically from sample index $0$ to $N$.
   - For `--mode signal`: Reads parallel ground-truth stream recorded directly at generation source.
2. **Input B (Pipeline Export):**
   - Reads NDJSON stream from `pipeline export --session <ID> --from device` (or `--from cloud`).
3. **Auditing Verification Algorithm:**
   - **Step 1 (Sample Set Construction):** Populates index set of received samples and checks for duplicate indices.
   - **Step 2 (Loss Ledger Accounting):** Reconciles missing index spans against exported loss records.
   - **Step 3 (Zero Silent Loss Invariant):**
     $$\text{Total Expected Indices} = \text{Count}(\text{Saved Samples}) + \sum_{\text{losses}} (\text{to\_index} - \text{from\_index} + 1)$$
     If any single index in the range $[0, \max(\text{index})]$ is neither in saved samples nor covered by an explicit loss entry, the tool flags a **SILENT LOSS DEFECT** with the exact index and sensor.
   - **Step 4 (Value Verification):** Verifies bit-for-bit equality of channel values against ground-truth math.
   - **Step 5 (Analytics Parity):** Verifies Section 3 statistics against exported `pipeline results`.

---

## 5. Linux & ARM64 Specific Test Procedures

### 5.1 Sudden Power-Loss Verification
- **Test Environment:** ARM64 Linux VM (or Raspberry Pi with smart relay) or container hard kill.
- **Procedure:**
  1. Start `pipeline` ingesting both sensors.
  2. Verify active ingestion for 60 seconds.
  3. Execute hardware reset or ungraceful VM termination (`echo c > /proc/sysrq-trigger` or host power severance).
  4. Boot system, launch pipeline recovery audit.
  5. Compare database contents with ground truth.
  6. **Acceptance:** Database opens cleanly; lost sample span is $\le 0.5$ seconds; loss is logged.

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
