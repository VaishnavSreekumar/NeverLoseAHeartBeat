# Critical Engineering Risks & Architectural Assumptions

**Project:** MayaCardio "Never Lose a Heartbeat" Biosignal Pipeline  
**Document Version:** 1.0.0  
**Phase:** Phase 0 — Engineering Foundation  
**Target Deadline:** October 16, 2026, 2:00 PM IST  

---

## 1. Executive Summary & Defensible Durability Guarantee

### 1.1 The Reality of Physical Power Loss
In safety-critical medical and embedded systems, claiming "zero data loss under arbitrary, unannounced physical power severance" without dedicated battery-backed NVRAM or hardware Super-Capacitor Power-Loss Protection (PLP) is technically indefensible. 

Flash memory controllers on standard MicroSD cards or commodity eMMC modules experience in-flight block writes when $V_{cc}$ drops below operating threshold. If an application attempts synchronous per-sample disk writes (800 `fsync` calls per second), two catastrophic failures occur:
1. MicroSD/eMMC write latency (typically 5–30 ms per random synchronous flush) instantly causes ingestion thread starvation, causing the mock service's 10-second ring buffer to overflow.
2. Rapid flash endurance degradation (write amplification factor $> 50$) leading to premature media failure.

### 1.2 MayaCardio Defensible Durability Guarantee
Under sudden physical power loss, the pipeline guarantees:
1. **Structural Database Integrity:** The SQLite database file and Write-Ahead Log (WAL) will recover to an internally consistent state without page corruption or unrecoverable indices.
2. **Strictly Bounded Loss Window:** At most **0.50 seconds** (200 samples per sensor, 400 samples total) of in-flight received data will be lost during a sudden power cut.
3. **Auditability & Discontinuity Accounting:** On reboot, the ingestion engine detects any gap between the last committed sample index and the first sample received from the new/reconnected session, immediately logging an auditable entry into the local `loss_ledger`.
4. **Hardware Assumptions:** The host kernel accurately reports `fsync()` completion, and the underlying storage controller does not corrupt previously committed blocks during power collapse.

---

## 2. In-Depth Engineering Risk Analysis

### Risk 01: Durability During Sudden Power Loss
* **Failure Scenario:** Power cord is yanked from the Raspberry Pi or host system while writes to the database are actively occurring.
* **Likely Consequence:** Corrupted database header, half-written WAL frames, or uncommitted in-memory buffer loss leading to silent sample omission and severe scoring deductions.
* **Mitigation:** 
  - Employ SQLite with Write-Ahead Logging (`PRAGMA journal_mode = WAL;`).
  - Use `PRAGMA synchronous = FULL;` (or `PRAGMA synchronous = NORMAL;` combined with bounded micro-batch transactions).
  - Group incoming sensor samples into atomic micro-batches committed every **250 ms** or 100 samples per sensor.
  - Wrap batch writes in an explicit transaction (`BEGIN IMMEDIATE ... COMMIT`) followed by a controlled sync.
  - On restart, SQLite automatically runs WAL recovery to restore the database to the last committed atomic transaction.
* **Test Required:** Run continuous ingestion while issuing an abrupt hardware/container reset (or VM hard reset simulation). On restart, execute `PRAGMA integrity_check;`, calculate the missing sample span, and confirm it is $\le 0.5$ seconds.

---

### Risk 02: Mock Buffer Overflow & Explicit Loss Reporting
* **Failure Scenario:** The mock service only retains the last 10 seconds of data per sensor (4000 samples). If the local ingestion loop is blocked by slow disk I/O, CPU spikes, or garbage collection, the mock drops older samples and appends them to the `lost` list.
* **Likely Consequence:** Permanent data loss. If the pipeline ignores or fails to parse the `lost` array in the mock response, the loss is silent, incurring a 5-point deduction per occurrence (up to 30 points).
* **Mitigation:**
  - Decouple TCP network acquisition from disk persistence via an asynchronous, lock-free queue (`asyncio.Queue` with bounded capacity).
  - The TCP ingestion loop runs with high priority, performing zero expensive operations beyond frame framing and CRC validation.
  - Parse the `lost` array on every reply and immediately insert loss ranges into the `loss_ledger` table with reason `mock_buffer_overflow`.
* **Test Required:** Inject a synthetic 12-second processing stall into the pipeline read loop. Verify that upon resuming, the pipeline records the exact dropped range reported by the mock in its `loss_ledger` and reflects it in `pipeline export`.

---

### Risk 03: Corrupt CRC Responses
* **Failure Scenario:** The mock intentionally injects communication faults by altering payload characters or sending a mismatching `crc32` string.
* **Likely Consequence:** Pipeline ingests corrupted values into the database, skewing heart-rate and window statistical calculations (violating Criterion 7), or fails to re-read and loses data silently.
* **Mitigation:**
  - Extract the raw string of the `data` array exactly as received between the outer `[` and `]`.
  - Compute standard zlib CRC-32 on the UTF-8 bytes: `f"{zlib.crc32(raw_data_bytes) & 0xffffffff:08x}"`.
  - If mismatch occurs, discard the payload immediately and send a re-read command: `{"cmd": "read", "sensor": S, "from_index": expected_index}`.
  - Bound re-reads to 3 attempts before logging a communication failure.
* **Test Required:** Configure mock fault injection to corrupt 5% of responses. Verify that all corrupted payloads are rejected, re-read commands are dispatched, and zero corrupt values enter SQLite.

---

### Risk 04: Duplicate Mock Replies
* **Failure Scenario:** The mock service retransmits the same reply frame twice due to internal fault injection or TCP window retransmissions.
* **Likely Consequence:** Local database contains duplicated sample indices, distorting 1-second window counts and triggering Criterion 1 failure ("Every sample received is on its disk, exactly once").
* **Mitigation:**
  - Database schema enforces a composite unique constraint: `PRIMARY KEY (session_id, sensor, sample_index)`.
  - Batch insert statements utilize `INSERT OR IGNORE INTO samples ...`.
  - Track `expected_next_index` per sensor in memory; if `reply.first_index < expected_next_index`, skip already-stored records before insertion.
* **Test Required:** Feed duplicate NDJSON frames to the pipeline ingestion socket. Query the database to verify `COUNT(*)` matches the unique index range with zero duplicate keys.

---

### Risk 05: Mock Session Restarts & Unknown Loss-Range Ends
* **Failure Scenario:** The mock service crashes or restarts mid-run. This resets the index counter to 0 and generates a new `session_id`. The client was reading previous session data and has an unread gap.
* **Likely Consequence:** Unread samples of the old session are lost forever. If the pipeline does not record this loss, it is classified as silent loss.
* **Mitigation:**
  - On every response, inspect `session_id`. If `session_id != current_session_id`:
    1. Mark the previous session as terminated.
    2. If the previous session ended without `session_end: true`, record a loss entry in `loss_ledger`:  
       `{"sensor": S, "from_index": last_seen_index + 1, "to_index": None, "reason": "session_restart"}`.
    3. Initialize state for the new session starting at `first_index`.
* **Test Required:** Restart the mock service process during an active recording session. Verify that `pipeline export` outputs the `to_index: null` loss record for the aborted session and correctly begins tracking the new session from index 0.

---

### Risk 06: Simultaneous Collection from Two Sensors over Single TCP Socket
* **Failure Scenario:** Both sensors run at 400 Hz (total 800 samples/sec). However, the mock service only accepts a single TCP client connection and requires explicit read requests specifying `"sensor": 1` or `"sensor": 2`.
* **Likely Consequence:** If the pipeline reads Sensor 1 sequentially for too long, Sensor 2's 10-second ring buffer overflows, causing unforced data loss.
* **Mitigation:**
  - Implement a round-robin interleaved polling scheduler over the single connection: dispatch `read(sensor=1)`, await reply, dispatch `read(sensor=2)`, await reply.
  - Balance requests dynamically based on the returned `count` and backlog hints.
  - Maintain a maximum batch size of 400 samples (1.0 second) per read to ensure both sensors are serviced at least twice per second.
* **Test Required:** Run high-throughput ingestion with both sensors for 30 minutes. Measure buffer lag and verify that neither sensor ever lags by more than 1.5 seconds.

---

### Risk 07: Network Failure During Cloud Writes
* **Failure Scenario:** WiFi disconnects, TCP connection resets, or cloud ingestion endpoint times out while the device is in the middle of pushing a sample batch.
* **Likely Consequence:** Network thread blocks or crashes, pipeline hangs, or data in transit is lost if not locally preserved.
* **Mitigation:**
  - **Local-First Architecture:** The cloud sync worker reads strictly from the local SQLite database. It never intercepts the primary ingestion pipeline.
  - Network writes are fully asynchronous and decoupled from local disk storage.
  - HTTP client uses strict connection timeouts (connect: 3s, read: 5s). On failure, the connection is closed, the backoff timer triggers, and mode manager begins the offline transition.
* **Test Required:** Disconnect network interface (simulate with `iptables` drop or cable pull) during an active cloud push. Verify that local ingestion throughput remains exactly 400 Hz per sensor with zero memory growth.

---

### Risk 08: Idempotent Ingestion & Cloud Reconciliation
* **Failure Scenario:** Device pushes a batch of 1,000 samples to the cloud. The cloud receives and commits them, but the HTTP 200 OK response is dropped by a network fault. The device retries the batch upon reconnecting.
* **Likely Consequence:** Cloud database inserts duplicate samples, corrupting sample counts, window metrics, and failing Criterion 6 ("every sample exactly once").
* **Mitigation:**
  - Cloud database table implements `PRIMARY KEY (session_id, sensor, sample_index)`.
  - Ingestion endpoint uses UPSERT / `ON CONFLICT (session_id, sensor, sample_index) DO NOTHING`.
  - Ingestion response returns the highest acknowledged index per sensor: `{"acknowledged_up_to": {"1": idx1, "2": idx2}}`.
  - Device updates its `cloud_synced_checkpoint` based on the cloud's response.
* **Test Required:** Transmit identical sample batches multiple times to the cloud ingestion endpoint. Verify that cloud sample count remains exactly equal to unique samples.

---

### Risk 09: Processing-Window Boundaries & Missing Sample Drift
* **Failure Scenario:** Network or sensor loss causes sporadic sample gaps (e.g., indices 410 to 425 missing). A naive windowing algorithm that partitions data by received sample count rather than absolute sample index will shift all subsequent window boundaries.
* **Likely Consequence:** Sample indices become misaligned with their true time windows ($400w \le i \le 400w + 399$). Mean, min, max, and heart rate become completely invalid for the remainder of the session.
* **Mitigation:**
  - Windows are strictly addressed by integer index arithmetic: `window_id = sample_index // 400`.
  - Window $w$ is defined strictly as the closed index interval $[400w, 400w + 399]$.
  - Missing count is derived as: `missing = 400 - count_present(window_w)`.
* **Test Required:** Pass an input stream with random missing index ranges to the analytics engine. Verify that window IDs and boundary intervals remain mathematically fixed to $[400w, 400w + 399]$.

---

### Risk 10: Heart-Rate Peak Detection & History Requirements
* **Failure Scenario:**
  1. The peak detector checks for candidate peaks at index $i$, which requires 800 samples of history ($[i-800, i-1]$) and 1 sample ahead ($i+1$). If windows are processed independently in isolation, boundary samples cannot be validated.
  2. Heart-rate calculation requires a 5-second window ($[400(w-4), 400(w+1)-1]$, 2000 samples). If $w < 4$ or any single sample in those 2000 samples is missing, `heart_rate` must be `null`.
* **Likely Consequence:** False positive peaks near window boundaries, improper rate calculations, or failure to return `null` on missing samples, causing Criterion 7 failure.
* **Mitigation:**
  - Maintain a rolling sample history buffer spanning at least 2800 samples ($800 + 2000$) in memory.
  - When computing metrics for window $w$, query the rolling buffer for the exact 2000-sample interval $[400(w-4), 400(w+1)-1]$.
  - Check `len(available_samples) == 2000`. If `< 2000`, immediately set `heart_rate = None`.
  - Apply the exact 4 peak rules in sequence: 
    1. History available,
    2. Strict local maximum ($x[i] > x[i-1]$ and $x[i] > x[i+1]$),
    3. $x[i] > 0.5 \times \max(x[i-800:i-1])$,
    4. Refractory spacing ($i - i_{\text{prev}} \ge 100$).
* **Test Required:** Run unit test with:
  - $w < 4$ (assert `heart_rate is None`).
  - 1 sample missing in the 5s window (assert `heart_rate is None`).
  - Artificial peaks with known intervals (e.g. 400 samples apart = 1.0s gap = 60.0 bpm).

---

### Risk 11: Disk Exhaustion on Small Device
* **Failure Scenario:** In continuous operation, 2 sensors $\times$ 400 samples/sec = 800 samples/sec. If uncompressed JSON or verbose database records are stored, disk usage could exceed 100 MB/hour. Over long runs or multi-hour outages, disk space on a small SD card or constrained VM could be exhausted.
* **Likely Consequence:** SQLite throws `SQLITE_FULL`, writes fail, unhandled exceptions crash the daemon, and incoming data is lost.
* **Mitigation:**
  - Design a compact SQLite schema: store values as binary blobs (`BLOB` packed integers via `struct.pack`) or compact integer columns with integer primary keys (`WITHOUT ROWID` or standard compact b-tree).
  - Estimated storage: 800 samples/sec $\times$ ~20 bytes = 16 KB/s $\approx$ 57.6 MB/hour $\approx$ 1.38 GB/24h.
  - Implement a proactive disk space watchdog (`shutil.disk_usage`). If free disk space falls below 100 MB, reject non-essential operations and emit an emergency telemetry alert.
* **Test Required:** Mount a small loopback filesystem (e.g. 50 MB) and run the pipeline until disk space fills. Verify that the pipeline catches `sqlite3.OperationalError: database or disk is full`, maintains database page integrity, and avoids catastrophic crash.

---

### Risk 12: System Clock Changes
* **Failure Scenario:** Hospital NTP synchronization adjusts the system clock forward or backward by hours during an active session, or judges intentionally alter the system clock to test pipeline resilience.
* **Likely Consequence:** Time-based windowing calculations break, timer loops hang or trigger prematurely, and timestamps become non-monotonic.
* **Mitigation:**
  - **Zero Wall-Clock Dependency for Data Logic:** Windowing, sample indexing, and heart-rate intervals are derived strictly from integer sample counters ($i$ and $w$), never from `time.time()` or system timestamps.
  - All internal scheduling, timeouts, and SLA monitors use Python's monotonic clock (`time.monotonic()`), which is immune to system clock shifts.
* **Test Required:** Artificially shift the system time forward by 2 hours and backward by 1 hour during an active pipeline run. Verify that window metrics and sample counts remain perfectly continuous.

---

### Risk 13: ARM64 Compatibility & Resource Limits (4 Cores, 2 GB RAM)
* **Failure Scenario:** The software relies on pre-compiled x86_64 binary wheels or memory-hungry runtimes that exceed the 2 GB memory ceiling or thrash 4 CPU cores on a Raspberry Pi 4.
* **Likely Consequence:** Pipeline cannot be deployed on Raspberry Pi 4 or gets killed by Linux OOM killer (`Out of Memory: Kill process`).
* **Mitigation:**
  - Restrict technology stack to pure-Python 3 and Python standard library where possible (e.g., standard `sqlite3`, `asyncio`, `socket`, `zlib`).
  - Cap all internal queues (`asyncio.Queue(maxsize=1000)`).
  - Explicitly benchmark memory footprint to guarantee steady-state RSS remains $< 150\text{ MB}$, well below the 2 GB limit.
* **Test Required:** Run pipeline inside a Linux cgroup restricted to 4 CPU cores (`cpu.max = "400000 100000"`) and 2 GB RAM (`memory.max = "2G"`). Verify stable execution and record CPU and memory utilization.

---

### Risk 14: Free-Tier Cloud Limitations & Catch-Up Throughput
* **Failure Scenario:** 
  1. The free cloud hosting provider (e.g. Render, Koyeb) puts inactive services to sleep, causing high cold-start latencies ($> 30\text{ s}$) that violate the 5-second online SLA.
  2. Following a 2-hour network outage, the device must upload 5,760,000 backlogged samples within 15 minutes (900 seconds). If uploads are sent with small payloads, the required 6,400 samples/sec will exceed free HTTP rate limits or connection quotas.
* **Likely Consequence:** SLA breach (Criterion 4), timeout during catch-up (Criterion 6), or account suspension for rate abuse.
* **Mitigation:**
  - Choose a free cloud platform with sustained availability or keep-alive pings (e.g. Supabase / Neon direct pooled database or a lightweight FastAPI service on a non-sleeping tier).
  - **High-Density Batch Ingestion:** Backlog sync bundles 2,000 samples per HTTP POST payload in a compressed binary or compact JSON array format. At 2,000 samples/batch, catching up 5.76 million samples requires only 2,880 requests over 900 seconds $\approx$ **3.2 HTTP requests per second**. This easily complies with all cloud free-tier rate limits.
* **Test Required:** Benchmark batch upload of 5.76 million synthetic samples to candidate cloud endpoints and measure completion time (target $< 15\text{ minutes}$).

---

## 3. Risk Priority & Mitigation Matrix

| Risk ID | Description | Severity | Probability | Primary Mitigation |
| :--- | :--- | :---: | :---: | :--- |
| **RSK-01** | Power loss database corruption | Critical | High | SQLite WAL mode, 250ms batch transactions, explicit fsync |
| **RSK-02** | Mock 10s buffer overflow | High | Medium | Decoupled async reader, high-priority TCP loop, loss ledger |
| **RSK-03** | Corrupt CRC-32 response | High | High | Strict zlib CRC validation, re-read via `from_index` |
| **RSK-04** | Duplicate mock replies | Medium | High | `PRIMARY KEY (session_id, sensor, index)`, `INSERT OR IGNORE` |
| **RSK-05** | Mock restart / unknown end | High | Medium | Track `session_id`, log `to_index: null` in loss ledger |
| **RSK-06** | Single socket dual-sensor starvation | High | Medium | Interleaved round-robin polling over single connection |
| **RSK-07** | Network drop during cloud write | Critical | High | Local-first architecture: cloud sync reads from local SQLite |
| **RSK-08** | Cloud duplicate ingestion | High | High | Idempotent UPSERT, checkpoint tracking |
| **RSK-09** | Window boundary sample drift | Critical | Medium | Index-based window partitioning ($400w \le i \le 400w + 399$) |
| **RSK-10** | ECG peak detection & history gaps | High | High | Rolling 2800-sample window, strict 4-condition evaluator |
| **RSK-11** | Disk exhaustion | High | Low | Compact schema, storage monitor watchdog |
| **RSK-12** | System clock manipulation | Medium | Medium | Use sample indices for time; use `time.monotonic()` for clocks |
| **RSK-13** | ARM64 memory/CPU limits | High | Medium | Lightweight pure Python, capped queues, $< 150$ MB RSS |
| **RSK-14** | Free cloud catch-up bottleneck | Critical | High | 2,000-sample batch payloads, 3.2 req/s catch-up throughput |
