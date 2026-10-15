# Critical Engineering Risks & Architectural Assumptions

**Project:** MayaCardio "Never Lose a Heartbeat" Biosignal Pipeline  
**Document Version:** 1.0.0  
**Phase:** Phase 0 — Engineering Foundation  
**Target Deadline:** October 16, 2026, 2:00 PM IST  

---

## 1. Executive Summary & Defensible Durability Guarantee

### 1.1 The Reality of Physical Power Loss: The Three-Layer Durability Model
In safety-critical medical and embedded systems, claiming "zero data loss under arbitrary, unannounced physical power severance" without dedicated battery-backed NVRAM or hardware Super-Capacitor Power-Loss Protection (PLP) is technically indefensible. A systems engineer must distinguish three distinct layers of durability:

1. **Layer 1: Application-Level Commit (SQLite WAL)**
   - When SQLite finishes `COMMIT`, transactions are serialized into the WAL file in user space and passed to the operating system kernel via `write()`.
   - Data is safe against user-space application crashes (`SIGKILL`, segfaults), but remains in the OS dirty page cache.
2. **Layer 2: OS & Filesystem Durability (`fsync` / `fdatasync`)**
   - The OS kernel flushes dirty file system pages from the page cache to the physical disk controller queue.
   - Data is safe against OS kernel panics, but may still reside in the volatile write-back cache of the disk controller.
3. **Layer 3: Physical Storage Controller & Flash Media Guarantees**
   - Commodity storage (Raspberry Pi MicroSD cards, USB flash, cheap eMMC) contains proprietary firmware and internal volatile SRAM/DRAM caches without power-loss protection (PLP).
   - When physical $V_{cc}$ collapses abruptly during a flash page erase/program cycle, in-flight writes can result in torn blocks, or data in the volatile controller buffer can be lost.
   - Synchronous per-sample disk writes (800 `fsync` calls/s) cause extreme flash write amplification (WAF $> 50$) and stall the ingestion loop (flash write latency spikes to 30–100 ms).

### 1.2 Durability Specification: Targets vs. Proven Guarantees
- **Structural Database Integrity:** Guaranteed by SQLite Write-Ahead Logging (`PRAGMA journal_mode = WAL;`) and checksummed WAL frame headers. On reboot, WAL recovery automatically recovers to the last committed atomic frame.
- **Unverified Target Loss Bound:** A target loss window of **$\le 0.50$ seconds** (at most 200 samples per sensor, 400 samples total) is established based on a 250ms batch commit cadence.
  > [!IMPORTANT]
  > This 0.50-second bound is an **UNVERIFIED DESIGN TARGET**. It cannot be claimed as a verified guarantee until empirical testing with sudden unbuffered power cuts is executed and measured on the target storage hardware.
- **Auditability & Discontinuity Accounting:** On reboot, the pipeline inspects `MAX(sample_index)` from the database and the first sample received from the reconnected mock, immediately logging any gap into `loss_ledger` with reason `power_loss_interruption`.
- **Underlying Assumptions:** The host kernel flushes page cache on `fsync()`, and the storage media controller guarantees that committed sectors are not corrupted by neighboring write collapses.

---

## 2. In-Depth Engineering Risk Analysis

### Risk 01: Durability During Sudden Power Loss
* **Failure Scenario:** Power cord is pulled from the Raspberry Pi or host system while writes to the database are actively occurring.
* **Likely Consequence:** Corrupted database header, torn WAL frames, or uncommitted in-memory buffer loss leading to silent sample omission and severe scoring deductions.
* **Mitigation:** 
  - Employ SQLite with Write-Ahead Logging (`PRAGMA journal_mode = WAL;`).
  - Use `PRAGMA synchronous = FULL;` (or `PRAGMA synchronous = NORMAL;` combined with bounded micro-batch transactions).
  - Group incoming sensor samples into atomic micro-batches committed every **250 ms** (100 samples per sensor).
  - Wrap batch writes in an explicit transaction (`BEGIN IMMEDIATE ... COMMIT`) followed by a controlled sync.
  - On restart, SQLite automatically runs WAL recovery to restore the database to the last committed atomic transaction.
* **Exact Empirical Test Required to Establish Defensible Bound:**
  1. Start continuous dual-sensor ingestion at 800 Hz against mock generating deterministic ramp data.
  2. Record parallel ground-truth timestamp and generation index at source.
  3. Trigger an abrupt, unannounced power severance (via hardware relay, physical power cut, or Linux kernel hard crash trigger `echo c > /proc/sysrq-trigger`).
  4. Reboot system, mount filesystem, execute `PRAGMA integrity_check;` to verify structural zero-corruption.
  5. Query `SELECT MAX(sample_index) FROM samples WHERE sensor = 1;` to determine the last durably committed sample $i_{\text{committed}}$.
  6. Compare $i_{\text{committed}}$ against the ground-truth index $i_{\text{cut}}$ emitted at the exact millisecond of power severance.
  7. Compute empirical loss: $\Delta t = (i_{\text{cut}} - i_{\text{committed}}) / 400.0\text{ seconds}$.
  8. Verify that $\Delta t \le \text{Stated Target Bound}$, and verify that post-boot pipeline logs the gap in `loss_ledger`.

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
* **Protocol & Transport Constraints:**
  - Both sensors generate data continuously at 400 Hz (total 800 samples/sec, 1 sample every 2.5 ms per sensor).
  - The mock service operates strictly as a single-client TCP server with a request-response protocol (`{"cmd": "read", "sensor": S}`).
  - Each reply contains at most 1 second of data (`count <= 400`).
  - The mock's internal ring buffer holds only the last 10 seconds (4000 samples per sensor). Older unread samples are permanently dropped.
* **Failure Scenarios & Bottleneck Analysis:**
  1. *Starvation via Sequential Polling:* If the client polls Sensor 1 continuously or waits too long between polls, Sensor 2 accumulates unread samples at 400 samples/sec and overflows its 10-second ring buffer within 10.0 seconds.
  2. *Sensor Stall Deadlock:* The mock intentionally stalls or stops sending one sensor. If the client executes a blocking `socket.recv()` waiting for that sensor's reply, the entire polling loop blocks, starving the other sensor and causing ring buffer overflow on both.
  3. *Corrupt CRC Re-read Delays:* If replies are corrupted, client must issue re-read commands (`from_index`). Multiple sequential re-reads could consume polling cycle time.
  4. *Duplicate Reply Ingestion Latency:* Processing redundant replies must not introduce I/O stalls that back up the TCP stream.
* **Mitigation & Mathematical Feasibility Validation:**
  - **Strict Round-Robin Scheduler:** The ingestion loop strictly alternates polling: `read(sensor=1) -> process -> read(sensor=2) -> process`.
  - **Non-Blocking / Timed Socket I/O:** The TCP socket uses a strict **1.0-second read timeout** (`sock.settimeout(1.0)`). If a sensor read times out (e.g. mock stall), the client immediately logs a communication event and switches to poll the other sensor, preventing mutual starvation.
  - **Nominal Cadence & Margin:** In normal operation at 4 Hz polling (every 250 ms), each read retrieves ~100 samples. Over local TCP loopback, round-trip request-reply latency is $< 1.0\text{ ms}$. Total round-trip time for both sensors is $< 2.5\text{ ms}$ per 250ms window, yielding an operational safety margin of **$> 99\%$ idle/buffer margin**.
  - **Burst Backlog Catch-Up:** When catching up after a stall, each reply returns up to 400 samples (1.0 second of data) in a single $\approx 1\text{ ms}$ round trip. The client can drain 4,000 samples (10 seconds of data) in 10 sequential round trips ($\approx 15\text{ ms}$), catching up at **$> 200\times$ real-time speed**, easily preventing buffer overflow as long as the mock resumes before 10 seconds elapse.
  - **Duplicate & Corrupt Re-read Bounding:** CRC verification is in-memory pure integer zlib ($< 0.1\text{ ms}$). Re-reads are bounded to 3 attempts before declaring an unrecoverable gap in `loss_ledger`. Duplicate frames are detected via `first_index < expected_index` in memory and dropped without disk overhead.
* **Test Required:** Run stress-test harness injecting 5-second selective sensor stalls, 10% corrupted CRCs, and duplicated replies under full 800 Hz generation. Verify that neither sensor's buffer exceeds 70% capacity (7.0 seconds lag) and zero unforced buffer overflow occurs.

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

### Risk 14: Free-Tier Cloud Limitations, Database Overhead & Catch-Up Throughput
* **Failure Scenario:** 
  1. The free cloud hosting provider (e.g. Render) spins down inactive services, causing 50+ second cold-start delays that violate the 5-second online SLA.
  2. Following a 2-hour outage ($5,760,000$ samples), the device attempts to upload the backlog within 15 minutes (900 seconds). High network payload size or individual row inserts in cloud Postgres choke bandwidth or trigger API rate limits.
  3. **Critical Storage Breach:** Storing 5,760,000 individual sample rows in a free cloud database exhausts the provider's free storage quota (e.g. Supabase 500 MB, Neon 512 MB).
* **Storage & Network Rigorous Recalculation:**
  - **Sample Volume:** 2 hours $\times$ 3,600 s $\times$ 800 samples/s = **5,760,000 samples**.
  - **Raw JSON Network Payload:** `{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}` $\approx 65\text{ bytes/sample}$. Total raw payload = $5.76\text{M} \times 65\text{ B} \approx \mathbf{374.4\text{ MB}}$.
  - **Compact JSON Array Payload:** `[1, 120000, 720, 1200, 1560]` $\approx 30\text{ bytes/sample}$. Total uncompressed = $\mathbf{172.8\text{ MB}}$.
  - **Gzip Compressed Transfer:** Applying gzip compression (ratio $\approx 4.5:1$) reduces total wire transfer to $\approx \mathbf{38.4\text{ MB}}$ (or $\approx 80\text{ MB}$ for raw JSON). Required bandwidth over 900 seconds: $80\text{ MB} / 900\text{ s} \approx 89\text{ KB/s}$ (712 kbps), easily sustainable over standard broadband.
  - **Cloud Database Row & Index Overhead:**
    - In PostgreSQL, each table row incurs: Heap tuple header (24 bytes) + column data (24–32 bytes) + padding + B-tree index entry on `(session_id, sensor, sample_index)` (~32 bytes) $\approx \mathbf{88\text{ bytes per row}}$.
    - For 5.76 million individual rows: $5,760,000 \times 88\text{ B} \approx \mathbf{506.8\text{ MB}}$.
    - **QUOTA EXHAUSTION DEFECT:** Supabase free tier provides **500 MB**; Neon provides **512 MB**. Storing one 2-hour outage as individual row items will **exhaust or crash the free-tier database quota**.
* **Mitigations & Architectural Schema Design:**
  - **Chunked Sample Storage in Cloud:** Instead of 1 row per sample (5.76M rows), store samples in 1-second chunks (400 samples/row, 14,400 rows total) or binary bytea blobs in PostgreSQL. This eliminates $>95\%$ of row header and B-tree index overhead, shrinking database footprint from $>500\text{ MB}$ to $<\mathbf{25\text{ MB}}$.
  - **High-Density HTTP Upload:** Stream batches of 2,000 samples per HTTP POST payload ($2,880\text{ requests}$ over 900 seconds = **3.2 requests/second**), completely within free-tier rate limits.
  - **Unverified Target Status:**
    > [!IMPORTANT]
    > The 15-minute catch-up SLA ($< 900\text{ s}$) and cloud free-tier viability are classified as **UNVERIFIED TARGETS**. They must be empirically benchmarked against the live target cloud provider before claiming verified feasibility.
* **Empirical Test Required:** Execute a representative catch-up benchmark uploading 5.76 million synthetic samples to candidate cloud endpoints, measuring end-to-end sync duration, network egress, and cloud database disk space consumption.

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
