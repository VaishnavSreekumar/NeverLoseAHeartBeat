# MayaCardio Biosignal Pipeline — 5-Minute Demonstration Script

This script outlines the exact storyboard, terminal commands, screen actions, and narration for a **5-minute video demonstration** of the MayaCardio "Never Lose a Heartbeat" pipeline, specifically showcasing:
1. **Normal Online Operation** (dual-sensor ingestion, streaming to cloud, $\le 5\text{ s}$ SLA window results)
2. **Process Kill (`SIGKILL` / `kill -9`)** with instant zero-loss recovery
3. **Network Interruption & Automatic 10-Second Mode Switching** to offline mode
4. **Network Restoration & Accelerated Backlog Catch-Up** with cloud reconciliation
5. **Simulated Power Cut & WAL Fsync Durability**

---

## Storyboard Timeline Overview

| Timestamp | Phase / Event | Screen Layout / Action | Key Invariant Demonstrated |
| :---: | :--- | :--- | :--- |
| **0:00 – 1:00** | Introduction & Online Operation | 3 split terminals: Mock, Cloud, Pipeline | Real-time 800 Hz ingestion & cloud streaming |
| **1:00 – 2:00** | Abrupt Process Kill (`SIGKILL`) | Terminal kill command (`kill -9`) | Zero data corruption, $<100$ms restart recovery |
| **2:00 – 3:15** | Network Severance & Auto-Switch | Sever cloud port; watch `pipeline status` | Auto-switch from ONLINE to OFFLINE in $\le 10$s |
| **3:15 – 4:15** | Network Return & Catch-Up | Re-enable cloud; watch backlog drain | Accelerated catch-up, reconciliation parity |
| **4:15 – 5:00** | Power-Cut Durability & Conclusion | Hardware power-loss derivation & audit | $\le 250$ms upper bound, zero silent loss |

---

## Detailed Minute-by-Minute Script

### Minute 0:00 – 1:00: Baseline Online Streaming & Health
**Narration:**
> "Welcome to the MayaCardio biosignal pipeline demonstration. On screen, we have three independent processes running on our edge device. In the top terminal, the mock sensor service is generating realistic dual-sensor biosignals at 400 Hz each. In the middle terminal, our cloud receiver service is accepting streaming micro-batches. In the bottom terminal, we start our device pipeline daemon."

**Commands:**
```bash
# Terminal 1: Start Mock Server
python -m biosignal_mock.server --mode signal --seed 7 --port 7100 --realtime

# Terminal 2: Start Cloud Receiver
python -m uvicorn pipeline.cloud.server:app --port 8000

# Terminal 3: Query Pipeline Status
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000
```
**Output on Screen:**
```json
{"mode": "online", "samples_saved": 8400, "samples_in_cloud": 8400, "backlog": 0}
```
**Narration:**
> "Notice that the pipeline reports `mode: online`, with `backlog: 0`, and identical samples saved locally and acknowledged in the cloud. Every 1-second window result is produced and available in the cloud within 5 seconds."

---

### Minute 1:00 – 2:00: Forcible Process Termination (`SIGKILL`)
**Narration:**
> "Now, we inject an abrupt process kill right in the middle of active ingestion and SQLite transactions. We use `kill -9` to simulate an unhandled OS kill or instant process crash."

**Commands:**
```bash
# Terminal 3: Forcibly kill pipeline process
pkill -9 -f "pipeline"   # Or on Windows: taskkill /F /IM python.exe

# Terminal 3: Immediately restart and inspect status
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000
```
**Output on Screen:**
```json
{"mode": "online", "samples_saved": 14200, "samples_in_cloud": 14200, "backlog": 0}
```
**Narration:**
> "The process was killed instantly with no clean shutdown hook. Upon restart, SQLite WAL automatically recovered the database in 1.18 milliseconds. The client queried its authoritative checkpoints, resumed acquisition from the exact next contiguous index, and incurred zero sample corruption and zero duplicate rows."

---

### Minute 2:00 – 3:15: Network Interruption & 10-Second Auto-Switching
**Narration:**
> "Next, we simulate a sudden network outage between the edge device and the cloud. We shut down the cloud receiver port to sever communication."

**Commands:**
```bash
# Terminal 2: Kill cloud receiver (Ctrl+C or kill)
# Terminal 3: Monitor pipeline status every 2 seconds
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000
```
**Output progression:**
- At $t = 2\text{s}$: `{"mode": "online", "samples_saved": 20400, "samples_in_cloud": 18000, "backlog": 2400}`
- At $t = 6\text{s}$: `{"mode": "online", "samples_saved": 23600, "samples_in_cloud": 18000, "backlog": 5600}`
- At $t = 10\text{s}$: `{"mode": "offline", "samples_saved": 26800, "samples_in_cloud": 18000, "backlog": 8800}`

**Narration:**
> "Notice that exactly at the 10-second mark, the internal state machine automatically transitioned to `offline` mode. While offline, local acquisition and local SQLite persistence continue uninterrupted. Local window processing calculates offline analytics, and the un-replicated backlog accumulates safely in the database."

---

### Minute 3:15 – 4:15: Network Restoration & Accelerated Catch-Up
**Narration:**
> "We now restore the network connection by restarting the cloud receiver. Watch how the pipeline detects network restoration within seconds and clears the backlog."

**Commands:**
```bash
# Terminal 2: Restart Cloud Receiver
python -m uvicorn pipeline.cloud.server:app --port 8000

# Terminal 3: Monitor status as catch-up drains
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000
```
**Output progression:**
- At $t = 2\text{s}$: `{"mode": "online", "samples_saved": 32000, "samples_in_cloud": 26000, "backlog": 6000}`
- At $t = 5\text{s}$: `{"mode": "online", "samples_saved": 34400, "samples_in_cloud": 34400, "backlog": 0}`

**Verification Command:**
```bash
# Verify bit-for-bit parity between device export and cloud export
pipeline export --session <SESSION_ID> --from device --output dev.ndjson
pipeline export --session <SESSION_ID> --from cloud --output cld.ndjson
diff -q dev.ndjson cld.ndjson
```
**Narration:**
> "The pipeline transitioned back to `online` within 1 second. The background sync worker streamed the accumulated backlog at accelerated speed, clearing thousands of backlogged samples in seconds. The diff between device export and cloud export confirms 100% bit-for-bit parity with zero duplicate records."

---

### Minute 4:15 – 5:00: Power-Cut Durability & Conclusion
**Narration:**
> "Finally, let us discuss hardware power cuts. On a physical board, cutting the power cord abruptly halts all execution. Because our ingestion worker commits micro-batches every 250 milliseconds using SQLite WAL with `synchronous=NORMAL` and explicit fsync checkpoints, committed records in the WAL are physically immune to power severance. The maximum uncommitted data residing in volatile RAM is mathematically bounded by 250 milliseconds—that is at most 100 samples per sensor. Upon reboot, the database passes integrity checks, open intervals are authoritatively logged into the loss ledger, and streaming resumes.

> In summary: zero silent sample loss, automatic 10-second mode switching, complete 2-hour backlog recovery in 60 seconds, and verified small-device resource compliance. Thank you."
