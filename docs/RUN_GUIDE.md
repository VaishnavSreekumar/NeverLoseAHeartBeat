# MayaCardio Biosignal Pipeline — Installation & Run Guide

This guide provides end-to-end instructions for deploying, running, and validating the **MayaCardio "Never Lose a Heartbeat"** biosignal pipeline on a fresh edge board (Raspberry Pi 4 ARM64 Linux) or development workstation (x86-64 Linux / Windows / macOS).

---

## 1. System Requirements

### Hardware Targets
- **Target Edge Board:** Raspberry Pi 4 Model B (ARM64 / aarch64, $\le 4$ CPU cores, $\le 2\text{ GB}$ RAM)
- **Host OS:** Raspberry Pi OS 64-bit (Debian 12 Bookworm) or Ubuntu 22.04/24.04 LTS ARM64
- **Storage:** MicroSD Card or SSD ($\ge 8\text{ GB}$ free space)
- **Workstation:** Any Linux, macOS, or Windows environment running Python $\ge 3.10$.

### System Dependencies
Install system packages on a fresh Debian/Ubuntu/Raspberry Pi OS system:
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv git sqlite3 libsqlite3-dev
```

---

## 2. Installation from Scratch

Clone the repository and prepare an isolated virtual environment:

```bash
# 1. Clone repository
git clone <repo_url> biosignal-assessment
cd biosignal-assessment

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate

# 3. Upgrade build tools and install package in editable mode
pip install --upgrade pip setuptools wheel
pip install -e .
```

Verify that the CLI command is accessible:
```bash
pipeline --help
```

---

## 3. Running the Pipeline

The system consists of three decoupled operational tiers:
1. **Mock Sensor Service** (TCP server simulating dual 400 Hz sensors)
2. **Device Pipeline Daemon** (ingestion, SQLite WAL persistence, local analytics, sync worker)
3. **Cloud Receiver Service** (FastAPI receiver, cloud storage, online window analytics)

### Terminal 1: Start the Cloud Receiver (Optional / Cloud Mode)
```bash
# Runs on localhost or remote server on port 8000
python -m uvicorn pipeline.cloud.server:app --host 0.0.0.0 --port 8000
```

### Terminal 2: Start the Mock Sensor Daemon
```bash
# Starts realistic dual-sensor signal generation on port 7100
python -m biosignal_mock.server --mode signal --seed 7 --port 7100 --realtime
```

### Terminal 3: Operate the Device Pipeline CLI

The single device-resident `pipeline` executable supports all required operations:

#### A. Check Pipeline Status
Displays operational mode (`online` or `offline`), total samples persisted on device, samples acknowledged in cloud, and current synchronization backlog:
```bash
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000
```
*Output (Single JSON line):*
```json
{"mode": "online", "samples_saved": 48200, "samples_in_cloud": 48200, "backlog": 0}
```

#### B. Export Acquired Data
Exports sample-by-sample NDJSON records and authoritative loss entries:
```bash
# Export from local device storage
pipeline export --session <SESSION_ID> --from device --output device_export.ndjson

# Export from cloud receiver
pipeline export --session <SESSION_ID> --from cloud --cloud-url http://localhost:8000 --output cloud_export.ndjson
```

#### C. Compute and View Section 3 Analytics Results
Calculates Section 3 1-second window aggregates (mean, min, max, std, missing counts, 4-condition ECG R-peak detection, and 5-second heart rate):
```bash
pipeline results --session <SESSION_ID> --db biosignal_pipeline.db --output results.ndjson
```

---

## 4. Running the Complete Verification Suite

Execute the entire automated test suite (189 comprehensive unit, durability, protocol, processing, cloud, sync, and recovery tests):

```bash
pytest -v
```

Expected result:
```
189 passed in ~30s (100% pass rate)
```

---

## 5. Running Empirical Benchmarks

### A. Phase 4C 2-Hour Backlog Catch-Up SLA Benchmark
Simulates a full 2-hour network outage ($5,760,000$ index space across dual sensors) and validates that catch-up finishes within the 15-minute target:
```bash
python tools/run_phase4_catchup_benchmark.py
```
*Typical result:* Synchronized and reconciled in $\approx 60.1\text{ s}$ ($\approx 15\times$ faster than the 900s SLA) with zero discrepancies and peak RAM $< 86\text{ MB}$.

### B. 1-Hour Online Resource Profiling Harness
Measures average CPU, peak memory, disk write volume, and network transmission:
```bash
python tools/profile_1hour_online.py --duration 60
```
Generates a structured report at `benchmarks/resource_profile_summary.json`.

### C. Independent Ground-Truth Auditor
Verifies zero data loss or corruption against the deterministic mock generator:
```bash
python tools/run_performance_benchmark.py
```
