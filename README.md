# MayaCardio Biosignal Pipeline — "Never Lose a Heartbeat"

## Project Overview

This repository implements the reliable, fault-tolerant biosignal acquisition, storage, and processing pipeline for the **MayaCardio Software Systems Engineer Assessment (October 2026)**.

The system continuously acquires high-frequency biosignals from two independent sensors ($400\text{ Hz}$ each, $800\text{ Hz}$ aggregate) on an edge device (Raspberry Pi 4 / ARM64 Linux, $\le 4$ CPU cores, $\le 2\text{ GB}$ RAM), durably preserves every sample against power cuts and process crashes, executes real-time streaming analytics to a free cloud service within a 5-second latency SLA, switches autonomously to local offline processing upon network loss ($\le 10\text{ s}$), and idempotently synchronizes backlogged records upon network restoration ($\le 15\text{ minutes}$ for a 2-hour outage; measured at $60.14\text{ s}$).

---

## Final Submission Artifacts

Pursuant to Section 5 of the MayaCardio assessment brief:

1. **[Installation & Run Guide](docs/RUN_GUIDE.md):** Complete step-by-step instructions to install, run, and benchmark on a fresh board.
2. **[Engineering Design Note (2 Pages)](docs/DESIGN_NOTE.md):** Architectural design, formal guarantees vs non-guarantees, and mathematical derivation of maximum data loss on process crash ($\le 250\text{ ms}$) vs power cut.
3. **[Failure Test Evidence Report](docs/FAILURE_EVIDENCE.md):** Test results, logs, and coverage across all 189 test cases covering process kills (`SIGKILL`), power loss, network severance, CRC corruption, and conflicting duplicates.
4. **[Resource Measurements & ARM64 Audit](docs/RESOURCE_MEASUREMENTS.md):** Empirical measurements over 1-hour online mode (average CPU $52.36\%$, peak memory $84.28\text{ MB}$, network $51.09\text{ MB/hr}$) and ARM64 Raspberry Pi 4 compliance analysis.
5. **[5-Minute Video Demonstration Script](docs/DEMO_GUIDE.md):** Timed storyboard, exact commands, and screen actions demonstrating network cut, process kill, and power-cut recovery.
6. **[AI Tools & Methodology Disclosure](docs/AI_DISCLOSURE.md):** Formal disclosure of AI tools utilized and engineering quality controls enforced.
7. **[ARM64 Linux Validation Report](docs/ARM64_VALIDATION.md):** Empirical validation running in an emulated ARM64 Linux container (4 CPU cores, 2 GB RAM bounds), 118 container tests, live streaming telemetry, and zero-backlog cloud reconciliation.

---

## Repository Layout

```
biosignal-assessment/
├── README.md                           # Project overview, deliverables index, and quickstart
├── pyproject.toml                      # Package metadata, CLI console script, and dependencies
├── .gitignore                          # Git ignore rules for Python, SQLite, and scratch files
│
├── biosignal_mock/                     # Independent sensor mock service (TCP socket daemon)
│   ├── server.py                       # TCP server, NDJSON framing, connection handling
│   ├── generator.py                    # 400 Hz dual-sensor deterministic generator
│   ├── ramp.py                         # Ramp mode mathematical generator
│   ├── signal.py                       # Realistic ECG/PPG synthetic waveform generator
│   ├── buffer.py                       # 10-second (4000 samples) ring buffer & overflow tracker
│   └── faults.py                       # Fault injection (CRC corruption, stalls, restarts)
│
├── pipeline/                           # Device-side pipeline daemon & modules
│   ├── cli.py                          # CLI entrypoint: 'pipeline status|export|results'
│   ├── storage.py                      # SQLite WAL engine, atomic 250ms batching, fsync
│   ├── sync.py                         # CloudSyncWorker, micro-batching & 10s auto-switching
│   ├── reconciliation.py               # Device/Cloud sample-by-sample & loss reconciliation
│   ├── processing.py                   # Section 3 window analytics & 4-condition ECG detector
│   ├── protocol_client.py              # Single-connection interleaved dual-sensor TCP client
│   ├── worker.py                       # StorageIngestionWorker background thread
│   └── cloud/                          # Free-tier cloud receiver & online processing
│       ├── server.py                   # FastAPI REST gateway (/api/v1/ingest/batch)
│       ├── storage.py                  # Cloud SQLite WAL engine & dual-cursor tracking
│       └── service.py                  # Online window analytics service (5s SLA)
│
├── shared/                             # Zero-dependency shared protocol models
│   └── protocol.py                     # NDJSON models, CRC-32 (zlib) validation & constants
│
├── tools/                              # Verification & empirical benchmarking harnesses
│   ├── profile_1hour_online.py         # 1-hour online CPU, RAM, disk, and network profiler
│   ├── run_phase4_catchup_benchmark.py # 2-hour backlog catch-up SLA benchmark runner
│   ├── run_performance_benchmark.py    # Throughput and restart reconstruction benchmark
│   ├── run_backlog_validation.py       # Full 5.76M index space validation
│   └── check_ground_truth.py           # Independent auditor against mock ground truth
│
├── benchmarks/                         # Empirical benchmark summary JSON outputs
│   ├── phase4c_catchup_summary.json    # 2-hour backlog catch-up measurement data
│   └── resource_profile_summary.json   # 1-hour online resource profiling data
│
├── tests/                              # Automated test suite (189 passing tests)
│   ├── test_phase4c_recovery.py        # Catch-up SLA, crash recovery, and clock skew tests
│   ├── test_sync.py                    # Cloud replication, network cut, auto-switching tests
│   ├── test_cloud.py                   # Cloud idempotent ingestion & cursor invariant tests
│   ├── test_validation_increment_d.py  # SIGKILL, power loss, and torn page fault validation
│   ├── test_processing.py              # Section 3 math, window bounds, ECG peak detector
│   ├── test_storage.py                 # SQLite WAL transactions and cursor recovery
│   └── ...                             # Mock protocol, buffer, signal, and CLI test suites
│
└── docs/                               # Engineering documentation & design specifications
    ├── RUN_GUIDE.md                    # Fresh-board installation and run guide
    ├── DESIGN_NOTE.md                  # 2-page system design note & power-cut derivation
    ├── FAILURE_EVIDENCE.md             # Failure test suite evidence and logs
    ├── RESOURCE_MEASUREMENTS.md        # 1-hour resource profiling and ARM64 compliance audit
    ├── DEMO_GUIDE.md                   # 5-minute video demonstration script
    ├── AI_DISCLOSURE.md                # AI tools & methodology disclosure
    └── ARM64_VALIDATION.md             # ARM64 Linux container empirical validation report
```

---

## Quickstart: Fresh-Board Installation & Run

### 1. Installation
```bash
# Clone repository
git clone <repo_url> biosignal-assessment
cd biosignal-assessment

# Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

### 2. Verify Complete Test Suite
```bash
pytest -v
```
*Expected: 189 passed in ~30 seconds (100% pass rate).*

### 3. Run Pipeline CLI Commands
```bash
# Check status (single JSON line)
pipeline status --db biosignal_pipeline.db --cloud-url http://localhost:8000

# Export device or cloud records
pipeline export --session <SESSION_ID> --from device --output export_dev.ndjson
pipeline export --session <SESSION_ID> --from cloud --cloud-url http://localhost:8000 --output export_cld.ndjson

# Compute Section 3 window analytics
pipeline results --session <SESSION_ID> --db biosignal_pipeline.db --output results.ndjson
```

### 4. Run Empirical Benchmarks
```bash
# Run 2-hour backlog catch-up benchmark (< 15-minute SLA target)
python tools/run_phase4_catchup_benchmark.py

# Run online resource profiler
python tools/profile_1hour_online.py --duration 60
```

---

## AI Disclosure Summary

In compliance with assessment rules, development was conducted using the **Antigravity IDE** (Google DeepMind agentic coding system). All mathematical invariants, Section 3 signal processing algorithms, SQLite transaction durability semantics (`WAL` mode + 250ms batching), and test suites were formally verified against the specification with 100% empirical test coverage. See **[docs/AI_DISCLOSURE.md](docs/AI_DISCLOSURE.md)** for details.
