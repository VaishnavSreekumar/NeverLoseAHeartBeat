# MayaCardio Biosignal Pipeline — "Never Lose a Heartbeat"

## Project Overview

This repository implements the reliable, fault-tolerant biosignal acquisition, storage, and processing pipeline for the **MayaCardio Software Systems Engineer Assessment (October 2026)**.

The system continuously acquires high-frequency biosignals from two independent sensors ($400\text{ Hz}$ each) running on an edge device (Raspberry Pi 4 / ARM64 Linux, limited to $\le 4$ CPU cores and $\le 2\text{ GB}$ RAM), durably preserves every sample against power cuts and process crashes, executes real-time streaming analytics to a free cloud service within a 5-second latency SLA, switches autonomously to local offline processing upon network loss ($\le 10\text{ s}$), and idempotently synchronizes backlogged records upon network restoration ($\le 15\text{ minutes}$ for a 2-hour outage).

---

## Repository Layout & Module Responsibilities

```
biosignal-assessment/
├── README.md                           # Project documentation, quickstart, and AI disclosure
├── pyproject.toml                      # Packaging, script entrypoints, and pytest configuration
├── .gitignore                          # Git ignore rules for Python, SQLite, and temp files
│
├── biosignal_mock/                     # [Phase 1] Independent sensor mock service
│   ├── __init__.py
│   ├── server.py                       # TCP server, NDJSON framing, connection handling
│   ├── generator.py                    # 400 Hz dual-sensor deterministic generator
│   ├── ramp.py                         # Ramp mode mathematical generator
│   ├── signal.py                       # Realistic ECG/PPG synthetic signal generator
│   ├── buffer.py                       # 10-second (4000 samples) ring buffer & overflow tracker
│   └── faults.py                       # Fault injection (CRC corruption, stalls, restarts)
│
├── pipeline/                           # [Phase 2 & 5] Device-side acquisition & storage daemon
│   ├── __init__.py
│   ├── cli.py                          # CLI entrypoint: 'pipeline status|export|results'
│   ├── transport.py                    # Interleaved single-connection dual-sensor TCP client
│   ├── storage.py                      # SQLite WAL engine, atomic 250ms batching, fsync
│   ├── loss_ledger.py                  # Loss accounting logger (zero silent loss guarantee)
│   ├── mode_manager.py                 # Network health monitor & 10s auto-switching state machine
│   ├── offline_processor.py            # Local background worker for offline window analytics
│   └── cloud_sync.py                   # Live streamer & high-density backlog catch-up worker
│
├── cloud_service/                      # [Phase 4] Free-tier cloud ingestion & processing service
│   ├── __init__.py
│   ├── api.py                          # FastAPI ingestion gateway (/api/v1/ingest/batch)
│   ├── processor.py                    # Cloud-side window aggregator (5s SLA)
│   └── storage.py                      # Cloud database persistence & idempotent UPSERT
│
├── shared/                             # [Phase 1 & 3] Shared zero-dependency modules
│   ├── __init__.py
│   ├── protocol.py                     # NDJSON models & CRC-32 (zlib) validation logic
│   └── analytics.py                    # Section 3 window stats & 4-condition ECG peak detector
│
├── tools/                              # [Phase 6 & 7] Verification & benchmarking utilities
│   ├── check_ground_truth.py           # Independent auditor comparing exports with ground truth
│   ├── benchmark_resources.py          # 1-hour CPU, RAM, disk, and network profiler
│   └── inject_faults.py                # Network cut, SIGKILL, and power-loss test runners
│
├── tests/                              # Automated test suites across all tiers
│   ├── test_mock_protocol.py           # Protocol framing, CRC-32, and command syntax
│   ├── test_generator.py               # Ramp and signal math verification
│   ├── test_storage_durability.py      # SQLite WAL transactions, crash recovery, deduplication
│   ├── test_analytics.py               # Section 3 formulas and ECG peak detection rules
│   ├── test_online_mode.py             # Cloud ingestion streaming & 5s SLA
│   ├── test_mode_switching.py          # Network cut/restore timing & backlog catch-up
│   └── test_fault_matrix.py            # End-to-end fault resilience suite
│
└── docs/                               # Phase 0 Engineering Foundation deliverables
    ├── requirements-traceability.md    # Category A–H requirements matrix
    ├── risks-and-assumptions.md        # 14 critical engineering risks & durability guarantee
    ├── architecture-proposal.md        # Subsystem architecture, Mermaid diagrams, deduplication
    ├── test-strategy.md                # Comprehensive test strategy & platform separation
    ├── delivery-plan.md                # Time-boxed delivery schedule for Oct 16 deadline
    ├── cloud-feasibility.md            # Free-tier cloud service quota & throughput analysis
    └── ai-workflow-and-discipline.md   # AI usage rules, engineering principles, & disclosure
```

### Module Responsibilities Rationale
1. **`biosignal_mock/`:** Strictly decoupled mock sensor daemon. The pipeline interacts with this exclusively via the specified TCP socket protocol, preserving absolute architectural independence.
2. **`pipeline/`:** Contains all device-resident code. Single entrypoint CLI (`pipeline`) exposes `status`, `export`, and `results`.
3. **`cloud_service/`:** Stateless, asynchronous cloud service running on a free cloud provider. Consumes streaming sample batches and produces `mode='online'` window results.
4. **`shared/`:** Zero-dependency pure Python library containing protocol definitions and mathematical algorithms. Imported by both `pipeline` and `cloud_service` to guarantee 100% calculation parity.
5. **`tools/`:** Dedicated candidate testing harnesses, including the ground-truth verifier and 1-hour resource profiler.
6. **`tests/`:** Pytest test suites mapped directly to requirements.
7. **`docs/`:** Phase 0 architecture, risk analysis, and engineering roadmaps.

---

## Phase 0 Engineering Foundation Artifacts

- **[Requirements Traceability Matrix](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/requirements-traceability.md)**
- **[Critical Engineering Risks & Durability Guarantee](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/risks-and-assumptions.md)**
- **[System Architecture Proposal](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/architecture-proposal.md)**
- **[Verification & Test Strategy](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/test-strategy.md)**
- **[Time-Boxed Delivery Plan](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/delivery-plan.md)**
- **[Cloud Feasibility & Quota Analysis](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/cloud-feasibility.md)**
- **[AI Workflow & Disclosure Framework](file:///C:/Users/vaish/.gemini/antigravity-ide/scratch/biosignal-assessment/docs/ai-workflow-and-discipline.md)**

---

## AI Disclosure Statement

In compliance with the MayaCardio assessment guidelines:
- **Tools Used:** Antigravity IDE (DeepMind agentic coding environment) and local Ollama (`qwen3:8b`).
- **Scope of AI Assistance:** Requirements traceability extraction, architecture drafting, failure scenario modeling, boilerplate code structure, and test strategy formulation.
- **Human Review & Accountability:** All architectural decisions, SQLite transaction durability configurations (`WAL` mode + 250ms batching), mathematical formulas (Section 3 window stats and ECG peak detection rules), and testing plans were authored, reviewed, and verified by the candidate.
