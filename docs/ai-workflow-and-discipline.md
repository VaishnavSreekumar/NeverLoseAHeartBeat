# AI Usage, Engineering Discipline & Disclosure Framework

**Project:** MayaCardio "Never Lose a Heartbeat" Biosignal Pipeline  
**Document Version:** 1.0.0  
**Phase:** Phase 0 — Engineering Foundation  
**Target Deadline:** October 16, 2026, 2:00 PM IST  

---

## 1. Context & Available AI Infrastructure

1. **Local Model Infrastructure:**
   - Ollama v0.40.3 is installed locally with model `qwen3:8b` (5.2 GB).
   - This provides an offline, local LLM execution engine capable of code review, regex generation, and docstring formatting without internet access.
2. **Interactive Development Environment:**
   - Antigravity IDE is the primary agentic pair-programming environment.
   - Note: Antigravity IDE and Ollama operate as independent systems. Antigravity IDE utilizes its configured LLM backend, while Ollama remains available for local/offline scripting and independent verification.
3. **Disclosure Policy:**
   - MayaCardio permits AI assistance ("AI tools are welcome with disclosure. Be ready to explain your choices, verify your results and adapt your work").
   - A dedicated AI Disclosure statement will be submitted in `README.md` and the final Design Note.

---

## 2. Engineering Discipline & Quality Principles

To maintain high engineering rigor throughout the 45.5-hour development cycle, all development will strictly adhere to five operational rules:

### Principle 1: Small, Atomic, Reviewable Work Units
- No multi-thousand-line monolithic commits.
- Every subsystem is broken into distinct modules (`transport`, `storage`, `analytics`, `cli`, `sync`).
- Implementation proceeds strictly in the sequence outlined in `docs/delivery-plan.md`.

### Principle 2: Test-First Validation (Verification Before Completion)
- No feature is declared "done" until its corresponding test in `docs/test-strategy.md` passes.
- Unit tests run continuously during coding.
- Integration tests verify boundary conditions (e.g. $w < 4$, missing samples, CRC mismatch).

### Principle 3: Independent Review of Generated Code
- Every generated module must undergo static analysis and manual engineering review.
- Verification checks:
  1. Does it introduce unexpected dependencies?
  2. Does it use wall-clock time instead of monotonic counters?
  3. Does it catch broad exceptions (`except Exception`) without logging?
  4. Does it violate the 4-core / 2 GB RAM envelope?

### Principle 4: Absolute Empirical Integrity (Zero Fabrication)
- Never fabricate test outputs, benchmark figures, cloud latencies, or ARM64 execution results.
- If a test fails, document the failure and fix it.
- If an environment is emulated (e.g. QEMU on x86_64 vs physical Raspberry Pi 4), explicitly state the emulation layer.

### Principle 5: Explicit Defect & Untested Requirements Logging
- Maintain an active issue tracker / test log in `docs/verification-evidence.md`.
- Unimplemented or unverified requirements must remain marked as `Unverified` in `docs/requirements-traceability.md`.

---

## 3. Formal AI Disclosure Template (For Final Submission)

```markdown
### AI Assistance Disclosure
In compliance with the MayaCardio assessment guidelines, the candidate discloses the use of AI tools during this project:
- **Tools Used:** Antigravity IDE (DeepMind agentic coding system) and local Ollama (qwen3:8b).
- **Scope of AI Assistance:** Architecture drafting, requirements traceability mapping, test case generation, boilerplate serialization code, and documentation structuring.
- **Human Verification:** All mathematical formulas (Section 3 window statistics and ECG peak detection rules), SQLite durability configurations (WAL mode and fsync batching), network protocol handling (interleaved dual-sensor polling and CRC-32 validation), and test executions were reviewed, verified, and benchmarked by the candidate.
```
