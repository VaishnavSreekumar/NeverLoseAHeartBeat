# Phase 2, Increment C: Independent Adversarial Audit Report

**Target Workspace:** `C:\Users\vaish\.gemini\antigravity-ide\scratch\biosignal-assessment`  
**Base Commit:** `014b207` (`fix(pipeline): preserve terminal unresolved uncertainty across session transitions`)  
**Audit Date:** 2026-10-16  
**Auditor:** Independent Adversarial Agent  
**Overall Verdict:** **PASS (Hardened & Verified)**

---

## 1. Executive Summary

An independent, adversarial audit of the Increment C local SQLite persistence layer (`pipeline/storage.py`, `pipeline/worker.py`, `pipeline/protocol_client.py`, and `tests/test_storage.py`) was conducted. The implementation was subjected to adversarial fault injection, edge-case analysis, and verification of failure recovery semantics.

The hardened test suite now contains **18 focused storage and integration tests**, with all **79 tests in the full workspace suite passing cleanly**.

---

## 2. In-Depth Analysis of Targeted Review Areas

### Case A: Recovering After a Storage Failure
1. **Commit Failure Sequence:**
   - Client enqueues verified batch in `accepted_queue`.
   - Worker dequeues batch and attempts `StorageEngine.commit_batch(batch)`.
   - SQLite fails during commit (e.g. simulated disk-full `OperationalError`).
   - Transaction is rolled back cleanly.
2. **Strict Queue Acknowledgment:**
   - `accepted_queue.task_done()` is **never** called if commit fails.
   - The unpersisted batch is preserved in `worker.last_failed_batch`, and the exception in `worker.last_fatal_error`.
   - `client.storage_error_event` is asserted, which immediately causes `BiosignalProtocolClient.poll_once()` to pause wire polling.
3. **Recovery Path 1 — In-Process Recovery:**
   - **Operator/Application Action:** Operator resolves the disk/storage issue (e.g. frees disk space), then application invokes `await worker.retry_failed_batch()`.
   - **Outcome:** The failed batch is committed to SQLite, `task_done()` is called, failure states are cleared, `storage_error_event` is cleared, and worker ingestion resumes.
4. **Recovery Path 2 — Process Restart Recovery:**
   - **Limitation Acknowledged:** In-memory `worker.last_failed_batch` resides in RAM and **does not survive process restart**.
   - **Outcome:** On process restart, `recover_checkpoints_on_restart()` reconstructs committed checkpoints from SQLite. Because the failed batch never committed, $I_{\text{persisted}}$ and $I_{\text{resolved}}$ remain at the prior committed boundary (e.g., 99). Wire client queries `recovery_cursor_provider`, initializing wire polling at $I_{\text{resolved}} + 1 = 100$. The mock server re-delivers the missing samples, which are committed cleanly. No path reports the failed batch as persisted until it is durably stored.
- **Test Evidence:** `tests/test_storage.py::test_storage_recovery_after_failure_in_process_and_restart`.

---

### Case B: Checkpoint Reconstruction with Unresolved Gaps
1. **Scenario Tested:**
   - Contiguous stored samples through index 99.
   - Checkpoint recorded with `highest_observed_index = 250` (simulating wire observation ahead of a gap).
   - Unresolved gap beginning at index 100 (no samples and no loss records for 100..249).
2. **Restart Recovery Invariants:**
   - `I_persisted` is authoritatively computed as 99 (samples 0..99).
   - `I_resolved` is authoritatively computed as 99 (blocked by unresolved gap at 100).
   - `I_obs` is restored as 250 according to documented semantics without being incorrectly flagged as corruption merely because samples after 99 are absent.
   - The transport recovery cursor (`ckpt.next_recovery_index`) requests index 100 (the earliest unresolved index), **never** an index beyond the gap.
3. **Genuine Forgery Detection:**
   - If a corrupted checkpoint row claims `contiguous_resolved_index = 150` while actual database contents only resolve through index 99, `recover_checkpoints_on_restart()` detects the contradiction and raises `StorageError`, failing safely.
- **Test Evidence:** `tests/test_storage.py::test_checkpoint_reconstruction_with_unresolved_gaps`.

---

### Case C: Loss-Range Merge Safety & Incident Resolution
1. **Identical Causes:**
   - Adjacent loss ranges with identical causes (`[10, 19]` and `[20, 29]` with `buffer_overflow`) merge cleanly into a single contiguous record `[10, 29]`.
2. **Conflicting Causes:**
   - Adjacent loss ranges with differing causes (`[30, 39]` with `buffer_overflow` and `[40, 49]` with `rate_limit`) do **not** merge and remain distinct rows.
   - Overlapping loss ranges with differing causes raise `ContradictoryLossConflictError` and trigger rollback.
3. **Intervening Stored Sample:**
   - When sample index 60 is physically stored, loss `[50, 59]` and loss `[61, 70]` do not merge across index 60. Attempting to insert a loss record covering index 60 raises `ContradictoryLossConflictError`.
4. **Unresolved Incident Transition:**
   - An unresolved CRC incident at `[80, 89]` (`resolved = 0`) transitions to `resolved = 1` only upon the arrival of authoritative source loss covering that range.
   - Pre-commit resolution refusal: `mark_incident_resolved()` refuses to mark an incident resolved if neither samples nor authoritative loss is committed in SQLite.
- **Test Evidence:** `tests/test_storage.py::test_loss_range_merge_safety_and_incident_transition`.

---

### Case D: Durability Wording and Assumptions
> [!IMPORTANT]
> **Durability Classification (Process-Crash vs Host Power-Loss):**
> 1. **Process-Crash Recovery (`PRAGMA synchronous = NORMAL`):**
>    - SQLite WAL mode with `synchronous=NORMAL` provides complete crash consistency against application process aborts (`SIGKILL`, segmentation faults, or unhandled exceptions).
>    - The WAL file structure guarantees that uncommitted transactions are rolled back and committed transactions remain intact upon reopening the database.
> 2. **Host Power-Loss Durability:**
>    - `synchronous=NORMAL` does **not** issue a synchronous `fsync` call to physical disk hardware on every batch commit. Instead, OS dirty pages are flushed periodically by the operating system.
>    - In the event of sudden physical power loss, dirty OS pages not yet flushed to non-volatile storage can be lost.
>    - Setting `synchronous=FULL` issues per-transaction `fsync` calls, but on commodity SSD/flash drives lacking hardware Power-Loss Protection (PLP) capacitors, drive-level volatile write caches can still lose data during abrupt power cuts.
>    - **Durability Claim Clarification:** Durability against host power loss within the assessment's 0.5-second target is an **unverified design target**, not a verified or unconditional guarantee, until empirical power-cut testing is conducted on PLP-rated target hardware.

---

## 3. Test Suite Execution Summary

- **Focused Storage Tests:** 18 passed in 1.33s (`tests/test_storage.py`)
- **Full Suite Regression:** 79 passed in 2.56s (`pytest -v`)
- **Regressions:** None. All Phase 1 ground-truth tests, Phase 2 Increment B client tests, Increment B adversarial tests, and Increment C storage tests pass 100%.

---

## 4. Git Status

```text
On branch main
Changes not staged for commit:
	modified:   pipeline/__init__.py
	modified:   pipeline/protocol_client.py
	pipeline/storage.py
	pipeline/worker.py

Untracked files:
	docs/phase2_increment_c_adversarial_audit.md
	tests/test_storage.py

no changes added to commit (use "git add" and/or "git commit -a")
```
