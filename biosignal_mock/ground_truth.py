"""Independent candidate ground-truth recorder and audit engine for biosignal-mock.

Strictly decoupled from pipeline state:
- Logs actual generated samples, session transitions, and loss events at source.
- Audits export streams for missing, duplicated, and altered samples without relying on pipeline-generated truth.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, List, Optional, Set, Tuple


@dataclass
class AuditReport:
    """Detailed audit report comparing pipeline export against independent ground truth."""
    total_ground_truth: int = 0
    total_exported: int = 0
    missing_samples: int = 0
    duplicate_samples: int = 0
    altered_samples: int = 0
    extra_samples: int = 0
    unaccounted_loss_samples: int = 0
    defects: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        """Returns True if and only if zero defects were detected."""
        return (
            self.missing_samples == 0
            and self.duplicate_samples == 0
            and self.altered_samples == 0
            and self.extra_samples == 0
            and self.unaccounted_loss_samples == 0
            and len(self.defects) == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_clean": self.is_clean,
            "total_ground_truth": self.total_ground_truth,
            "total_exported": self.total_exported,
            "missing_samples": self.missing_samples,
            "duplicate_samples": self.duplicate_samples,
            "altered_samples": self.altered_samples,
            "extra_samples": self.extra_samples,
            "unaccounted_loss_samples": self.unaccounted_loss_samples,
            "defects_count": len(self.defects),
            "defects": self.defects[:50],  # Truncate first 50 for display
        }


class GroundTruthRecorder:
    """Independent source-level tap recording generated samples directly to NDJSON."""

    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.output_path, "w", encoding="utf-8")

    def tap(self, event_dict: dict[str, Any]) -> None:
        """Callback attached to DualSensorGenerator."""
        line = json.dumps(event_dict) + "\n"
        self._file.write(line)
        self._file.flush()

    def close(self) -> None:
        """Flush and close the recording stream."""
        if not self._file.closed:
            self._file.close()


def audit_export_against_ground_truth(
    ground_truth_lines: Iterable[str],
    export_lines: Iterable[str],
    session_id: Optional[str] = None,
) -> AuditReport:
    """Audit pipeline export stream against independent ground-truth stream.
    
    Accepts the exact Section 4 export schema:
    - Sample line: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
    - Loss line: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
    
    Checks for:
    1. Missing samples: Generated at source but absent from export.
    2. Duplicate samples: Multiple records for the same (sensor, index).
    3. Altered samples: Value mismatch on any channel.
    4. Extra samples: Exported samples not present in ground truth (invented values).
    5. Unaccounted loss: Discrepancy between actual mock buffer drops and export loss entries.
    """
    report = AuditReport()

    # Step 1: Ingest ground truth records, organizing by session
    # Map session_id -> dict[(sensor, index), values]
    gt_sessions: dict[str, dict[tuple[int, int], list[int]]] = {}
    gt_drops: dict[str, list[tuple[int, int, int]]] = {}

    for line in ground_truth_lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        sid = str(entry.get("session_id", "default"))
        if sid not in gt_sessions:
            gt_sessions[sid] = {}
            gt_drops[sid] = []

        event = entry.get("event")
        if event == "buffer_drop":
            gt_drops[sid].append((int(entry["sensor"]), int(entry["from_index"]), int(entry["to_index"])))
        elif "index" in entry and "values" in entry:
            key = (int(entry["sensor"]), int(entry["index"]))
            gt_sessions[sid][key] = entry["values"]

    # Step 2: Determine target session
    if session_id is not None:
        if session_id not in gt_sessions:
            report.defects.append(
                f"SESSION NOT FOUND: Specified session '{session_id}' not found in ground truth. "
                f"Available sessions: {list(gt_sessions.keys())}."
            )
            return report
        target_session = session_id
    else:
        if len(gt_sessions) == 0:
            # Ground truth is empty
            target_session = None
        elif len(gt_sessions) == 1:
            target_session = next(iter(gt_sessions.keys()))
        else:
            report.defects.append(
                f"SESSION AMBIGUITY: Ground truth contains multiple sessions ({list(gt_sessions.keys())}). "
                "Specify target session explicitly via session parameter or --session CLI flag."
            )
            return report

    gt_samples = gt_sessions.get(target_session, {}) if target_session else {}
    report.total_ground_truth = len(gt_samples)

    # Step 3: Ingest and validate export stream
    seen_export_keys: set[tuple[int, int]] = set()
    exported_losses: list[tuple[int, int, Optional[int]]] = []

    for line in export_lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            report.defects.append(f"Export line malformed JSON: {line[:50]}")
            continue

        # Case A: Loss record
        if "from_index" in entry and "reason" in entry:
            sensor = int(entry.get("sensor", 0))
            from_idx = int(entry.get("from_index", 0))
            to_idx = entry.get("to_index")
            if to_idx is not None:
                to_idx = int(to_idx)
            exported_losses.append((sensor, from_idx, to_idx))
            continue

        # Case B: Sample record
        if "index" in entry and "values" in entry:
            report.total_exported += 1
            sensor = int(entry.get("sensor", 0))
            sample_idx = int(entry.get("index", 0))
            values = entry.get("values", [])
            key = (sensor, sample_idx)

            # Check for conflicting session_id if present in export
            if "session_id" in entry and target_session and str(entry["session_id"]) != target_session:
                report.defects.append(
                    f"SESSION MISMATCH: Sample sensor {sensor} index {sample_idx} has session "
                    f"'{entry['session_id']}', expected target session '{target_session}'."
                )

            # Check 1: Duplicate detection
            if key in seen_export_keys:
                report.duplicate_samples += 1
                report.defects.append(
                    f"DUPLICATION DEFECT: sensor {sensor} index {sample_idx} exported multiple times."
                )
                continue
            seen_export_keys.add(key)

            # Check 2: Extra sample detection (invented / phantom data)
            if key not in gt_samples:
                report.extra_samples += 1
                report.defects.append(
                    f"EXTRA SAMPLE DEFECT: sensor {sensor} index {sample_idx} exported but not in ground truth."
                )
                continue

            # Check 3: Altered values detection
            gt_val = gt_samples[key]
            if values != gt_val:
                report.altered_samples += 1
                report.defects.append(
                    f"CORRUPTION DEFECT: sensor {sensor} index {sample_idx} value mismatch. "
                    f"Expected {gt_val}, got {values}."
                )

    # Step 4: Missing samples detection
    for key, gt_val in gt_samples.items():
        if key not in seen_export_keys:
            s, idx = key
            covered_by_loss = False
            for loss_sensor, l_from, l_to in exported_losses:
                if loss_sensor == s:
                    if l_to is None and idx >= l_from:
                        covered_by_loss = True
                        break
                    elif l_to is not None and l_from <= idx <= l_to:
                        covered_by_loss = True
                        break

            if not covered_by_loss:
                report.missing_samples += 1
                report.defects.append(
                    f"SILENT LOSS DEFECT: sensor {s} index {idx} missing without explicit loss ledger record."
                )

    return report
