"""Independent candidate ground-truth verification CLI tool.

Audits pipeline export NDJSON files against raw ground truth without relying
on any pipeline-internal state or assumptions.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure workspace root is in sys.path when invoked directly as a script
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from biosignal_mock.ground_truth import audit_export_against_ground_truth


def main() -> None:
    parser = argparse.ArgumentParser(description="Candidate Independent Ground-Truth Auditor")
    parser.add_argument("--ground-truth", required=True, help="Path to ground_truth.ndjson")
    parser.add_argument("--export", required=True, help="Path to pipeline export NDJSON")
    args = parser.parse_args()

    gt_path = Path(args.ground_truth)
    export_path = Path(args.export)

    if not gt_path.exists():
        print(f"Error: Ground truth file not found: {gt_path}", file=sys.stderr)
        sys.exit(2)
    if not export_path.exists():
        print(f"Error: Export file not found: {export_path}", file=sys.stderr)
        sys.exit(2)

    with open(gt_path, "r", encoding="utf-8") as f_gt, open(export_path, "r", encoding="utf-8") as f_exp:
        report = audit_export_against_ground_truth(f_gt, f_exp)

    print(json.dumps(report.to_dict(), indent=2))

    if report.is_clean:
        print("\n[VERIFICATION PASSED]: Zero silent losses, duplicates, or value corruptions.")
        sys.exit(0)
    else:
        print(f"\n[VERIFICATION FAILED]: {len(report.defects)} defect(s) detected.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
