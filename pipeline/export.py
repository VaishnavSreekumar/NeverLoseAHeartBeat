"""MayaCardio Device Storage Exporter (Phase 2 Increment D).

Converts local SQLite stored samples and loss ledger records into the official
NDJSON export stream required by raw_assessment_spec.txt:
- Sample record: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
- Loss record:   {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}

Compatible with tools/check_ground_truth.py and independent audit oracle.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TextIO

from pipeline.storage import StorageEngine


def export_device_data(
    storage: StorageEngine,
    session_id: str,
    destination: str | Path | TextIO,
) -> int:
    """Export all samples and loss records for a given session into NDJSON format.

    Args:
        storage: StorageEngine instance to read from.
        session_id: Target session ID to export.
        destination: File path or open text file object.

    Returns:
        Total number of lines exported (samples + loss records).
    """
    if isinstance(destination, (str, Path)):
        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "w", encoding="utf-8") as f:
            return _write_export_lines(storage, session_id, f)
    else:
        return _write_export_lines(storage, session_id, destination)


def _write_export_lines(storage: StorageEngine, session_id: str, f: TextIO) -> int:
    """Internal helper writing export lines to text stream."""
    lines_written = 0

    with storage._lock:
        cursor = storage._conn.cursor()
        try:
            # 1. Export stored samples ordered by sample_index, sensor
            cursor.execute(
                """
                SELECT sensor, sample_index, ch1, ch2, ch3
                FROM samples
                WHERE session_id = ?
                ORDER BY sample_index ASC, sensor ASC;
                """,
                (session_id,),
            )
            for row in cursor.fetchall():
                sensor = row["sensor"]
                idx = row["sample_index"]
                if sensor == 1:
                    values = [row["ch1"], row["ch2"], row["ch3"]]
                else:
                    values = [row["ch1"], row["ch2"]]

                record = {
                    "session_id": session_id,
                    "sensor": sensor,
                    "index": idx,
                    "values": values,
                }
                f.write(json.dumps(record) + "\n")
                lines_written += 1

            # 2. Export authoritative loss ledger records ordered by from_index
            cursor.execute(
                """
                SELECT sensor, from_index, to_index, reason
                FROM loss_ledger
                WHERE session_id = ?
                ORDER BY from_index ASC, sensor ASC;
                """,
                (session_id,),
            )
            for row in cursor.fetchall():
                record = {
                    "session_id": session_id,
                    "sensor": row["sensor"],
                    "from_index": row["from_index"],
                    "to_index": row["to_index"],
                    "reason": row["reason"],
                }
                f.write(json.dumps(record) + "\n")
                lines_written += 1

            return lines_written
        finally:
            cursor.close()
