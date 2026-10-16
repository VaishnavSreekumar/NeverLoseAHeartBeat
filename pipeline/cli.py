"""MayaCardio Biosignal Pipeline CLI (Increment B).

Implements the official device pipeline CLI required by Section 4:
1. `pipeline status`:
   Emits one JSON line with current mode, samples saved, samples in cloud, and backlog.
   Until cloud integration is active, cloud figures are reported honestly as null.
2. `pipeline export --session <ID> --from [device|cloud]`:
   Streams NDJSON lines: one line per sample, followed by one line per authoritative loss record.
   --from cloud returns a clear non-zero error until cloud synchronization is deployed.
3. `pipeline results --session <ID>`:
   Emits Section 3 window metrics as NDJSON, one JSON line per window and mode.
   Processes persisted data from SQLite using the shared deterministic processing engine.

All machine-readable outputs are emitted strictly to stdout.
All diagnostics, warnings, and errors are emitted strictly to stderr.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Sequence

from pipeline.export import export_device_data
from pipeline.processing import LossRange, SignalProcessingEngine, WindowResult
from pipeline.storage import StorageEngine

DEFAULT_DB_FILENAME: str = "pipeline.db"


def get_default_db_path() -> Path:
    """Determine the default database file path.

    Checks PIPELINE_DB_PATH environment variable, falling back to 'pipeline.db'
    or 'data/pipeline.db' if it exists.
    """
    env_path = os.environ.get("PIPELINE_DB_PATH")
    if env_path:
        return Path(env_path)

    data_dir_db = Path("data") / DEFAULT_DB_FILENAME
    if data_dir_db.exists():
        return data_dir_db

    return Path(DEFAULT_DB_FILENAME)


def cmd_status(args: argparse.Namespace) -> int:
    """Execute `pipeline status` command."""
    db_path = Path(args.db) if args.db else get_default_db_path()
    cloud_url = getattr(args, "cloud_url", None) or os.environ.get("PIPELINE_CLOUD_URL", "http://localhost:8000")

    samples_saved = 0
    device_sessions: list[str] = []
    local_replicated_samples: Optional[int] = None
    local_backlog: Optional[int] = None

    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path), timeout=5.0)
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='samples';"
                )
                if cursor.fetchone():
                    cursor.execute("SELECT COUNT(*) FROM samples;")
                    samples_saved = cursor.fetchone()[0]
                    cursor.execute("SELECT DISTINCT session_id FROM samples ORDER BY rowid DESC;")
                    device_sessions = [r[0] for r in cursor.fetchall()]

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='cloud_checkpoints';"
                )
                if cursor.fetchone():
                    cursor.execute("SELECT COUNT(*) FROM cloud_checkpoints;")
                    if cursor.fetchone()[0] > 0:
                        cursor.execute("""
                            SELECT COUNT(*) FROM samples s
                            JOIN cloud_checkpoints c ON s.session_id = c.session_id AND s.sensor = c.sensor
                            WHERE s.sample_index <= c.cloud_resolved_index;
                        """)
                        local_replicated_samples = cursor.fetchone()[0]
                        local_backlog = max(0, samples_saved - local_replicated_samples)
            finally:
                conn.close()
        except sqlite3.Error as exc:
            sys.stderr.write(f"Warning: Error querying database '{db_path}': {exc}\n")

    api_key = getattr(args, "api_key", None) or os.environ.get("PIPELINE_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}

    # Probe cloud receiver
    cloud_healthy = False
    cloud_sample_count: Optional[int] = None
    try:
        import httpx
        with httpx.Client(timeout=1.0) as client:
            params = {}
            if len(device_sessions) == 1:
                params["session"] = device_sessions[0]
            resp = client.get(f"{cloud_url.rstrip('/')}/api/v1/status", params=params, headers=headers)
            if resp.status_code == 200:
                cloud_healthy = True
                c_data = resp.json()
                cloud_sample_count = c_data.get("total_samples", 0)
    except Exception:
        cloud_healthy = False

    if cloud_healthy and cloud_sample_count is not None:
        mode = "online"
        samples_in_cloud = cloud_sample_count
        backlog = max(0, samples_saved - (local_replicated_samples if local_replicated_samples is not None else samples_in_cloud))
    elif local_replicated_samples is not None:
        mode = "offline"
        samples_in_cloud = local_replicated_samples
        backlog = local_backlog
    else:
        mode = "offline"
        samples_in_cloud = None
        backlog = None

    status_record = {
        "mode": mode,
        "samples_saved": samples_saved,
        "samples_in_cloud": samples_in_cloud,
        "backlog": backlog,
    }

    sys.stdout.write(json.dumps(status_record) + "\n")
    sys.stdout.flush()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Execute `pipeline export --session <ID> --from [device|cloud]` command."""
    session_id: str = args.session
    source: str = args.from_source.lower()

    if source not in ("device", "cloud"):
        sys.stderr.write(
            f"Error: Unsupported export source '{args.from_source}'. Must be 'device' or 'cloud'.\n"
        )
        return 2

    if source == "cloud":
        cloud_url = getattr(args, "cloud_url", None) or os.environ.get("PIPELINE_CLOUD_URL", "http://localhost:8000")
        api_key = getattr(args, "api_key", None) or os.environ.get("PIPELINE_API_KEY")
        headers = {"X-API-Key": api_key} if api_key else {}
        try:
            import httpx
            with httpx.Client(timeout=10.0) as client:
                resp = client.get(
                    f"{cloud_url.rstrip('/')}/api/v1/export",
                    params={"session": session_id},
                    headers=headers,
                )
                if resp.status_code == 200:
                    content = resp.text
                    lines = [ln for ln in content.strip().splitlines() if ln]
                    for line in lines:
                        sys.stdout.write(line + "\n")
                    sys.stdout.flush()
                    sys.stderr.write(f"Exported {len(lines)} lines for session {session_id} from cloud.\n")
                    return 0
                elif resp.status_code == 404:
                    sys.stderr.write(f"Error: Session '{session_id}' not found in cloud receiver.\n")
                    return 1
                else:
                    sys.stderr.write(
                        f"Error: Cloud export is unavailable. Receiver returned HTTP {resp.status_code}: {resp.text}\n"
                    )
                    return 1
        except Exception as exc:
            sys.stderr.write(
                f"Error: Cloud export is unavailable. Could not connect to cloud receiver at {cloud_url}: {exc}\n"
            )
            return 1

    db_path = Path(args.db) if args.db else get_default_db_path()
    if not db_path.exists():
        sys.stderr.write(f"Error: Database file not found: {db_path}\n")
        return 1

    try:
        storage = StorageEngine(db_path=db_path)
        try:
            # Check if session exists in sessions, samples, or loss ledger
            with storage._lock:
                cursor = storage._conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM sessions WHERE session_id = ? UNION SELECT 1 FROM samples WHERE session_id = ? UNION SELECT 1 FROM loss_ledger WHERE session_id = ? LIMIT 1;",
                    (session_id, session_id, session_id),
                )
                if not cursor.fetchone():
                    sys.stderr.write(
                        f"Error: Session '{session_id}' not found in database '{db_path}'.\n"
                    )
                    return 1

            lines = export_device_data(storage=storage, session_id=session_id, destination=sys.stdout)
            sys.stdout.flush()
            sys.stderr.write(f"Exported {lines} lines for session {session_id}.\n")
            return 0
        finally:
            storage.close()
    except Exception as exc:
        sys.stderr.write(f"Error during export: {exc}\n")
        return 1


def cmd_results(args: argparse.Namespace) -> int:
    """Execute `pipeline results --session <ID>` command."""
    session_id: str = args.session
    db_path = Path(args.db) if args.db else get_default_db_path()

    if not db_path.exists():
        sys.stderr.write(f"Error: Database file not found: {db_path}\n")
        return 1

    try:
        storage = StorageEngine(db_path=db_path)
        try:
            with storage._lock:
                cursor = storage._conn.cursor()

                # Verify session existence in sessions, samples, or loss ledger
                cursor.execute(
                    "SELECT 1 FROM sessions WHERE session_id = ? UNION SELECT 1 FROM samples WHERE session_id = ? UNION SELECT 1 FROM loss_ledger WHERE session_id = ? LIMIT 1;",
                    (session_id, session_id, session_id),
                )
                if not cursor.fetchone():
                    sys.stderr.write(
                        f"Error: Session '{session_id}' not found in database '{db_path}'.\n"
                    )
                    return 1

                # Query all samples for session ordered chronologically
                cursor.execute(
                    """
                    SELECT sensor, sample_index, ch1, ch2, ch3
                    FROM samples
                    WHERE session_id = ?
                    ORDER BY sample_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                rows = cursor.fetchall()

                s1_samples: dict[int, tuple[int, ...]] = {}
                s2_samples: dict[int, tuple[int, ...]] = {}
                for r in rows:
                    sensor = r["sensor"]
                    idx = r["sample_index"]
                    if sensor == 1:
                        s1_samples[idx] = (r["ch1"], r["ch2"], r["ch3"])
                    else:
                        s2_samples[idx] = (r["ch1"], r["ch2"])

                # Query authoritative loss records
                cursor.execute(
                    """
                    SELECT sensor, from_index, to_index, reason
                    FROM loss_ledger
                    WHERE session_id = ?
                    ORDER BY from_index ASC, sensor ASC;
                    """,
                    (session_id,),
                )
                loss_rows = cursor.fetchall()
                loss_ranges: list[LossRange] = []
                for lr in loss_rows:
                    loss_ranges.append(
                        LossRange(
                            sensor=lr["sensor"],
                            from_index=lr["from_index"],
                            to_index=lr["to_index"],
                            reason=lr["reason"],
                        )
                    )

            # Determine maximum window boundary
            max_s1 = max(s1_samples.keys()) if s1_samples else -1
            max_s2 = max(s2_samples.keys()) if s2_samples else -1
            max_loss_to = max(
                (lr.to_index for lr in loss_ranges if lr.to_index is not None),
                default=-1,
            )
            max_loss_from = max((lr.from_index for lr in loss_ranges), default=-1)

            max_index = max(max_s1, max_s2, max_loss_to, max_loss_from)
            if max_index < 0:
                # Session exists but has 0 samples and 0 loss records
                sys.stderr.write(f"Session '{session_id}' has no sample or loss data.\n")
                return 0

            max_window = max_index // 400

            # Execute offline signal processing using shared deterministic engine
            engine = SignalProcessingEngine(session_id=session_id, mode="offline")
            windows_to_process = list(range(max_window + 1))
            results = engine.process_windows(
                windows=windows_to_process,
                sensor1_samples=s1_samples,
                sensor2_samples=s2_samples,
                loss_ranges=loss_ranges,
                session_id=session_id,
            )

            for res in results:
                sys.stdout.write(json.dumps(res.to_dict()) + "\n")

            sys.stdout.flush()
            sys.stderr.write(
                f"Processed {len(results)} windows for session {session_id} in offline mode.\n"
            )
            return 0
        finally:
            storage.close()
    except Exception as exc:
        sys.stderr.write(f"Error calculating results: {exc}\n")
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the pipeline CLI."""
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="MayaCardio Biosignal Pipeline CLI: Never Lose a Heartbeat",
    )
    parser.add_argument(
        "--db",
        dest="db",
        default=None,
        help="Path to pipeline SQLite database file (overrides PIPELINE_DB_PATH)",
    )

    subparsers = parser.add_subparsers(dest="subcommand", title="subcommands")

    # 1. pipeline status
    p_status = subparsers.add_parser(
        "status",
        help="Show pipeline operational status, sample counts, and backlog",
    )
    p_status.add_argument(
        "--db",
        dest="db",
        default=None,
        help="Path to pipeline SQLite database file",
    )
    p_status.add_argument(
        "--cloud-url",
        dest="cloud_url",
        default=None,
        help="URL of the cloud receiver service",
    )
    p_status.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="API key for authenticated cloud receiver",
    )
    p_status.set_defaults(func=cmd_status)

    # 2. pipeline export --session <ID> --from [device|cloud]
    p_export = subparsers.add_parser(
        "export",
        help="Export stored samples and authoritative loss ledger as NDJSON",
    )
    p_export.add_argument(
        "--session",
        required=True,
        help="Target session ID to export",
    )
    p_export.add_argument(
        "--from",
        dest="from_source",
        required=True,
        choices=["device", "cloud"],
        help="Export source: 'device' or 'cloud'",
    )
    p_export.add_argument(
        "--db",
        dest="db",
        default=None,
        help="Path to pipeline SQLite database file",
    )
    p_export.add_argument(
        "--cloud-url",
        dest="cloud_url",
        default=None,
        help="URL of the cloud receiver service",
    )
    p_export.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="API key for authenticated cloud receiver",
    )
    p_export.set_defaults(func=cmd_export)

    # 3. pipeline results --session <ID>
    p_results = subparsers.add_parser(
        "results",
        help="Compute and stream Section 3 window processing results as NDJSON",
    )
    p_results.add_argument(
        "--session",
        required=True,
        help="Target session ID to compute results for",
    )
    p_results.add_argument(
        "--db",
        dest="db",
        default=None,
        help="Path to pipeline SQLite database file",
    )
    p_results.set_defaults(func=cmd_results)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main CLI entrypoint.

    Args:
        argv: Optional command line argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code (0 for success, non-zero for error).
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if not args.subcommand:
        parser.print_help(sys.stderr)
        return 2

    if hasattr(args, "func"):
        return args.func(args)

    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
