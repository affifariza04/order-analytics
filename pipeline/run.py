"""Pipeline runner: daily incremental runs, backfills, and full rebuilds.

uv run python -m pipeline.run daily --date 2026-02-14     # what the scheduler calls
uv run python -m pipeline.run backfill --start 2026-01-01 --end 2026-03-01
uv run python -m pipeline.run rebuild                      # full refresh from raw
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


def dbt(*args: str) -> None:
    """One dbt process per run, as a scheduler would do: no warehouse handles
    leak between runs, and a crash can't corrupt the caller."""
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    cmd = [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        *args,
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
    ]
    # check=False: we raise our own error that includes dbt's output.
    done = subprocess.run(
        cmd, check=False, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if done.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed:\n{done.stdout[-3000:]}")


def daily(day: date) -> None:
    """Process everything that has landed up to `day`. Safe to re-run: the
    incremental model only touches orders with events newer than its watermark."""
    dbt("build", "--vars", json.dumps({"as_of": str(day)}))


def backfill(start: date, end: date) -> None:
    day = start
    while day <= end:
        t = time.perf_counter()
        daily(day)
        print(f"{day}  ok  {time.perf_counter() - t:.1f}s")
        day += timedelta(days=1)


def rebuild() -> None:
    dbt("build", "--full-refresh")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--date", type=date.fromisoformat, required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--start", type=date.fromisoformat, required=True)
    b.add_argument("--end", type=date.fromisoformat, required=True)
    sub.add_parser("rebuild")
    a = ap.parse_args()
    if a.cmd == "daily":
        daily(a.date)
    elif a.cmd == "backfill":
        backfill(a.start, a.end)
    else:
        rebuild()
