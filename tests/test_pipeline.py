"""End-to-end properties of the pipeline, on freshly generated messy data.

The key property for any incremental pipeline: replaying history one day at a
time (with late, duplicated, out-of-order data) must produce exactly the same
tables as rebuilding everything from raw in one go. If it doesn't, the
incremental logic is silently losing or double-counting data.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from pipeline.generate import generate
from pipeline.run import daily, rebuild

START = date(2026, 1, 1)
DAYS = 24  # crosses day 20, where the `channel` field appears (schema drift)
MARTS = ["fct_orders", "dim_customers_scd2", "mart_daily_revenue", "mart_cohort_retention"]


@pytest.fixture(scope="module")
def warehouses(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    raw = root / "raw"
    generate(raw, START, DAYS, orders_per_day=80, seed=11)
    os.environ["RAW_PATH"] = raw.as_posix()

    os.environ["DUCKDB_PATH"] = str(root / "incremental.duckdb")
    for d in range(DAYS):
        daily(START + timedelta(days=d))
    daily(START + timedelta(days=DAYS - 1))  # re-run the last day: must be a no-op

    os.environ["DUCKDB_PATH"] = str(root / "full.duckdb")
    rebuild()
    return root / "incremental.duckdb", root / "full.duckdb", raw


def diff(a: Path, b: Path, table: str) -> int:
    con = duckdb.connect()
    con.execute(f"attach '{a.as_posix()}' as a (read_only)")
    con.execute(f"attach '{b.as_posix()}' as b (read_only)")
    q = f"select count(*) from ((select * from a.{table} except all select * from b.{table}) union all (select * from b.{table} except all select * from a.{table}))"
    return con.execute(q).fetchone()[0]


@pytest.mark.parametrize("table", MARTS)
def test_incremental_replay_equals_full_rebuild(warehouses, table):
    incremental, full, _ = warehouses
    assert diff(incremental, full, table) == 0


def test_late_events_were_actually_exercised(warehouses):
    _, full, _ = warehouses
    con = duckdb.connect(str(full), read_only=True)
    late = con.execute(
        "select count(*) from stg_order_events where load_date > cast(occurred_at_utc + interval 7 hour as date)"
    ).fetchone()[0]
    dupes = con.execute("select count(*) - count(distinct event_id) from base_order_events").fetchone()[0]
    assert late > 0 and dupes > 0  # otherwise the equality test proves nothing


def test_orders_use_the_tier_customers_had_at_the_time(warehouses):
    _, full, _ = warehouses
    con = duckdb.connect(str(full), read_only=True)
    # Customers who were upgraded during the period must have orders at more than one tier.
    upgraded = con.execute(
        """
        select count(*) from (
            select customer_id from fct_orders group by 1 having count(distinct customer_tier_at_order) > 1
        )
        """
    ).fetchone()[0]
    assert upgraded > 0
