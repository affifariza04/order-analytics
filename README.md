# order-analytics

**Analytics engineering with dbt and DuckDB: messy, late, duplicated event data goes in, and tested, contracted, point-in-time-correct marts come out. The incremental pipeline is proven to give the same answer as a full rebuild.**

![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white)
![duckdb](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![python](https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white)
![license](https://img.shields.io/badge/license-MIT-blue)

Most dbt demos use clean seed CSVs. Real event data arrives duplicated, late, in mixed formats, and with columns that appear halfway through the year. This project starts from that kind of data and shows how to stay correct anyway.

## The data problems, and how each is handled

| Problem in the raw data | Where it's fixed | How |
|---|---|---|
| **Duplicates** (~2%, at-least-once delivery) | `stg_order_events` | `qualify row_number() over (partition by event_id …) = 1` |
| **Late arrivals** (~5% land 1–3 days late) | `fct_orders` (incremental) | Recompute every order that received *any* new event since the watermark, from its full history |
| **Out-of-order lifecycle** (payment seen before the order) | `fct_orders` | Hold the order until `order_placed` arrives, then fold all its events |
| **Mixed types** (`amount` as int *or* `"125000"`) | `base_*` + staging | An explicit ingestion schema reads leniently, and staging casts strictly |
| **Mixed timezones** (`+07:00` and `Z`) | staging | Everything is normalised to UTC. Business days are computed in **Jakarta time**. |
| **Schema drift** (`channel` appears on day 20) | staging | The declared column is null before it exists, then `coalesce(…, 'unknown')` |
| **Changing customers** (tier upgrades, moves) | `dim_customers_scd2` | Type-2 SCD with `[valid_from, valid_to)` windows, plus a **point-in-time join**, so each order carries the tier the customer had *when they ordered* |

## Lineage

```
raw/order_events/dt=*/  ─▶ base_order_events ─▶ stg_order_events ─┐
                                                                   ├─▶ fct_orders (incremental) ─▶ mart_daily_revenue
raw/customers/dt=*/     ─▶ base_customer_exports ─▶ stg_customer_exports ─▶ dim_customers_scd2 ─┘          mart_cohort_retention
```

## Correctness, proven rather than assumed

**1. Incremental equals full rebuild.** [`tests/test_pipeline.py`](tests/test_pipeline.py) generates 24 days of messy data. It then replays them one day at a time, exactly as production would have seen them (`as_of` hides files that hadn't landed yet), re-runs the last day, and compares every mart against a single full rebuild with `EXCEPT ALL` in both directions.

```
test_incremental_replay_equals_full_rebuild[fct_orders]              PASSED
test_incremental_replay_equals_full_rebuild[dim_customers_scd2]      PASSED
test_incremental_replay_equals_full_rebuild[mart_daily_revenue]      PASSED
test_incremental_replay_equals_full_rebuild[mart_cohort_retention]   PASSED
test_late_events_were_actually_exercised                             PASSED   ← otherwise the above proves nothing
test_orders_use_the_tier_customers_had_at_the_time                   PASSED
```

**2. Revenue reconciles independently.** A singular test recomputes net revenue straight from de-duplicated events and fails the build if the mart disagrees by a single rupiah.

**3. Contracts on every mart.** `contract: enforced` fails the build if a column's name or type drifts. This caught a real bug during development: `date_trunc` returned a `TIMESTAMP` where consumers were promised a `DATE`.

**4. dbt unit test** for the gnarliest model: duplicates, string amounts, mixed offsets and missing fields in, clean rows out.

**5. Data tests:** unique and not-null keys, accepted values, relationships, payment never before the order, no overlapping SCD2 windows, and mart grain.

## Scale

On one simulated year (365 days × ~1,000 orders/day) on a laptop:

| | |
|---|---|
| Raw events read | **719,417** (14,025 duplicates removed) |
| Orders | **364,526** |
| CRM export rows → customer versions | 1,516,187 → **30,884** |
| Full rebuild **including all 33 tests** | **72 s** |
| Daily incremental run, no new files (idempotency check) | 21 s, mostly dbt startup and tests |

## Design decisions

- **SCD2 as a model, not `dbt snapshot`.** Snapshots record history only while they run: a missed run loses it, and a rebuild can't reproduce it. Because the raw daily exports are immutable and kept, history here is a pure function of the files, so any day can be rebuilt exactly.
- **Watermark-driven incremental logic** (`last_loaded_date`) instead of "the last N days". It handles arbitrarily late data and is naturally idempotent.
- **Business time vs. system time.** Events are stored in UTC, and revenue is reported in Jakarta business days. Mixing these is the classic "revenue moved between days" bug.
- **One dbt process per run.** This mirrors how a scheduler would call it, and no warehouse handles leak between runs.

## Run it

```bash
uv sync
uv run python -m pipeline.generate --days 60          # messy raw data → data/raw/
uv run python -m pipeline.run rebuild                 # full build + all tests
uv run python -m pipeline.run daily --date 2026-02-14 # what a scheduler calls each day
uv run pytest -q                                      # equivalence and property tests
```

MIT licensed.
