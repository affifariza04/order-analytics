"""Deterministic generator for messy, realistic raw data, the kind a real
pipeline actually receives. Writes one folder per load date:

  data/raw/order_events/dt=YYYY-MM-DD/events.jsonl   event stream (append-only)
  data/raw/customers/dt=YYYY-MM-DD/customers.jsonl   daily full export of the CRM

Deliberate dirt, so the models must earn their correctness:
  * duplicates   ~2% of events are delivered twice (at-least-once delivery)
  * late arrival ~5% of events land in a file 1-3 days after they happened
  * mixed types  amount sometimes arrives as a string ("125000")
  * timezones    timestamps use +07:00 (WIB) or Z inconsistently
  * schema drift a `channel` field only exists from day 20 on
  * CRM churn    customers change tier/city over time (needs SCD2 history)
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

WIB = timezone(timedelta(hours=7))
CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Yogyakarta", "Makassar"]
TIERS = ["bronze", "silver", "gold"]
CHANNELS = ["web", "mobile", "marketplace"]


@dataclass
class Customer:
    id: str
    name: str
    city: str
    tier: str
    signup: date
    updated: date


def _ts(dt: datetime, rng: random.Random) -> str:
    return (
        dt.astimezone(WIB).isoformat()
        if rng.random() < 0.5
        else dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    )


def generate(root: Path, start: date, days: int, orders_per_day: int = 250, seed: int = 7) -> None:
    rng = random.Random(seed)
    customers: list[Customer] = []
    pending: dict[date, list[dict]] = {}  # load date -> events (late ones land later)
    next_order = 1

    for d in range(days):
        day = start + timedelta(days=d)

        # CRM: signups and attribute changes.
        for _ in range(rng.randint(15, 30)):
            cid = f"C{len(customers) + 1:05d}"
            customers.append(Customer(cid, f"Customer {cid}", rng.choice(CITIES), "bronze", day, day))
        for c in rng.sample(customers, k=max(1, len(customers) // 60)):
            if rng.random() < 0.7 and c.tier != "gold":
                c.tier = TIERS[TIERS.index(c.tier) + 1]
            else:
                c.city = rng.choice(CITIES)
            c.updated = day

        # Orders and their lifecycle events.
        for _ in range(rng.randint(int(orders_per_day * 0.8), int(orders_per_day * 1.2))):
            oid = f"O{next_order:07d}"
            next_order += 1
            cust = rng.choice(customers)
            placed = datetime(day.year, day.month, day.day, tzinfo=WIB) + timedelta(
                seconds=rng.randint(0, 86_399)
            )
            amount = rng.choice([25_000, 48_000, 95_000, 105_000, 149_000, 175_000]) * rng.randint(1, 4)
            channel = rng.choice(CHANNELS)
            lifecycle = [("order_placed", placed)]
            if rng.random() < 0.9:
                paid = placed + timedelta(minutes=rng.randint(1, 600))
                lifecycle.append(("order_paid", paid))
                if rng.random() < 0.04:
                    lifecycle.append(("order_refunded", paid + timedelta(days=rng.randint(1, 5))))

            for kind, at in lifecycle:
                event = {
                    "event_id": f"E{rng.getrandbits(64):016x}",
                    "event_type": kind,
                    "order_id": oid,
                    "customer_id": cust.id,
                    "occurred_at": _ts(at, rng),
                    "amount_idr": str(amount) if rng.random() < 0.1 else amount,
                }
                if day >= start + timedelta(days=20):
                    event["channel"] = channel  # schema drift
                load = at.astimezone(WIB).date()
                if rng.random() < 0.05:
                    load += timedelta(days=rng.randint(1, 3))  # late arrival
                copies = 2 if rng.random() < 0.02 else 1  # at-least-once delivery
                pending.setdefault(load, []).extend([event] * copies)

        _write(root / "order_events" / f"dt={day}" / "events.jsonl", pending.pop(day, []), rng)
        _write(
            root / "customers" / f"dt={day}" / "customers.jsonl",
            [
                {
                    "customer_id": c.id,
                    "name": c.name,
                    "city": c.city,
                    "tier": c.tier,
                    "signup_date": str(c.signup),
                    "updated_at": str(c.updated),
                }
                for c in customers
            ],
            rng,
        )


def _write(path: Path, rows: list[dict], rng: random.Random) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = rows[:]
    rng.shuffle(rows)  # files are never sorted in real life
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--orders-per-day", type=int, default=250)
    ap.add_argument("--start", default="2026-01-01")
    a = ap.parse_args()
    generate(Path("data/raw"), date.fromisoformat(a.start), a.days, a.orders_per_day)
