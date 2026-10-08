"""Check that Gold Revenue matches Revenue computed directly from the Shop Database."""

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from pipeline import config
from pipeline.warehouse import connect

# The Revenue rule is written out again here on purpose: the check must not reuse the code it checks.
SOURCE_REVENUE = """
    select coalesce(sum(i.price), 0)
    from shop.order_items i
    join shop.orders o on o.order_id = i.order_id
    where o.order_status not in ('canceled', 'unavailable')
      and o.order_purchase_timestamp::date between %s and %s
"""
GOLD_REVENUE = """
    select sum(revenue)
    from gold.daily_revenue_by_category
    where business_date between {start:Date} and {end:Date}
"""


@dataclass(frozen=True)
class Reconciliation:
    source_revenue: Decimal
    gold_revenue: Decimal

    @property
    def matches(self) -> bool:
        return self.source_revenue == self.gold_revenue


def reconcile(start: date, end: date) -> Reconciliation:
    with psycopg.connect(config.shop_db().conninfo()) as shop:
        source = shop.execute(SOURCE_REVENUE, (start, end)).fetchone()[0]
    client = connect()
    try:
        gold = client.query(GOLD_REVENUE, parameters={"start": start, "end": end}).result_rows[0][0]
    finally:
        client.close()
    return Reconciliation(Decimal(source), Decimal(gold))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.reconcile")
    parser.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)
    result = reconcile(args.start, args.end)
    print(f"Shop Database Revenue: {result.source_revenue}")
    print(f"Gold Revenue:          {result.gold_revenue}")
    print("MATCH" if result.matches else "MISMATCH")
    return 0 if result.matches else 1


if __name__ == "__main__":
    sys.exit(main())
