from datetime import date
from decimal import Decimal

import psycopg

from pipeline import config
from pipeline.reconcile import Reconciliation, main, reconcile
from pipeline.run import STEPS, run_step
from tests.conftest import BUSINESS_DATE as D

D2 = date(2017, 11, 25)


def _run(*dates):
    for business_date in dates:
        for step in STEPS:
            run_step(step, business_date)


def test_gold_matches_the_shop_database(bronze_loaded):
    _run(D, D2)
    assert reconcile(D, D2) == Reconciliation(Decimal("240.00"), Decimal("240.00"))
    assert main(["--from", "2017-11-24", "--to", "2017-11-25"]) == 0


def test_missing_gold_rows_are_reported(bronze_loaded):
    _run(D, D2)
    bronze_loaded.command("alter table gold.fact_order_items drop partition tuple(toDate('2017-11-25'))")
    assert not reconcile(D, D2).matches
    assert main(["--from", "2017-11-24", "--to", "2017-11-25"]) == 1


def test_unavailable_orders_are_not_revenue(bronze_loaded):
    # o5 is unavailable; give it an Order Item so the exclusion is exercised on both sides.
    with psycopg.connect(config.shop_db().conninfo()) as shop:
        shop.execute(
            "insert into shop.order_items values ('o5', 1, 'p1', 's1', '2017-11-30 12:00:00', 40.00, 4.00)"
        )
    _run(D, D2)
    assert reconcile(D, D2) == Reconciliation(Decimal("240.00"), Decimal("240.00"))
