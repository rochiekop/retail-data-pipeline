from datetime import date
from decimal import Decimal

from openpyxl import load_workbook

from pipeline.run import STEPS, run_step
from tests.conftest import BUSINESS_DATE as D


def _run_all(business_date):
    for step in STEPS:
        run_step(step, business_date)


def _rows(client, query, **params):
    return client.query(query, parameters=params).result_rows


def _revenue(client, business_date=D):
    return _rows(
        client,
        "select product_category, revenue, order_count, order_item_count "
        "from gold.daily_revenue_by_category where business_date = {d:Date} order by product_category",
        d=business_date,
    )


def test_fact_has_one_row_per_order_item_and_flags_revenue(bronze_loaded):
    _run_all(D)
    assert _rows(
        bronze_loaded,
        "select order_id, order_item_id, order_status, price, freight, counts_as_revenue "
        "from gold.fact_order_items order by order_id, order_item_id",
    ) == [
        ("o1", 1, "delivered", Decimal("100.00"), Decimal("10.00"), 1),
        ("o1", 2, "delivered", Decimal("50.00"), Decimal("5.00"), 1),
        ("o2", 1, "canceled", Decimal("30.00"), Decimal("3.00"), 0),
        ("o3", 1, "delivered", Decimal("20.00"), Decimal("2.00"), 1),
    ]


def test_dim_product_resolves_the_product_category(bronze_loaded):
    _run_all(D)
    assert _rows(bronze_loaded, "select product_id, product_category from gold.dim_product order by product_id") == [
        ("p1", "toys"),
        ("p2", "Uncategorized"),  # no category in the catalog
        ("p3", "pc_gamer"),  # category without an English translation keeps its name
    ]


def test_dim_date_covers_the_dataset(bronze_loaded):
    _run_all(D)
    assert _rows(
        bronze_loaded,
        "select min(calendar_date), max(calendar_date), countIf(calendar_date = {d:Date} and is_weekend = 0) "
        "from gold.dim_date",
        d=D,
    ) == [(date(2016, 1, 1), date(2018, 12, 31), 1)]


def test_revenue_by_category_excludes_freight_and_canceled_orders(bronze_loaded):
    _run_all(D)
    assert _revenue(bronze_loaded) == [
        ("Uncategorized", Decimal("50.00"), 1, 1),
        ("pc_gamer", Decimal("20.00"), 1, 1),
        ("toys", Decimal("100.00"), 1, 1),  # o2 (canceled, 30.00) is excluded
    ]


def test_rerunning_a_business_date_changes_nothing(bronze_loaded):
    _run_all(D)
    first = _revenue(bronze_loaded)
    _run_all(D)
    assert _revenue(bronze_loaded) == first
    assert _rows(bronze_loaded, "select count() from gold.fact_order_items") == [(4,)]


def test_each_business_date_is_independent(bronze_loaded):
    _run_all(D)
    first = _revenue(bronze_loaded)
    _run_all(date(2017, 11, 25))
    assert _revenue(bronze_loaded, date(2017, 11, 25)) == [("toys", Decimal("70.00"), 1, 1)]
    assert _revenue(bronze_loaded) == first


def test_product_dropped_from_the_catalog_keeps_its_revenue(bronze_loaded, catalog_file):
    _run_all(D)
    workbook = load_workbook(catalog_file)
    workbook["products"].delete_rows(4)  # p3 (pc_gamer) leaves the catalog
    workbook.save(catalog_file)
    later = date(2017, 11, 25)
    run_step("bronze", later)  # loads the edited catalog
    run_step("dimensions", later)  # rebuilds dim_product without p3
    assert _revenue(bronze_loaded) == [
        ("Uncategorized", Decimal("70.00"), 2, 2),  # o3's 20.00 is still counted
        ("toys", Decimal("100.00"), 1, 1),
    ]


def test_quarantined_rows_do_not_change_revenue(bronze_loaded, add_bronze_rows):
    _run_all(D)
    first = _revenue(bronze_loaded)
    add_bronze_rows("order_items", [
        {"order_id": "o3", "order_item_id": "2", "product_id": "p1", "seller_id": "s1",
         "shipping_limit_date": "2017-11-30 10:00:00", "price": "-5.00", "freight_value": "1.00"},
    ])
    for step in STEPS[1:]:  # rerun from Silver on the same Bronze load
        run_step(step, D)
    assert _rows(bronze_loaded, "select count() from silver.quarantine") == [(1,)]
    assert _revenue(bronze_loaded) == first
