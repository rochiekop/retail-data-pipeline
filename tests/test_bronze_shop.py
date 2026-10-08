from datetime import date

import psycopg

from pipeline import config
from pipeline.bronze import extract_shop

D = date(2017, 11, 24)


def _rows(client, query, **params):
    return client.query(query, parameters=params).result_rows


def test_extracts_only_orders_purchased_on_the_business_date(seeded_shop, warehouse_client):
    assert extract_shop(D) == {"orders": 4, "order_items": 4, "order_payments": 4}
    ids = {r[0] for r in _rows(warehouse_client, "select order_id from bronze.orders")}
    # o3 at 23:59:59 belongs to D; o4 at 00:00:00 the next day does not.
    assert ids == {"o1", "o2", "o3", "o5"}


def test_midnight_order_belongs_to_the_next_day(seeded_shop, warehouse_client):
    assert extract_shop(date(2017, 11, 25)) == {"orders": 1, "order_items": 1, "order_payments": 1}
    assert _rows(warehouse_client, "select order_id from bronze.orders") == [("o4",)]


def test_source_values_are_stored_as_text(seeded_shop, warehouse_client):
    extract_shop(D)
    row = _rows(
        warehouse_client,
        "select price, freight_value, _business_date from bronze.order_items "
        "where order_id = 'o1' and order_item_id = '1'",
    )
    assert row == [("100.00", "10.00", D)]
    types = {
        r[0]
        for r in _rows(
            warehouse_client,
            "select type from system.columns "
            "where database = 'bronze' and table = 'orders' and not startsWith(name, '_')",
        )
    }
    assert types == {"Nullable(String)"}


def test_rerun_appends_a_new_load(seeded_shop, warehouse_client):
    extract_shop(D)
    extract_shop(D)
    rows = _rows(
        warehouse_client,
        "select uniqExact(_loaded_at), count() from bronze.orders where _business_date = {d:Date}",
        d=D,
    )
    assert rows == [(2, 8)]


def test_day_without_orders_loads_nothing(seeded_shop, warehouse_client):
    assert extract_shop(date(2017, 1, 1)) == {"orders": 0, "order_items": 0, "order_payments": 0}


def test_rerun_that_finds_nothing_becomes_the_latest_load(seeded_shop, warehouse_client):
    def orders_in_latest_load():
        return _rows(
            warehouse_client,
            "select count() from bronze.orders where _business_date = {d:Date} and _loaded_at = "
            "(select max(_loaded_at) from bronze.shop_loads where _business_date = {d:Date})",
            d=D,
        )

    extract_shop(D)
    assert orders_in_latest_load() == [(4,)]
    with psycopg.connect(config.shop_db().conninfo()) as shop:
        shop.execute("delete from shop.orders where order_purchase_timestamp::date = %s", (D,))
    extract_shop(D)
    assert orders_in_latest_load() == [(0,)]
