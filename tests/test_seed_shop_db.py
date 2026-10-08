import shutil

import psycopg
import pytest

from pipeline import config
from pipeline.olist import SourceSchemaError
from pipeline.seed import seed_shop_db
from tests.conftest import FIXTURES

EXPECTED_COUNTS = {
    "orders": 5,
    "order_items": 5,
    "order_payments": 5,
    "customers": 5,
    "sellers": 2,
    "order_reviews": 1,
}


def _fetch(query: str):
    with psycopg.connect(config.shop_db().conninfo()) as conn:
        return conn.execute(query).fetchall()


def test_seed_loads_every_shop_table():
    assert seed_shop_db(FIXTURES) == EXPECTED_COUNTS


def test_seed_is_repeatable():
    seed_shop_db(FIXTURES)
    assert seed_shop_db(FIXTURES) == EXPECTED_COUNTS


def test_quoted_empty_values_become_null():
    seed_shop_db(FIXTURES)
    rows = _fetch("select order_delivered_customer_date from shop.orders where order_id = 'o2'")
    assert rows == [(None,)]


def test_zip_codes_keep_leading_zeros():
    seed_shop_db(FIXTURES)
    assert _fetch("select seller_zip_code_prefix from shop.sellers where seller_id = 's2'") == [("04195",)]


def test_changed_columns_fail_the_seed(tmp_path):
    source = tmp_path / "olist"
    shutil.copytree(FIXTURES, source)
    orders = source / "olist_orders_dataset.csv"
    header_renamed = orders.read_text(encoding="utf-8").replace('"order_status"', '"status"', 1)
    orders.write_text(header_renamed, encoding="utf-8")
    with pytest.raises(SourceSchemaError, match="olist_orders_dataset.csv"):
        seed_shop_db(source)
