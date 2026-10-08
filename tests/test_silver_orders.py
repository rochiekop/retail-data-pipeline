import subprocess

import pytest

from pipeline.run import DBT_DIR, dbt_executable, main, run_step
from tests.conftest import BUSINESS_DATE as D


def _orders(client):
    return client.query(
        "select order_id, order_status, toString(purchased_at) from silver.orders order by order_id"
    ).result_rows


def _order(order_id, status="delivered", purchased="2017-11-24 09:00:00"):
    return {"order_id": order_id, "customer_id": "c1", "order_status": status, "order_purchase_timestamp": purchased}


def test_valid_orders_reach_silver(bronze_loaded):
    run_step("silver", D)
    assert _orders(bronze_loaded) == [
        ("o1", "delivered", "2017-11-24 10:00:00"),
        ("o2", "canceled", "2017-11-24 11:00:00"),
        ("o3", "delivered", "2017-11-24 23:59:59"),
        ("o5", "unavailable", "2017-11-24 12:00:00"),
    ]


def test_rows_added_to_the_latest_load_reach_silver(bronze_loaded, add_bronze_rows):
    # Guards the fixture itself: if its rows missed the latest load, every bad-row test would pass vacuously.
    add_bronze_rows("orders", [_order("o6")])
    run_step("silver", D)
    assert "o6" in [r[0] for r in _orders(bronze_loaded)]


def test_bad_orders_are_left_out_of_silver(bronze_loaded, add_bronze_rows):
    add_bronze_rows(
        "orders",
        [
            _order("o9", purchased="not-a-date"),
            _order("o8", status="teleported"),
            _order("o1", purchased="2017-11-24 10:00:00"),
        ],
    )
    run_step("silver", D)
    assert [r[0] for r in _orders(bronze_loaded)] == ["o1", "o2", "o3", "o5"]


def test_silver_reads_only_the_latest_load(bronze_loaded, add_bronze_rows):
    add_bronze_rows("orders", [_order("o9")])
    run_step("bronze", D)  # a new, clean load of the same Business Date
    run_step("silver", D)
    assert [r[0] for r in _orders(bronze_loaded)] == ["o1", "o2", "o3", "o5"]


def test_rerun_replaces_the_business_date(bronze_loaded):
    run_step("silver", D)
    run_step("silver", D)
    assert len(_orders(bronze_loaded)) == 4
    assert bronze_loaded.query("select count() from silver.order_items").result_rows == [(4,)]


def test_cli_rejects_a_malformed_business_date():
    with pytest.raises(SystemExit):
        main(["--business-date", "2017-13-40", "--step", "silver"])


def test_silver_without_a_bronze_load_fails(warehouse_client):
    with pytest.raises(RuntimeError, match="run the bronze step first"):
        run_step("silver", D)


def test_dbt_refuses_to_run_without_a_business_date(bronze_loaded):
    result = subprocess.run(
        [str(dbt_executable()), "build", "--select", "path:models/silver",
         "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "business_date" in result.stdout
