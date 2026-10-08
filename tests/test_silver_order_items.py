from decimal import Decimal

from pipeline.run import run_step
from tests.conftest import BUSINESS_DATE as D


def _rows(client, query):
    return client.query(query).result_rows


def _item(order_id, item_id, product_id, price, freight="1.00"):
    return {
        "order_id": order_id, "order_item_id": item_id, "product_id": product_id, "seller_id": "s1",
        "shipping_limit_date": "2017-11-30 10:00:00", "price": price, "freight_value": freight,
    }


def test_valid_order_items_reach_silver(bronze_loaded):
    run_step("silver", D)
    assert _rows(
        bronze_loaded,
        "select order_id, order_item_id, product_id, price, freight from silver.order_items "
        "order by order_id, order_item_id",
    ) == [
        ("o1", 1, "p1", Decimal("100.00"), Decimal("10.00")),
        ("o1", 2, "p2", Decimal("50.00"), Decimal("5.00")),
        ("o2", 1, "p1", Decimal("30.00"), Decimal("3.00")),
        ("o3", 1, "p3", Decimal("20.00"), Decimal("2.00")),
    ]


def test_bad_order_items_are_left_out_of_silver(bronze_loaded, add_bronze_rows):
    add_bronze_rows("orders", [
        {"order_id": "o9", "customer_id": "c1", "order_status": "delivered", "order_purchase_timestamp": "bad"},
    ])
    add_bronze_rows("order_items", [
        _item("o1", "3", "p404", "12.00"),  # unknown product
        _item("o3", "2", "p1", "-5.00"),  # negative price
        _item("o3", "3", "p1", "abc"),  # invalid price
        _item("o3", "4", "p1", "9.00", freight="-1.00"),  # negative freight_value
        _item("o9", "1", "p1", "9.00"),  # parent order quarantined
        _item("o77", "1", "p1", "9.00"),  # unknown order
        _item("o1", "1", "p1", "100.00"),  # duplicate order item
    ])
    run_step("silver", D)
    assert _rows(
        bronze_loaded, "select order_id, order_item_id from silver.order_items order by order_id, order_item_id"
    ) == [("o1", 1), ("o1", 2), ("o2", 1), ("o3", 1)]


def test_item_number_written_differently_is_still_a_duplicate(bronze_loaded, add_bronze_rows):
    # "01" and "1" are the same Order Item; letting both through would fail the uniqueness test and the run.
    add_bronze_rows("order_items", [_item("o1", "01", "p1", "100.00")])
    run_step("silver", D)
    assert _rows(
        bronze_loaded, "select order_id, order_item_id from silver.order_items order by order_id, order_item_id"
    ) == [("o1", 1), ("o1", 2), ("o2", 1), ("o3", 1)]
