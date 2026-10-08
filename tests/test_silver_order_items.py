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


def _payment(order_id, sequential, value, payment_type="voucher"):
    return {
        "order_id": order_id, "payment_sequential": sequential, "payment_type": payment_type,
        "payment_installments": "1", "payment_value": value,
    }


def _quarantined(client, entity):
    return _rows(
        client,
        f"select record_key, failure_reason from silver.quarantine where entity = '{entity}' order by record_key",
    )


def _bad_parent_order(add_bronze_rows):
    add_bronze_rows("orders", [
        {"order_id": "o9", "customer_id": "c1", "order_status": "delivered", "order_purchase_timestamp": "bad"},
    ])


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


def test_valid_payments_reach_silver(bronze_loaded):
    run_step("silver", D)
    assert _rows(
        bronze_loaded,
        "select order_id, payment_sequential, payment_type, payment_installments, payment_value "
        "from silver.payments order by order_id, payment_sequential",
    ) == [
        ("o1", 1, "voucher", 1, Decimal("15.00")),
        ("o1", 2, "credit_card", 3, Decimal("150.00")),
        ("o2", 1, "credit_card", 1, Decimal("33.00")),
        ("o3", 1, "boleto", 1, Decimal("22.00")),
    ]


def test_bad_order_items_go_to_quarantine(bronze_loaded, add_bronze_rows):
    _bad_parent_order(add_bronze_rows)
    add_bronze_rows("order_items", [
        _item(None, "1", "p1", "9.00"),
        _item("o3", "x", "p1", "9.00"),
        _item("o1", "1", "p1", "100.00"),
        _item("o77", "1", "p1", "9.00"),
        _item("o9", "1", "p1", "9.00"),
        _item("o3", "5", None, "9.00"),
        _item("o1", "3", "p404", "12.00"),
        _item("o3", "3", "p1", "abc"),
        _item("o3", "2", "p1", "-5.00"),
        _item("o3", "6", "p1", "9.00", freight="abc"),
        _item("o3", "4", "p1", "9.00", freight="-1.00"),
    ])
    run_step("silver", D)
    assert _quarantined(bronze_loaded, "order_item") == [
        ("?/1", "missing order_id"),
        ("o1/1", "duplicate order item"),
        ("o1/3", "unknown product"),
        ("o3/2", "negative price"),
        ("o3/3", "invalid price"),
        ("o3/4", "negative freight_value"),
        ("o3/5", "missing product_id"),
        ("o3/6", "invalid freight_value"),
        ("o3/x", "invalid order_item_id"),
        ("o77/1", "unknown order"),
        ("o9/1", "parent order quarantined"),
    ]
    assert _rows(bronze_loaded, "select count() from silver.order_items") == [(4,)]


def test_bad_payments_go_to_quarantine(bronze_loaded, add_bronze_rows):
    _bad_parent_order(add_bronze_rows)
    add_bronze_rows("order_payments", [
        _payment(None, "1", "3.00"),
        _payment("o3", "x", "3.00"),
        _payment("o3", "1", "22.00"),  # duplicate; "voucher" sorts after the original "boleto", so this one is dropped
        _payment("o77", "1", "3.00"),
        _payment("o9", "1", "3.00"),
        _payment("o3", "3", "abc"),
        _payment("o3", "2", "-3.00"),
    ])
    run_step("silver", D)
    assert _quarantined(bronze_loaded, "payment") == [
        ("?/1", "missing order_id"),
        ("o3/1", "duplicate payment"),
        ("o3/2", "negative payment_value"),
        ("o3/3", "invalid payment_value"),
        ("o3/x", "invalid payment_sequential"),
        ("o77/1", "unknown order"),
        ("o9/1", "parent order quarantined"),
    ]
    assert _rows(bronze_loaded, "select count() from silver.payments") == [(4,)]


def test_numbers_written_differently_are_still_duplicates(bronze_loaded, add_bronze_rows):
    # "01" and "1" are the same key; letting both through would fail the uniqueness test and the run.
    # The extra rows sort after the originals ("99.00" > "100.00", "16.00" > "15.00" as text), so they are the ones dropped.
    add_bronze_rows("order_items", [_item("o1", "01", "p1", "99.00")])
    add_bronze_rows("order_payments", [_payment("o1", "01", "16.00")])
    run_step("silver", D)
    assert _rows(bronze_loaded, "select count() from silver.order_items where order_id = 'o1'") == [(2,)]
    assert _rows(bronze_loaded, "select count() from silver.payments where order_id = 'o1'") == [(2,)]
    assert _rows(
        bronze_loaded,
        "select entity, record_key, failure_reason from silver.quarantine where record_key like 'o1/%' order by entity",
    ) == [("order_item", "o1/01", "duplicate order item"), ("payment", "o1/01", "duplicate payment")]


def test_valid_copies_win_over_invalid_duplicates(bronze_loaded, add_bronze_rows):
    # "-5.00" and "-1.00" sort before the valid values; if the bad copies ranked first, the valid ones would be lost.
    add_bronze_rows("order_items", [_item("o1", "1", "p1", "-5.00")])
    add_bronze_rows("order_payments", [_payment("o1", "1", "-1.00")])
    run_step("silver", D)
    assert _rows(bronze_loaded, "select price from silver.order_items where order_id = 'o1' and order_item_id = 1") == [
        (Decimal("100.00"),)
    ]
    assert _rows(
        bronze_loaded, "select payment_value from silver.payments where order_id = 'o1' and payment_sequential = 1"
    ) == [(Decimal("15.00"),)]
    assert _rows(
        bronze_loaded, "select entity, record_key, failure_reason from silver.quarantine order by entity"
    ) == [("order_item", "o1/1", "duplicate order item"), ("payment", "o1/1", "duplicate payment")]
