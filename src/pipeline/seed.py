"""Build the simulated sources from the Olist CSVs: the Shop Database and the Merchandising Catalog."""

import argparse
import sys
from pathlib import Path

import psycopg

from pipeline import config
from pipeline.olist import SHOP_TABLES, check_header

SHOP_DDL = """
create schema if not exists shop;
drop table if exists shop.orders, shop.order_items, shop.order_payments,
    shop.customers, shop.sellers, shop.order_reviews;
create table shop.orders (
    order_id text primary key,
    customer_id text not null,
    order_status text not null,
    order_purchase_timestamp timestamp not null,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);
create index on shop.orders ((order_purchase_timestamp::date));
create table shop.order_items (
    order_id text not null,
    order_item_id integer not null,
    product_id text not null,
    seller_id text not null,
    shipping_limit_date timestamp,
    price numeric(12, 2) not null,
    freight_value numeric(12, 2) not null,
    primary key (order_id, order_item_id)
);
create table shop.order_payments (
    order_id text not null,
    payment_sequential integer not null,
    payment_type text,
    payment_installments integer,
    payment_value numeric(12, 2) not null,
    primary key (order_id, payment_sequential)
);
create table shop.customers (
    customer_id text primary key,
    customer_unique_id text not null,
    customer_zip_code_prefix text,
    customer_city text,
    customer_state text
);
create table shop.sellers (
    seller_id text primary key,
    seller_zip_code_prefix text,
    seller_city text,
    seller_state text
);
create table shop.order_reviews (
    review_id text not null,
    order_id text not null,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp
);
"""


def seed_shop_db(olist_dir: Path) -> dict[str, int]:
    for file_name, columns in SHOP_TABLES.values():
        check_header(olist_dir / file_name, columns)

    counts = {}
    with psycopg.connect(config.shop_db().conninfo()) as conn:
        conn.execute(SHOP_DDL)
        for table, (file_name, columns) in SHOP_TABLES.items():
            column_list = ", ".join(columns)
            # FORCE_NULL: Olist quotes empty values (""), which must load as NULL, not ''.
            copy_sql = (
                f"copy shop.{table} ({column_list}) from stdin "
                f"with (format csv, header true, force_null ({column_list}))"
            )
            with conn.cursor() as cur, cur.copy(copy_sql) as copy, (olist_dir / file_name).open("rb") as f:
                while chunk := f.read(1 << 20):
                    copy.write(chunk)
            counts[table] = conn.execute(f"select count(*) from shop.{table}").fetchone()[0]
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.seed")
    parser.add_argument("target", choices=["shop-db"])
    parser.parse_args(argv)
    print(seed_shop_db(config.olist_dir()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
