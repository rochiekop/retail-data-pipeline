# Slice 1: Daily Orders Implementation Plan

> **For agentic workers:** Implement task-by-task (via `/to-tickets` → `/implement`, or superpowers:executing-plans). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** For any Business Date, move Olist Orders, Order Items, Payments and the Merchandising Catalog through Bronze → Silver → Gold in ClickHouse, ending in a star schema and a daily Revenue by Product Category view, orchestrated by Airflow.

**Architecture:** ELT. A Python package (`src/pipeline`) seeds the simulated sources (Postgres Shop Database, Excel Merchandising Catalog) and extracts and loads them unchanged into ClickHouse Bronze. dbt (`dbt/`) transforms Bronze into Silver (typed, checked, with a Quarantine) and then a Gold star schema. Tables are partitioned by Business Date, and a rerun replaces its partition. A CLI (`python -m pipeline.run`) runs one step for one date, and a thin Airflow DAG calls it once per step.

**Tech Stack:** Python 3.12, psycopg 3, openpyxl, clickhouse-connect, dbt-clickhouse 1.9+, ClickHouse 25.8, Postgres 17, Apache Airflow 3.1, Docker Compose, pytest.

**Spec:** `.scratch/slice-1-daily-orders/spec.md` (vocabulary in `CONTEXT.md`, decisions in `docs/adr/`, especially 0003 and 0004)

## Global Constraints

- Images: `postgres:17` (Shop Database, Airflow metadata), `clickhouse/clickhouse-server:25.8` (warehouse and warehouse-test), `apache/airflow:3.1.0`.
- Python `>=3.12`, `dbt-clickhouse>=1.9,<2`, `clickhouse-connect>=0.8`.
- Names follow `CONTEXT.md`: Order, Order Item, Payment, Product, Product Category, Revenue, Freight, Business Date, Quarantine, Bronze/Silver/Gold. Never name things "transaction", "sales", "raw" (except the `raw_` column prefix below), "staging" or "marts".
- Business Date = date part of `order_purchase_timestamp`, the timestamp as stored (São Paulo local time).
- Revenue = sum of Order Item `price`, excluding Freight, for Orders whose status is not `canceled` or `unavailable`.
- Bronze is append-only. Every source column is `Nullable(String)`. Shop tables add `_business_date Date` and `_loaded_at DateTime64(6, 'UTC')`; catalog tables add `_source_checksum String` and `_loaded_at`.
- Silver tables and Gold fact tables are `MergeTree`, `partition by business_date`. A run drops its Business Date's partition and then inserts.
- The dbt profile sets `join_use_nulls: 1`. Without it, ClickHouse LEFT JOINs return `''` or `0` instead of NULL and the Silver checks silently pass bad rows.
- **Unique CTE names:** dbt inlines ephemeral models as nested CTEs, so every CTE name must be unique across models (`orders_src`, `items_typed`, ...), never a generic `src` or `typed`.
- **ClickHouse alias rule:** never alias an expression to a name that is already a column of its input (for example `toInt32OrNull(order_item_id) as order_item_id`). ClickHouse substitutes aliases everywhere, which causes cyclic-alias errors or wrong results. The checks models therefore expose source text as `raw_*` and typed values under new names, and the Silver models rename them to the final names.
- A missing file, missing sheet or changed columns fails the run (`SourceSchemaError` / `FileNotFoundError`). Bad rows go to `silver.quarantine` and the run continues.
- Tests only touch the `shop_test` Postgres database and the `warehouse-test` ClickHouse container (port 8124), never `shop` or the `warehouse` container.
- Shell commands are written for Git Bash on Windows (`.venv/Scripts/...`). On macOS or Linux use `.venv/bin/...`.

## Review Focus

1. **Quoted empty values in Olist CSVs** (`""` for a missing timestamp) must become NULL, not an empty string that later fails a cast. Pinned in Task 2.
2. **UTF-8 BOM at the start of `product_category_name_translation.csv`** must not make the header check reject a valid file. Pinned in Task 3.
3. **Orders at the day boundary** (`23:59:59` vs `00:00:00`) must land in their own Business Date. Pinned in Task 4.
4. **A rerun after the source is fixed** must remove that date's old Quarantine rows, even when the new run quarantines nothing. Dropping the partition does this; an insert that only overwrites partitions it writes to would not. Pinned in Task 6.
5. **A Product whose category has no English translation** (Olist's `pc_gamer`) must keep its original category name rather than fall into Uncategorized. Pinned in Task 8.

---

## File Structure

```
.gitignore
pyproject.toml
docker-compose.yml
README.md                          # runbook (Task 10)
airflow/Dockerfile
airflow/dags/daily_orders.py
src/pipeline/__init__.py
src/pipeline/config.py             # connection settings and paths, from env vars
src/pipeline/warehouse.py          # ClickHouse client
src/pipeline/olist.py              # source contract: Olist files, tables and columns; SourceSchemaError
src/pipeline/seed.py               # seed the Shop Database, build the Merchandising Catalog (CLI)
src/pipeline/bronze.py             # extract + load into Bronze (shop + catalog)
src/pipeline/run.py                # run one step for one Business Date (CLI)
src/pipeline/reconcile.py          # Gold vs Shop Database Revenue check (CLI)
dbt/dbt_project.yml
dbt/profiles.yml
dbt/macros/*.sql                   # business_date, delete_business_date, latest loads, schema naming
dbt/models/sources.yml
dbt/models/silver/_checks/*.sql    # ephemeral: typed rows + failure_reason
dbt/models/silver/*.sql            # orders, order_items, payments, quarantine; ephemeral products, product_categories
dbt/models/gold/facts/fact_order_items.sql
dbt/models/gold/dimensions/dim_product.sql, dim_date.sql
dbt/models/gold/reports/daily_revenue_by_category.sql
dbt/tests/*.sql                    # singular uniqueness tests
tests/conftest.py
tests/fixtures/olist_mini/*.csv    # tiny Olist-shaped dataset
tests/test_*.py
```

Pipeline steps (CLI and Airflow tasks, in order): `bronze` → `silver` → `facts` → `dimensions`. The `dimensions` step also rebuilds the report views, and only one run executes it at a time.

---

### Task 1: Project scaffold, local databases, config

**Files:**
- Create: `.gitignore`, `pyproject.toml`, `docker-compose.yml`, `src/pipeline/__init__.py`, `src/pipeline/config.py`, `src/pipeline/warehouse.py`, `tests/__init__.py`, `tests/conftest.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces:
  - `config.Db` (frozen dataclass: `host: str, port: int, dbname: str, user: str, password: str`, method `conninfo() -> str` for Postgres)
  - `config.shop_db() -> Db` (env `SHOP_DB_HOST/PORT/NAME/USER/PASSWORD`, default `localhost:5433`, `shop`)
  - `config.warehouse() -> Db` (env `WAREHOUSE_HOST/PORT/USER/PASSWORD`, default `localhost:8123`, user `warehouse`)
  - `config.olist_dir() -> Path`, `config.catalog_path() -> Path`
  - `warehouse.connect() -> clickhouse_connect.driver.client.Client`
  - Test fixture `warehouse_client`: a ClickHouse client with the `bronze`, `silver` and `gold` databases dropped
  - Constant `FIXTURES: Path`

- [ ] **Step 1: Initialise git and the scaffold files**

```bash
cd /d/Labs/Repo/retail-data-pipeline
git init
```

`.gitignore`:
```gitignore
.venv/
__pycache__/
*.egg-info/
.pytest_cache/
data/
dbt/target/
dbt/logs/
dbt/.user.yml
```

`pyproject.toml`:
```toml
[project]
name = "retail-pipeline"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "psycopg[binary]>=3.2",
    "openpyxl>=3.1",
    "clickhouse-connect>=0.8",
    "dbt-clickhouse>=1.9,<2",
]

[project.optional-dependencies]
dev = ["pytest>=8"]

[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`docker-compose.yml` (Airflow is added in Task 10):
```yaml
x-clickhouse: &clickhouse
  image: clickhouse/clickhouse-server:25.8
  environment:
    CLICKHOUSE_USER: warehouse
    CLICKHOUSE_PASSWORD: warehouse
    CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
  ulimits:
    nofile: { soft: 262144, hard: 262144 }

services:
  shop-db:
    image: postgres:17
    environment:
      POSTGRES_USER: shop
      POSTGRES_PASSWORD: shop
      POSTGRES_DB: shop
    ports: ["5433:5432"]
    volumes: [shop-data:/var/lib/postgresql/data]

  warehouse:
    <<: *clickhouse
    ports: ["8123:8123"]
    volumes: [warehouse-data:/var/lib/clickhouse]

  # Throwaway warehouse for pytest; no volume, so it starts empty every time.
  warehouse-test:
    <<: *clickhouse
    ports: ["8124:8123"]

volumes:
  shop-data:
  warehouse-data:
```

`src/pipeline/__init__.py` and `tests/__init__.py`: empty files.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
docker compose up -d shop-db warehouse warehouse-test
```

- [ ] **Step 2: Write the test harness and failing tests**

`tests/conftest.py`:
```python
import os
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

# Point every test at throwaway stores so tests never touch dev data:
# the shop_test database, and the separate warehouse-test ClickHouse container.
os.environ["SHOP_DB_NAME"] = "shop_test"
os.environ["WAREHOUSE_PORT"] = "8124"

from pipeline import config  # noqa: E402
from pipeline.warehouse import connect  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "olist_mini"


def _recreate(db: config.Db) -> None:
    admin = replace(db, dbname="postgres")
    with psycopg.connect(admin.conninfo(), autocommit=True) as conn:
        name = sql.Identifier(db.dbname)
        conn.execute(sql.SQL("drop database if exists {} with (force)").format(name))
        conn.execute(sql.SQL("create database {}").format(name))


@pytest.fixture(scope="session", autouse=True)
def test_shop_database():
    _recreate(config.shop_db())


@pytest.fixture
def warehouse_client():
    client = connect()
    for database in ("bronze", "silver", "gold"):
        client.command(f"drop database if exists {database} sync")
    yield client
    client.close()
```

`tests/test_config.py`:
```python
import psycopg

from pipeline import config
from pipeline.warehouse import connect


def test_warehouse_defaults_to_local_compose_port(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_PORT", raising=False)
    assert config.warehouse().port == 8123


def test_env_overrides_shop_db(monkeypatch):
    monkeypatch.setenv("SHOP_DB_HOST", "shop-db")
    monkeypatch.setenv("SHOP_DB_PORT", "5432")
    db = config.shop_db()
    assert (db.host, db.port) == ("shop-db", 5432)


def test_shop_database_reachable():
    with psycopg.connect(config.shop_db().conninfo()) as conn:
        assert conn.execute("select 1").fetchone() == (1,)


def test_warehouse_reachable():
    client = connect()
    assert client.command("select 1") == 1
    client.close()
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_config.py -v`
Expected: ERROR, `ImportError: cannot import name 'config' from 'pipeline'`

- [ ] **Step 4: Implement config and the warehouse client**

`src/pipeline/config.py`:
```python
import os
from dataclasses import dataclass
from pathlib import Path

from psycopg.conninfo import make_conninfo


@dataclass(frozen=True)
class Db:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def conninfo(self) -> str:
        return make_conninfo(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password
        )


def shop_db() -> Db:
    return Db(
        host=os.getenv("SHOP_DB_HOST", "localhost"),
        port=int(os.getenv("SHOP_DB_PORT", "5433")),
        dbname=os.getenv("SHOP_DB_NAME", "shop"),
        user=os.getenv("SHOP_DB_USER", "shop"),
        password=os.getenv("SHOP_DB_PASSWORD", "shop"),
    )


def warehouse() -> Db:
    return Db(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "8123")),
        dbname="default",
        user=os.getenv("WAREHOUSE_USER", "warehouse"),
        password=os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
    )


def olist_dir() -> Path:
    return Path(os.getenv("OLIST_DIR", "data/olist/raw"))


def catalog_path() -> Path:
    return Path(os.getenv("CATALOG_PATH", "data/catalog/merchandising_catalog.xlsx"))
```

`src/pipeline/warehouse.py`:
```python
"""The ClickHouse warehouse that holds Bronze, Silver and Gold."""

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from pipeline import config


def connect() -> Client:
    db = config.warehouse()
    return clickhouse_connect.get_client(host=db.host, port=db.port, username=db.user, password=db.password)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_config.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add .gitignore pyproject.toml docker-compose.yml src tests CONTEXT.md CLAUDE.md docs .scratch
git commit -m "chore: scaffold pipeline package, Postgres shop and ClickHouse warehouse"
```

---

### Task 2: Source contract and Shop Database seeding

**Files:**
- Create: `src/pipeline/olist.py`, `src/pipeline/seed.py`, `tests/fixtures/olist_mini/*.csv`
- Modify: `tests/conftest.py` (append `seeded_shop` fixture)
- Test: `tests/test_seed_shop_db.py`

**Interfaces:**
- Consumes: `config.shop_db()`, `config.olist_dir()`
- Produces:
  - `olist.SourceSchemaError(Exception)`
  - `olist.SHOP_TABLES: dict[str, tuple[str, list[str]]]`: table name → (CSV file name, column list), with keys `orders`, `order_items`, `order_payments`, `customers`, `sellers`, `order_reviews`
  - `olist.CATALOG_SHEETS: dict[str, tuple[str, list[str]]]`: sheet name → (CSV file name, column list), with keys `products`, `category_translation`
  - `olist.check_header(path: Path, expected: list[str]) -> None`
  - `seed.seed_shop_db(olist_dir: Path) -> dict[str, int]`: row count per shop table
  - `seed.main(argv: list[str] | None = None) -> int`
  - Fixture `seeded_shop`

- [ ] **Step 1: Create the fixture dataset**

These files have exactly the shape of the real Olist files. `orders` is fully quoted, like Olist; the others are unquoted.

`tests/fixtures/olist_mini/olist_orders_dataset.csv`:
```csv
"order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
"o1","c1","delivered","2017-11-24 10:00:00","2017-11-24 10:15:00","2017-11-27 09:00:00","2017-12-01 14:00:00","2017-12-10 00:00:00"
"o2","c2","canceled","2017-11-24 11:00:00","","","","2017-12-12 00:00:00"
"o3","c3","delivered","2017-11-24 23:59:59","2017-11-25 08:00:00","2017-11-27 10:00:00","2017-12-15 16:00:00","2017-12-08 00:00:00"
"o4","c4","delivered","2017-11-25 00:00:00","2017-11-25 00:10:00","2017-11-28 09:00:00","2017-12-02 11:00:00","2017-12-14 00:00:00"
"o5","c5","unavailable","2017-11-24 12:00:00","2017-11-24 12:30:00","","","2017-12-20 00:00:00"
```

`tests/fixtures/olist_mini/olist_order_items_dataset.csv`:
```csv
order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value
o1,1,p1,s1,2017-11-30 10:00:00,100.00,10.00
o1,2,p2,s2,2017-11-30 10:00:00,50.00,5.00
o2,1,p1,s1,2017-11-30 11:00:00,30.00,3.00
o3,1,p3,s2,2017-12-01 00:00:00,20.00,2.00
o4,1,p1,s1,2017-12-01 00:10:00,70.00,7.00
```

`tests/fixtures/olist_mini/olist_order_payments_dataset.csv`:
```csv
order_id,payment_sequential,payment_type,payment_installments,payment_value
o1,1,voucher,1,15.00
o1,2,credit_card,3,150.00
o2,1,credit_card,1,33.00
o3,1,boleto,1,22.00
o4,1,credit_card,2,77.00
```

`tests/fixtures/olist_mini/olist_customers_dataset.csv` (c4 is the same person as c1):
```csv
customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state
c1,u1,01310,sao paulo,SP
c2,u2,22041,rio de janeiro,RJ
c3,u3,30130,belo horizonte,MG
c4,u1,01310,sao paulo,SP
c5,u5,80010,curitiba,PR
```

`tests/fixtures/olist_mini/olist_sellers_dataset.csv`:
```csv
seller_id,seller_zip_code_prefix,seller_city,seller_state
s1,13023,campinas,SP
s2,04195,sao paulo,SP
```

`tests/fixtures/olist_mini/olist_order_reviews_dataset.csv`:
```csv
review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp
r1,o1,5,,"Chegou antes do prazo, recomendo",2017-12-02 00:00:00,2017-12-03 10:00:00
```

`tests/fixtures/olist_mini/olist_products_dataset.csv` (p2 has no category; p3's category has no translation):
```csv
product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm
p1,brinquedos,40,287,1,225,16,10,14
p2,,,,,700,30,20,20
p3,pc_gamer,55,600,2,1500,40,10,30
```
(`lenght` is Olist's own spelling and must be kept.)

The real translation file starts with a UTF-8 BOM, so the fixture must too:
```bash
.venv/Scripts/python -c "open('tests/fixtures/olist_mini/product_category_name_translation.csv','w',encoding='utf-8-sig',newline='').write('product_category_name,product_category_name_english\nbrinquedos,toys\ncama_mesa_banho,bed_bath_table\n')"
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/conftest.py`:
```python
@pytest.fixture
def seeded_shop():
    from pipeline.seed import seed_shop_db

    seed_shop_db(FIXTURES)
```

`tests/test_seed_shop_db.py`:
```python
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
    orders.write_text(orders.read_text().replace('"order_status"', '"status"', 1))
    with pytest.raises(SourceSchemaError, match="olist_orders_dataset.csv"):
        seed_shop_db(source)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_seed_shop_db.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'pipeline.seed'`

- [ ] **Step 4: Implement the source contract**

`src/pipeline/olist.py`:
```python
"""The source contract: which Olist files exist and which columns each must have."""

import csv
from pathlib import Path


class SourceSchemaError(Exception):
    """A source file or sheet does not have the shape the pipeline expects."""


SHOP_TABLES: dict[str, tuple[str, list[str]]] = {
    "orders": (
        "olist_orders_dataset.csv",
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    ),
    "order_payments": (
        "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    ),
    "customers": (
        "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    ),
    "order_reviews": (
        "olist_order_reviews_dataset.csv",
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
    ),
}

CATALOG_SHEETS: dict[str, tuple[str, list[str]]] = {
    "products": (
        "olist_products_dataset.csv",
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        ["product_category_name", "product_category_name_english"],
    ),
}


def check_header(path: Path, expected: list[str]) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    with path.open(encoding="utf-8-sig", newline="") as f:
        actual = next(csv.reader(f), [])
    if actual != expected:
        raise SourceSchemaError(f"{path.name}: expected columns {expected}, got {actual}")
```

- [ ] **Step 5: Implement Shop Database seeding**

`src/pipeline/seed.py`:
```python
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
```

(The `catalog` target is added in Task 3.)

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_seed_shop_db.py -v`
Expected: 5 passed

- [ ] **Step 7: Commit**

```bash
git add src/pipeline/olist.py src/pipeline/seed.py tests
git commit -m "feat: seed the Shop Database from Olist CSVs with a checked source contract"
```

---

### Task 3: Merchandising Catalog builder

**Files:**
- Modify: `src/pipeline/seed.py` (add `build_catalog`, extend `main`)
- Modify: `tests/conftest.py` (append `catalog_file` fixture)
- Test: `tests/test_build_catalog.py`

**Interfaces:**
- Consumes: `olist.CATALOG_SHEETS`, `olist.check_header`
- Produces: `seed.build_catalog(olist_dir: Path, out_path: Path) -> None`, which writes an `.xlsx` with sheets `products` then `category_translation`. Header row = contract columns; data cells are strings, and empty cells are left blank. Fixture `catalog_file(tmp_path) -> Path`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/conftest.py`:
```python
@pytest.fixture
def catalog_file(tmp_path) -> Path:
    from pipeline.seed import build_catalog

    path = tmp_path / "merchandising_catalog.xlsx"
    build_catalog(FIXTURES, path)
    return path
```

`tests/test_build_catalog.py`:
```python
import shutil

import pytest
from openpyxl import load_workbook

from pipeline.olist import SourceSchemaError
from pipeline.seed import build_catalog
from tests.conftest import FIXTURES


def _rows(path, sheet):
    return [list(r) for r in load_workbook(path, read_only=True)[sheet].iter_rows(values_only=True)]


def test_catalog_has_one_sheet_per_catalog_table(catalog_file):
    assert load_workbook(catalog_file).sheetnames == ["products", "category_translation"]


def test_header_ignores_the_byte_order_mark(catalog_file):
    assert _rows(catalog_file, "category_translation")[0] == [
        "product_category_name",
        "product_category_name_english",
    ]


def test_values_are_copied_unmodified(catalog_file):
    products = _rows(catalog_file, "products")
    assert products[1] == ["p1", "brinquedos", "40", "287", "1", "225", "16", "10", "14"]
    assert products[2][:2] == ["p2", None]


def test_output_directory_is_created(tmp_path):
    out = tmp_path / "nested" / "catalog.xlsx"
    build_catalog(FIXTURES, out)
    assert out.exists()


def test_changed_columns_fail_the_build(tmp_path):
    source = tmp_path / "olist"
    shutil.copytree(FIXTURES, source)
    products = source / "olist_products_dataset.csv"
    products.write_text(products.read_text().replace("product_category_name", "category", 1))
    with pytest.raises(SourceSchemaError, match="olist_products_dataset.csv"):
        build_catalog(source, tmp_path / "catalog.xlsx")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_build_catalog.py -v`
Expected: ERROR, `ImportError: cannot import name 'build_catalog'`

- [ ] **Step 3: Implement `build_catalog`**

In `src/pipeline/seed.py`, add `import csv` and `from openpyxl import Workbook` to the imports, change the olist import to `from pipeline.olist import CATALOG_SHEETS, SHOP_TABLES, check_header`, and add after `seed_shop_db`:
```python
def build_catalog(olist_dir: Path, out_path: Path) -> None:
    for file_name, columns in CATALOG_SHEETS.values():
        check_header(olist_dir / file_name, columns)

    workbook = Workbook(write_only=True)
    for sheet, (file_name, _) in CATALOG_SHEETS.items():
        worksheet = workbook.create_sheet(sheet)
        with (olist_dir / file_name).open(encoding="utf-8-sig", newline="") as f:
            for row in csv.reader(f):
                worksheet.append([value if value != "" else None for value in row])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(out_path)
```

Replace `main` with:
```python
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.seed")
    parser.add_argument("target", choices=["shop-db", "catalog"])
    args = parser.parse_args(argv)
    if args.target == "shop-db":
        print(seed_shop_db(config.olist_dir()))
    else:
        build_catalog(config.olist_dir(), config.catalog_path())
        print(f"wrote {config.catalog_path()}")
    return 0
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/seed.py tests
git commit -m "feat: generate the Merchandising Catalog workbook from Olist"
```

---

### Task 4: Extract and load the Shop Database into Bronze

**Files:**
- Create: `src/pipeline/bronze.py`
- Test: `tests/test_bronze_shop.py`

**Interfaces:**
- Consumes: `config.shop_db()`, `warehouse.connect()`, `olist.SHOP_TABLES`, `olist.CATALOG_SHEETS`
- Produces:
  - `bronze.SHOP_EXTRACTS: dict[str, str]`, with keys `orders`, `order_items`, `order_payments` (Postgres queries taking `%(d)s`)
  - `bronze.ensure_bronze_tables(client: Client) -> None`
  - `bronze.extract_shop(business_date: date) -> dict[str, int]`: rows loaded per table
  - ClickHouse tables `bronze.orders`, `bronze.order_items` and `bronze.order_payments` (contract columns as `Nullable(String)`, plus `_business_date Date`, `_loaded_at DateTime64(6, 'UTC')`, `partition by toYYYYMM(_business_date)`), and `bronze.products` and `bronze.category_translation` (contract columns as `Nullable(String)`, plus `_source_checksum String`, `_loaded_at DateTime64(6, 'UTC')`)

- [ ] **Step 1: Write the failing tests**

`tests/test_bronze_shop.py`:
```python
from datetime import date

from pipeline.bronze import extract_shop

D = date(2017, 11, 24)


def _rows(client, query, **params):
    return client.query(query, parameters=params).result_rows


def test_extracts_only_orders_purchased_on_the_business_date(seeded_shop, warehouse_client):
    assert extract_shop(D) == {"orders": 4, "order_items": 4, "order_payments": 4}
    ids = {r[0] for r in _rows(warehouse_client, "select order_id from bronze.orders")}
    # o3 at 23:59:59 belongs to D; o4 at 00:00:00 the next day does not.
    assert ids == {"o1", "o2", "o3", "o5"}


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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_bronze_shop.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'pipeline.bronze'`

- [ ] **Step 3: Implement shop extraction**

`src/pipeline/bronze.py`:
```python
"""Bronze: extract each source and load an unmodified, text-typed copy into the warehouse."""

from datetime import date, datetime, timezone

import psycopg
from clickhouse_connect.driver.client import Client

from pipeline import config
from pipeline.olist import CATALOG_SHEETS, SHOP_TABLES
from pipeline.warehouse import connect


def _select_list(table: str) -> str:
    return ", ".join(f"x.{c}::text" for c in SHOP_TABLES[table][1])


SHOP_EXTRACTS: dict[str, str] = {
    "orders": f"select {_select_list('orders')} from shop.orders x "
    "where x.order_purchase_timestamp::date = %(d)s",
    "order_items": f"select {_select_list('order_items')} from shop.order_items x "
    "join shop.orders o on o.order_id = x.order_id where o.order_purchase_timestamp::date = %(d)s",
    "order_payments": f"select {_select_list('order_payments')} from shop.order_payments x "
    "join shop.orders o on o.order_id = x.order_id where o.order_purchase_timestamp::date = %(d)s",
}

_SHOP_TABLE_DDL = """
create table if not exists bronze.{table} (
    {columns},
    _business_date Date,
    _loaded_at DateTime64(6, 'UTC')
)
engine = MergeTree
partition by toYYYYMM(_business_date)
order by (_business_date, _loaded_at)
"""

_CATALOG_TABLE_DDL = """
create table if not exists bronze.{table} (
    {columns},
    _source_checksum String,
    _loaded_at DateTime64(6, 'UTC')
)
engine = MergeTree
order by _loaded_at
"""


def _text_columns(columns: list[str]) -> str:
    return ",\n    ".join(f"{c} Nullable(String)" for c in columns)


def ensure_bronze_tables(client: Client) -> None:
    client.command("create database if not exists bronze")
    for table in SHOP_EXTRACTS:
        client.command(_SHOP_TABLE_DDL.format(table=table, columns=_text_columns(SHOP_TABLES[table][1])))
    for sheet, (_, columns) in CATALOG_SHEETS.items():
        client.command(_CATALOG_TABLE_DDL.format(table=sheet, columns=_text_columns(columns)))


def extract_shop(business_date: date) -> dict[str, int]:
    # ClickHouse has no multi-table transactions: each table's insert is atomic on its own,
    # and Silver reads each table's latest load independently.
    loaded_at = datetime.now(timezone.utc)
    client = connect()
    try:
        ensure_bronze_tables(client)
        counts = {}
        with psycopg.connect(config.shop_db().conninfo()) as src:
            for table, query in SHOP_EXTRACTS.items():
                rows = src.execute(query, {"d": business_date}).fetchall()
                if rows:
                    client.insert(
                        table,
                        [(*row, business_date, loaded_at) for row in rows],
                        column_names=[*SHOP_TABLES[table][1], "_business_date", "_loaded_at"],
                        database="bronze",
                    )
                counts[table] = len(rows)
        return counts
    finally:
        client.close()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_bronze_shop.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/bronze.py tests/test_bronze_shop.py
git commit -m "feat: extract a Business Date of Orders, Order Items and Payments into ClickHouse Bronze"
```

---

### Task 5: Extract and load the Merchandising Catalog into Bronze

**Files:**
- Modify: `src/pipeline/bronze.py` (add `extract_catalog`)
- Test: `tests/test_bronze_catalog.py`

**Interfaces:**
- Consumes: `bronze.ensure_bronze_tables`, `warehouse.connect()`, `olist.CATALOG_SHEETS`, `olist.SourceSchemaError`
- Produces: `bronze.extract_catalog(path: Path) -> int | None`: rows loaded, or `None` when a file with the same SHA-256 checksum is already in Bronze

- [ ] **Step 1: Write the failing tests**

`tests/test_bronze_catalog.py`:
```python
import pytest
from openpyxl import Workbook, load_workbook

from pipeline.bronze import extract_catalog
from pipeline.olist import SourceSchemaError


def _rows(client, query):
    return client.query(query).result_rows


def test_loads_every_sheet_with_its_checksum(catalog_file, warehouse_client):
    assert extract_catalog(catalog_file) == 5  # 3 products + 2 translations
    assert _rows(
        warehouse_client,
        "select product_category_name, length(_source_checksum) from bronze.products where product_id = 'p1'",
    ) == [("brinquedos", 64)]


def test_unchanged_catalog_is_not_reloaded(catalog_file, warehouse_client):
    extract_catalog(catalog_file)
    assert extract_catalog(catalog_file) is None
    assert _rows(warehouse_client, "select count() from bronze.products") == [(3,)]


def test_edited_catalog_is_loaded_again(catalog_file, warehouse_client):
    extract_catalog(catalog_file)
    workbook = load_workbook(catalog_file)
    workbook["products"]["B3"] = "brinquedos"  # p2 gets a category
    workbook.save(catalog_file)
    assert extract_catalog(catalog_file) == 5
    assert _rows(warehouse_client, "select uniqExact(_loaded_at) from bronze.products") == [(2,)]


def test_blank_rows_are_ignored(catalog_file, warehouse_client):
    workbook = load_workbook(catalog_file)
    workbook["products"].append([None] * 9)
    workbook.save(catalog_file)
    assert extract_catalog(catalog_file) == 5


def test_numbers_typed_into_excel_are_stored_as_text(catalog_file, warehouse_client):
    workbook = load_workbook(catalog_file)
    workbook["products"]["F2"] = 225  # a number, as a person editing Excel would type it
    workbook.save(catalog_file)
    extract_catalog(catalog_file)
    assert _rows(
        warehouse_client, "select product_weight_g from bronze.products where product_id = 'p1'"
    ) == [("225",)]


def test_missing_catalog_fails_the_run(tmp_path, warehouse_client):
    with pytest.raises(FileNotFoundError, match="Merchandising Catalog"):
        extract_catalog(tmp_path / "missing.xlsx")


def test_missing_sheet_fails_the_run(tmp_path, warehouse_client):
    path = tmp_path / "catalog.xlsx"
    workbook = Workbook()
    workbook.active.title = "products"
    workbook.save(path)
    with pytest.raises(SourceSchemaError, match="category_translation"):
        extract_catalog(path)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_bronze_catalog.py -v`
Expected: ERROR, `ImportError: cannot import name 'extract_catalog'`

- [ ] **Step 3: Implement catalog extraction**

In `src/pipeline/bronze.py`, add `import hashlib`, `from pathlib import Path` and `from openpyxl import load_workbook` to the imports, change the olist import to `from pipeline.olist import CATALOG_SHEETS, SHOP_TABLES, SourceSchemaError`, and append:
```python
def _read_catalog(path: Path) -> dict[str, list[tuple]]:
    workbook = load_workbook(path, read_only=True)
    missing = [sheet for sheet in CATALOG_SHEETS if sheet not in workbook.sheetnames]
    if missing:
        raise SourceSchemaError(f"{path.name}: missing sheets {missing}")
    sheets = {}
    for sheet, (_, columns) in CATALOG_SHEETS.items():
        rows = workbook[sheet].iter_rows(values_only=True)
        header = list(next(rows, ()))
        if header != columns:
            raise SourceSchemaError(f"{path.name}[{sheet}]: expected columns {columns}, got {header}")
        sheets[sheet] = [
            tuple(None if v is None else str(v) for v in row[: len(columns)])
            for row in rows
            if any(v is not None for v in row)
        ]
    workbook.close()
    return sheets


def extract_catalog(path: Path) -> int | None:
    if not path.exists():
        raise FileNotFoundError(f"Merchandising Catalog not found: {path}")
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    sheets = _read_catalog(path)

    loaded_at = datetime.now(timezone.utc)
    client = connect()
    try:
        ensure_bronze_tables(client)
        already_loaded = client.query(
            "select 1 from bronze.products where _source_checksum = {c:String} limit 1",
            parameters={"c": checksum},
        ).result_rows
        if already_loaded:
            return None
        # Two parallel runs may both load a new file; Silver reads only the latest load, so that is harmless.
        for sheet, rows in sheets.items():
            if rows:
                client.insert(
                    sheet,
                    [(*row, checksum, loaded_at) for row in rows],
                    column_names=[*CATALOG_SHEETS[sheet][1], "_source_checksum", "_loaded_at"],
                    database="bronze",
                )
        return sum(len(rows) for rows in sheets.values())
    finally:
        client.close()
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/bronze.py tests/test_bronze_catalog.py
git commit -m "feat: load the Merchandising Catalog into Bronze, skipping unchanged files"
```

---

### Task 6: Run CLI, dbt project, and Silver Orders with Quarantine

**Files:**
- Create: `src/pipeline/run.py`, `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/macros/generate_schema_name.sql`, `dbt/macros/business_date.sql`, `dbt/macros/latest_load.sql`, `dbt/models/sources.yml`, `dbt/models/silver/_checks/orders_checked.sql`, `dbt/models/silver/orders.sql`, `dbt/models/silver/quarantine.sql`, `dbt/models/silver/_silver.yml`
- Modify: `tests/conftest.py` (append `BUSINESS_DATE`, `bronze_loaded`, `add_bronze_rows`)
- Test: `tests/test_silver_orders.py`

**Interfaces:**
- Consumes: `bronze.extract_shop`, `bronze.extract_catalog`, `config.catalog_path()`
- Produces:
  - `run.STEPS = ("bronze", "silver", "facts", "dimensions")`
  - `run.DBT_SELECTORS: dict[str, str]`
  - `run.DBT_DIR: Path`
  - `run.dbt_executable() -> Path`
  - `run.run_step(step: str, business_date: date) -> None`, which raises `subprocess.CalledProcessError` when dbt fails
  - `run.main(argv: list[str] | None = None) -> int`, with CLI `--business-date YYYY-MM-DD --step {bronze,silver,facts,dimensions,all}`
  - dbt macros `business_date()` (renders `toDate('YYYY-MM-DD')`), `delete_business_date()`, `latest_bronze_load(table)`, `latest_catalog_load(table)`
  - Ephemeral `orders_checked` with columns `raw_order_id, raw_customer_id, raw_order_status, purchased_at, estimated_delivery_at, delivered_at, loaded_at, occurrence, failure_reason`
  - Table `silver.orders(business_date, order_id, customer_id, order_status, purchased_at, estimated_delivery_at, delivered_at)`
  - Table `silver.quarantine(business_date, entity, record_key, failure_reason, loaded_at)`
  - Test fixtures `bronze_loaded` (shop seeded, catalog built, bronze step run for 2017-11-24; returns `warehouse_client`) and `add_bronze_rows(table: str, rows: list[dict], business_date: date = BUSINESS_DATE)`, which inserts into the latest load

- [ ] **Step 1: Write the failing tests**

Append to `tests/conftest.py`:
```python
from datetime import date  # noqa: E402

BUSINESS_DATE = date(2017, 11, 24)


@pytest.fixture
def bronze_loaded(seeded_shop, catalog_file, warehouse_client, monkeypatch):
    from pipeline.run import run_step

    monkeypatch.setenv("CATALOG_PATH", str(catalog_file))
    run_step("bronze", BUSINESS_DATE)
    return warehouse_client


@pytest.fixture
def add_bronze_rows(warehouse_client):
    """Insert extra rows into the latest Bronze load, simulating bad source data."""

    def add(table: str, rows: list[dict], business_date: date = BUSINESS_DATE) -> None:
        loaded_at = warehouse_client.query(
            f"select max(_loaded_at) from bronze.{table} where _business_date = {{d:Date}}",
            parameters={"d": business_date},
        ).result_rows[0][0]
        for row in rows:
            warehouse_client.insert(
                table,
                [(*row.values(), business_date, loaded_at)],
                column_names=[*row, "_business_date", "_loaded_at"],
                database="bronze",
            )

    return add
```

`tests/test_silver_orders.py`:
```python
import subprocess

import pytest

from pipeline.run import DBT_DIR, dbt_executable, main, run_step
from tests.conftest import BUSINESS_DATE as D


def _orders(client):
    return client.query(
        "select order_id, order_status, toString(purchased_at) from silver.orders order by order_id"
    ).result_rows


def _quarantine(client):
    return client.query(
        "select entity, record_key, failure_reason from silver.quarantine "
        "where business_date = {d:Date} order by record_key, failure_reason",
        parameters={"d": D},
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
    assert _quarantine(bronze_loaded) == []


def test_bad_orders_go_to_quarantine(bronze_loaded, add_bronze_rows):
    add_bronze_rows(
        "orders",
        [
            _order("o9", purchased="not-a-date"),
            _order("o8", status="teleported"),
            _order("o1", purchased="2017-11-24 10:00:00"),
        ],
    )
    run_step("silver", D)
    assert _quarantine(bronze_loaded) == [
        ("order", "o1", "duplicate order_id"),
        ("order", "o8", "unknown order_status"),
        ("order", "o9", "invalid order_purchase_timestamp"),
    ]
    assert [r[0] for r in _orders(bronze_loaded)] == ["o1", "o2", "o3", "o5"]


def test_rerun_after_source_fix_clears_quarantine(bronze_loaded, add_bronze_rows):
    add_bronze_rows("orders", [_order("o9", purchased="bad")])
    run_step("silver", D)
    assert len(_quarantine(bronze_loaded)) == 1

    run_step("bronze", D)  # a new, clean load of the same Business Date
    run_step("silver", D)
    assert _quarantine(bronze_loaded) == []


def test_rerun_replaces_the_business_date(bronze_loaded):
    run_step("silver", D)
    run_step("silver", D)
    assert len(_orders(bronze_loaded)) == 4


def test_cli_rejects_a_malformed_business_date():
    with pytest.raises(SystemExit):
        main(["--business-date", "2017-13-40", "--step", "silver"])


def test_dbt_refuses_to_run_without_a_business_date(bronze_loaded):
    result = subprocess.run(
        [str(dbt_executable()), "build", "--select", "path:models/silver",
         "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "business_date" in result.stdout
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_silver_orders.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'pipeline.run'`

- [ ] **Step 3: Implement the run CLI**

`src/pipeline/run.py`:
```python
"""Run one pipeline step for one Business Date."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

from pipeline import config
from pipeline.bronze import extract_catalog, extract_shop

STEPS = ("bronze", "silver", "facts", "dimensions")
DBT_SELECTORS = {
    "silver": "path:models/silver",
    "facts": "path:models/gold/facts",
    # Dimensions and report views are rebuilt whole, so only one run may do this at a time.
    "dimensions": "path:models/gold/dimensions path:models/gold/reports",
}
DBT_DIR = Path(os.getenv("DBT_DIR", Path(__file__).resolve().parents[2] / "dbt"))


def dbt_executable() -> Path:
    """dbt is installed next to the Python running this code (venv Scripts/ or bin/)."""
    return Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")


def _dbt_build(selector: str, business_date: date) -> None:
    # A private target/log dir per run lets several Business Dates run in parallel.
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            [
                str(dbt_executable()), "build",
                "--select", selector,
                "--vars", json.dumps({"business_date": business_date.isoformat()}),
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
                "--target-path", str(Path(tmp) / "target"),
                "--log-path", str(Path(tmp) / "logs"),
            ],
            check=True,
        )


def run_step(step: str, business_date: date) -> None:
    if step == "bronze":
        extract_catalog(config.catalog_path())
        extract_shop(business_date)
    elif step in DBT_SELECTORS:
        _dbt_build(DBT_SELECTORS[step], business_date)
    else:
        raise ValueError(f"unknown step {step!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.run")
    parser.add_argument("--business-date", required=True, type=date.fromisoformat)
    parser.add_argument("--step", required=True, choices=[*STEPS, "all"])
    args = parser.parse_args(argv)
    for step in STEPS if args.step == "all" else (args.step,):
        run_step(step, args.business_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Create the dbt project**

`dbt/dbt_project.yml`:
```yaml
name: retail
version: "1.0.0"
profile: retail
config-version: 2

model-paths: ["models"]
macro-paths: ["macros"]
test-paths: ["tests"]

models:
  retail:
    silver:
      +schema: silver
      +materialized: incremental
      +incremental_strategy: append
      +engine: MergeTree()
      +partition_by: business_date
      +pre-hook: "{{ delete_business_date() }}"
    gold:
      +schema: gold
      facts:
        +materialized: incremental
        +incremental_strategy: append
        +engine: MergeTree()
        +partition_by: business_date
        +pre-hook: "{{ delete_business_date() }}"
```

`dbt/profiles.yml`:
```yaml
retail:
  target: dev
  outputs:
    dev:
      type: clickhouse
      driver: http
      host: "{{ env_var('WAREHOUSE_HOST', 'localhost') }}"
      port: "{{ env_var('WAREHOUSE_PORT', '8123') | int }}"
      user: "{{ env_var('WAREHOUSE_USER', 'warehouse') }}"
      password: "{{ env_var('WAREHOUSE_PASSWORD', 'warehouse') }}"
      schema: default
      threads: 4
      custom_settings:
        # LEFT JOIN misses must be NULL, not '' or 0, or Silver checks pass bad rows.
        join_use_nulls: 1
```

`dbt/macros/generate_schema_name.sql` (so models land in the `silver`/`gold` databases, not `default_silver`):
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
```

`dbt/macros/business_date.sql`:
```sql
{% macro business_date() -%}
    {%- set value = var('business_date', none) -%}
    {%- if execute and value is none -%}
        {{ exceptions.raise_compiler_error("Missing Business Date: pass --vars '{business_date: YYYY-MM-DD}'") }}
    {%- endif -%}
    toDate('{{ value }}')
{%- endmacro %}

{# Pre-hook: a rerun replaces its Business Date's partition, even if it now produces no rows. #}
{% macro delete_business_date() -%}
    {%- if is_incremental() -%}
        alter table {{ this }} drop partition tuple({{ business_date() }})
    {%- endif -%}
{%- endmacro %}
```

`dbt/macros/latest_load.sql`:
```sql
{% macro latest_bronze_load(table) -%}
    select *
    from {{ source('bronze', table) }}
    where _business_date = {{ business_date() }}
      and _loaded_at = (
          select max(_loaded_at) from {{ source('bronze', table) }}
          where _business_date = {{ business_date() }}
      )
{%- endmacro %}

{% macro latest_catalog_load(table) -%}
    select *
    from {{ source('bronze', table) }}
    where _loaded_at = (select max(_loaded_at) from {{ source('bronze', table) }})
{%- endmacro %}
```

`dbt/models/sources.yml`:
```yaml
version: 2

sources:
  - name: bronze
    schema: bronze
    tables:
      - name: orders
      - name: order_items
      - name: order_payments
      - name: products
      - name: category_translation
```

- [ ] **Step 5: Create Silver Orders and the Quarantine**

`dbt/models/silver/_checks/orders_checked.sql`:
```sql
{{ config(materialized='ephemeral') }}

-- Source text is exposed as raw_*; never alias an expression to an input column name (ClickHouse).
with orders_src as (
    {{ latest_bronze_load('orders') }}
),

orders_typed as (
    select
        order_id as raw_order_id,
        customer_id as raw_customer_id,
        order_status as raw_order_status,
        toDateTimeOrNull(order_purchase_timestamp) as purchased_at,
        toDateTimeOrNull(order_estimated_delivery_date) as estimated_delivery_at,
        toDateTimeOrNull(order_delivered_customer_date) as delivered_at,
        _loaded_at as loaded_at,
        row_number() over (partition by order_id order by order_status, order_purchase_timestamp) as occurrence
    from orders_src
)

select
    *,
    case
        when raw_order_id is null then 'missing order_id'
        when occurrence > 1 then 'duplicate order_id'
        when raw_customer_id is null then 'missing customer_id'
        when purchased_at is null then 'invalid order_purchase_timestamp'
        when raw_order_status is null then 'missing order_status'
        when raw_order_status not in (
            'created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable'
        ) then 'unknown order_status'
    end as failure_reason
from orders_typed
```

`dbt/models/silver/orders.sql` (`purchased_at` keeps its Nullable type; `assumeNotNull(purchased_at) as purchased_at` would break the alias rule):
```sql
{{ config(order_by='(business_date, order_id)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(raw_customer_id) as customer_id,
    assumeNotNull(raw_order_status) as order_status,
    purchased_at,
    estimated_delivery_at,
    delivered_at
from {{ ref('orders_checked') }}
where failure_reason is null
```

`dbt/models/silver/quarantine.sql`:
```sql
{{ config(order_by='(business_date, entity, record_key)') }}

-- The full rejected record stays in Bronze; (entity, record_key, loaded_at) points to it.
select
    {{ business_date() }} as business_date,
    'order' as entity,
    ifNull(raw_order_id, '(missing)') as record_key,
    failure_reason,
    loaded_at
from {{ ref('orders_checked') }}
where failure_reason is not null
```

`dbt/models/silver/_silver.yml`:
```yaml
version: 2

models:
  - name: orders
    columns:
      - name: business_date
        data_tests: [not_null]
      - name: order_id
        data_tests: [not_null, unique]
  - name: quarantine
    columns:
      - name: failure_reason
        data_tests: [not_null]
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_silver_orders.py -v`
Expected: 6 passed. In `test_bad_orders_go_to_quarantine` the duplicate `o1` rows differ only in optional columns. Which one is kept doesn't matter: the test only checks that exactly one `o1` reaches Silver and one is quarantined.

If dbt reports a cyclic alias or an "ambiguous column" error, look for an expression aliased to a name that already exists in its input (see Global Constraints) and rename it.

- [ ] **Step 7: Commit**

```bash
git add src/pipeline/run.py dbt tests
git commit -m "feat: Silver Orders with Quarantine in ClickHouse, rerunnable per Business Date"
```

---

### Task 7: Silver Order Items, Payments and catalog lookups

**Files:**
- Create: `dbt/models/silver/products.sql`, `dbt/models/silver/product_categories.sql`, `dbt/models/silver/_checks/order_items_checked.sql`, `dbt/models/silver/_checks/payments_checked.sql`, `dbt/models/silver/order_items.sql`, `dbt/models/silver/payments.sql`, `dbt/tests/silver_order_items_unique.sql`
- Modify: `dbt/models/silver/quarantine.sql`, `dbt/models/silver/_silver.yml`
- Test: `tests/test_silver_order_items.py`

**Interfaces:**
- Consumes: `orders_checked`, the macros from Task 6, fixtures `bronze_loaded` and `add_bronze_rows`
- Produces:
  - Ephemeral `products(product_id, product_category_name)` and `product_categories(product_category_name, product_category_name_english)` from the latest catalog load. They are ephemeral so that parallel runs never issue DDL on them.
  - Table `silver.order_items(business_date, order_id, order_item_id Int32, product_id, seller_id, price Decimal(18,2), freight Decimal(18,2))`
  - Table `silver.payments(business_date, order_id, payment_sequential Int32, payment_type, payment_installments, payment_value Decimal(18,2))`
  - Quarantine entities `order_item` (key `order_id/order_item_id`) and `payment` (key `order_id/payment_sequential`)

- [ ] **Step 1: Write the failing tests**

`tests/test_silver_order_items.py`:
```python
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


def test_valid_payments_reach_silver(bronze_loaded):
    run_step("silver", D)
    assert _rows(
        bronze_loaded,
        "select order_id, payment_sequential, payment_type, payment_value from silver.payments "
        "order by order_id, payment_sequential",
    ) == [
        ("o1", 1, "voucher", Decimal("15.00")),
        ("o1", 2, "credit_card", Decimal("150.00")),
        ("o2", 1, "credit_card", Decimal("33.00")),
        ("o3", 1, "boleto", Decimal("22.00")),
    ]


def test_bad_order_items_go_to_quarantine(bronze_loaded, add_bronze_rows):
    add_bronze_rows("orders", [
        {"order_id": "o9", "customer_id": "c1", "order_status": "delivered", "order_purchase_timestamp": "bad"},
    ])
    add_bronze_rows("order_items", [
        _item("o1", "3", "p404", "12.00"),
        _item("o3", "2", "p1", "-5.00"),
        _item("o3", "3", "p1", "abc"),
        _item("o3", "4", "p1", "9.00", freight="-1.00"),
        _item("o9", "1", "p1", "9.00"),
        _item("o77", "1", "p1", "9.00"),
        _item("o1", "1", "p1", "100.00"),
    ])
    run_step("silver", D)
    assert _rows(
        bronze_loaded,
        "select record_key, failure_reason from silver.quarantine where entity = 'order_item' order by record_key",
    ) == [
        ("o1/1", "duplicate order item"),
        ("o1/3", "unknown product"),
        ("o3/2", "negative price"),
        ("o3/3", "invalid price"),
        ("o3/4", "negative freight_value"),
        ("o77/1", "unknown order"),
        ("o9/1", "parent order quarantined"),
    ]
    assert _rows(bronze_loaded, "select count() from silver.order_items") == [(4,)]


def test_bad_payments_go_to_quarantine(bronze_loaded, add_bronze_rows):
    add_bronze_rows("order_payments", [
        {"order_id": "o3", "payment_sequential": "2", "payment_type": "voucher", "payment_installments": "1", "payment_value": "-3.00"},
        {"order_id": "o3", "payment_sequential": "x", "payment_type": "voucher", "payment_installments": "1", "payment_value": "3.00"},
    ])
    run_step("silver", D)
    assert _rows(
        bronze_loaded,
        "select record_key, failure_reason from silver.quarantine where entity = 'payment' order by record_key",
    ) == [("o3/2", "negative payment_value"), ("o3/x", "invalid payment_sequential")]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_silver_order_items.py -v`
Expected: FAIL, ClickHouse error `Table silver.order_items does not exist` (UNKNOWN_TABLE)

- [ ] **Step 3: Create the catalog lookups**

`dbt/models/silver/products.sql`:
```sql
{{ config(materialized='ephemeral') }}

with products_src as (
    {{ latest_catalog_load('products') }}
),

products_ranked as (
    select
        assumeNotNull(product_id) as catalog_product_id,
        nullIf(trimBoth(product_category_name), '') as category_name,
        row_number() over (partition by product_id order by product_category_name) as occurrence
    from products_src
    where product_id is not null
)

select
    catalog_product_id as product_id,
    category_name as product_category_name
from products_ranked
where occurrence = 1
```

`dbt/models/silver/product_categories.sql`:
```sql
{{ config(materialized='ephemeral') }}

with categories_src as (
    {{ latest_catalog_load('category_translation') }}
),

categories_ranked as (
    select
        product_category_name as category_name,
        nullIf(trimBoth(product_category_name_english), '') as english_name,
        row_number() over (partition by product_category_name order by product_category_name_english) as occurrence
    from categories_src
    where product_category_name is not null
)

select
    category_name as product_category_name,
    english_name as product_category_name_english
from categories_ranked
where occurrence = 1
```

- [ ] **Step 4: Create the checks**

`dbt/models/silver/_checks/order_items_checked.sql`:
```sql
{{ config(materialized='ephemeral') }}

with items_src as (
    {{ latest_bronze_load('order_items') }}
),

items_typed as (
    select
        order_id as raw_order_id,
        order_item_id as raw_order_item_id,
        product_id as raw_product_id,
        seller_id as raw_seller_id,
        toInt32OrNull(order_item_id) as item_number,
        toDecimal64OrNull(price, 2) as price_amount,
        toDecimal64OrNull(freight_value, 2) as freight_amount,
        _loaded_at as loaded_at,
        row_number() over (partition by order_id, order_item_id order by product_id, price) as occurrence
    from items_src
),

item_parent_orders as (
    select raw_order_id as parent_order_id, failure_reason as parent_failure_reason
    from {{ ref('orders_checked') }}
    where occurrence = 1
)

select
    t.*,
    case
        when t.raw_order_id is null then 'missing order_id'
        when t.item_number is null then 'invalid order_item_id'
        when t.occurrence > 1 then 'duplicate order item'
        when o.parent_order_id is null then 'unknown order'
        when o.parent_failure_reason is not null then 'parent order quarantined'
        when t.raw_product_id is null then 'missing product_id'
        when p.product_id is null then 'unknown product'
        when t.price_amount is null then 'invalid price'
        when t.price_amount < 0 then 'negative price'
        when t.freight_amount is null then 'invalid freight_value'
        when t.freight_amount < 0 then 'negative freight_value'
    end as failure_reason
from items_typed t
left join item_parent_orders o on o.parent_order_id = t.raw_order_id
left join {{ ref('products') }} p on p.product_id = t.raw_product_id
```

`dbt/models/silver/_checks/payments_checked.sql`:
```sql
{{ config(materialized='ephemeral') }}

with payments_src as (
    {{ latest_bronze_load('order_payments') }}
),

payments_typed as (
    select
        order_id as raw_order_id,
        payment_sequential as raw_payment_sequential,
        payment_type as raw_payment_type,
        toInt32OrNull(payment_sequential) as sequence_number,
        toInt32OrNull(payment_installments) as installments,
        toDecimal64OrNull(payment_value, 2) as amount,
        _loaded_at as loaded_at,
        row_number() over (partition by order_id, payment_sequential order by payment_type, payment_value) as occurrence
    from payments_src
),

payment_parent_orders as (
    select raw_order_id as parent_order_id, failure_reason as parent_failure_reason
    from {{ ref('orders_checked') }}
    where occurrence = 1
)

select
    t.*,
    case
        when t.raw_order_id is null then 'missing order_id'
        when t.sequence_number is null then 'invalid payment_sequential'
        when t.occurrence > 1 then 'duplicate payment'
        when o.parent_order_id is null then 'unknown order'
        when o.parent_failure_reason is not null then 'parent order quarantined'
        when t.amount is null then 'invalid payment_value'
        when t.amount < 0 then 'negative payment_value'
    end as failure_reason
from payments_typed t
left join payment_parent_orders o on o.parent_order_id = t.raw_order_id
```

- [ ] **Step 5: Create the Silver tables and extend the Quarantine**

`dbt/models/silver/order_items.sql`:
```sql
{{ config(order_by='(business_date, order_id, order_item_id)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(item_number) as order_item_id,
    assumeNotNull(raw_product_id) as product_id,
    raw_seller_id as seller_id,
    assumeNotNull(price_amount) as price,
    assumeNotNull(freight_amount) as freight
from {{ ref('order_items_checked') }}
where failure_reason is null
```

`dbt/models/silver/payments.sql`:
```sql
{{ config(order_by='(business_date, order_id, payment_sequential)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(sequence_number) as payment_sequential,
    raw_payment_type as payment_type,
    installments as payment_installments,
    assumeNotNull(amount) as payment_value
from {{ ref('payments_checked') }}
where failure_reason is null
```

Replace `dbt/models/silver/quarantine.sql` with:
```sql
{{ config(order_by='(business_date, entity, record_key)') }}

-- The full rejected record stays in Bronze; (entity, record_key, loaded_at) points to it.
select
    {{ business_date() }} as business_date,
    'order' as entity,
    ifNull(raw_order_id, '(missing)') as record_key,
    failure_reason,
    loaded_at
from {{ ref('orders_checked') }}
where failure_reason is not null

union all

select
    {{ business_date() }},
    'order_item',
    concat(ifNull(raw_order_id, '?'), '/', ifNull(raw_order_item_id, '?')),
    failure_reason,
    loaded_at
from {{ ref('order_items_checked') }}
where failure_reason is not null

union all

select
    {{ business_date() }},
    'payment',
    concat(ifNull(raw_order_id, '?'), '/', ifNull(raw_payment_sequential, '?')),
    failure_reason,
    loaded_at
from {{ ref('payments_checked') }}
where failure_reason is not null
```

Append to `dbt/models/silver/_silver.yml` under `models:`:
```yaml
  - name: order_items
    columns:
      - name: order_id
        data_tests: [not_null]
      - name: product_id
        data_tests: [not_null]
  - name: payments
    columns:
      - name: order_id
        data_tests: [not_null]
```

`dbt/tests/silver_order_items_unique.sql` (ClickHouse doesn't enforce keys, so this test is the guard):
```sql
select order_id, order_item_id
from {{ ref('order_items') }}
group by order_id, order_item_id
having count() > 1
```

- [ ] **Step 6: Run all tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all passed. If `unknown order` or `unknown product` rows end up in Silver instead of Quarantine, `join_use_nulls` is not reaching dbt; check `dbt/profiles.yml`.

- [ ] **Step 7: Commit**

```bash
git add dbt tests/test_silver_order_items.py
git commit -m "feat: Silver Order Items and Payments with Quarantine checks"
```

---

### Task 8: Gold star schema and the daily Revenue view

**Files:**
- Create: `dbt/models/gold/facts/fact_order_items.sql`, `dbt/models/gold/dimensions/dim_product.sql`, `dbt/models/gold/dimensions/dim_date.sql`, `dbt/models/gold/reports/daily_revenue_by_category.sql`, `dbt/models/gold/_gold.yml`, `dbt/tests/gold_fact_order_items_unique.sql`, `dbt/tests/gold_daily_revenue_unique.sql`
- Test: `tests/test_gold_star.py`

**Interfaces:**
- Consumes: `silver.orders`, `silver.order_items`, ephemeral `products` and `product_categories`
- Produces (ADR 0004):
  - Fact `gold.fact_order_items(business_date, order_id, order_item_id, product_id, seller_id, order_status, price, freight, counts_as_revenue UInt8)`: incremental, partitioned by business_date, one row per Order Item
  - Dimension `gold.dim_product(product_id, product_category_name, product_category)`: table, rebuilt from the latest catalog
  - Dimension `gold.dim_date(calendar_date, year, month, day_of_month, day_of_week, is_weekend)`: table, 2016-01-01 to 2018-12-31
  - View `gold.daily_revenue_by_category(business_date, product_category, revenue, order_count, order_item_count)`

- [ ] **Step 1: Write the failing tests**

`tests/test_gold_star.py`:
```python
from datetime import date
from decimal import Decimal

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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_gold_star.py -v`
Expected: FAIL, `Table gold.fact_order_items does not exist` (dbt finds nothing to build under `models/gold`, so the steps succeed, but the tables are missing)

- [ ] **Step 3: Create the fact table**

`dbt/models/gold/facts/fact_order_items.sql`:
```sql
{{ config(order_by='(business_date, order_id, order_item_id)') }}

-- Grain: one Order Item. counts_as_revenue applies the Revenue rule from CONTEXT.md.
select
    business_date,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    order_status,
    price,
    freight,
    order_status not in ('canceled', 'unavailable') as counts_as_revenue
from {{ ref('order_items') }}
inner join (
    select business_date, order_id, order_status
    from {{ ref('orders') }}
    where business_date = {{ business_date() }}
) using (business_date, order_id)
where business_date = {{ business_date() }}
```

- [ ] **Step 4: Create the dimensions**

`dbt/models/gold/dimensions/dim_product.sql`:
```sql
{{ config(materialized='table', engine='MergeTree()', order_by='product_id') }}

-- Star, not snowflake: Product Category is folded into the product dimension (ADR 0004).
select
    product_id,
    product_category_name,
    coalesce(product_category_name_english, product_category_name, 'Uncategorized') as product_category
from {{ ref('products') }}
left join {{ ref('product_categories') }} using (product_category_name)
```

`dbt/models/gold/dimensions/dim_date.sql`:
```sql
{{ config(materialized='table', engine='MergeTree()', order_by='calendar_date') }}

select
    toDate('2016-01-01') + number as calendar_date,
    toYear(calendar_date) as year,
    toMonth(calendar_date) as month,
    toDayOfMonth(calendar_date) as day_of_month,
    toDayOfWeek(calendar_date) as day_of_week,
    toDayOfWeek(calendar_date) >= 6 as is_weekend
from numbers(toUInt64(dateDiff('day', toDate('2016-01-01'), toDate('2018-12-31')) + 1))
```

- [ ] **Step 5: Create the report view and tests**

`dbt/models/gold/reports/daily_revenue_by_category.sql`:
```sql
{{ config(materialized='view') }}

-- A view over the star, so it always reflects the current catalog (no category history).
select
    business_date,
    product_category,
    sum(price) as revenue,
    uniqExact(order_id) as order_count,
    count() as order_item_count
from {{ ref('fact_order_items') }}
inner join {{ ref('dim_product') }} using (product_id)
where counts_as_revenue = 1
group by business_date, product_category
```

`dbt/models/gold/_gold.yml`:
```yaml
version: 2

models:
  - name: fact_order_items
    columns:
      - name: business_date
        data_tests: [not_null]
      - name: product_id
        data_tests: [not_null]
  - name: dim_product
    columns:
      - name: product_id
        data_tests: [not_null, unique]
      - name: product_category
        data_tests: [not_null]
```

`dbt/tests/gold_fact_order_items_unique.sql`:
```sql
select business_date, order_id, order_item_id
from {{ ref('fact_order_items') }}
group by business_date, order_id, order_item_id
having count() > 1
```

`dbt/tests/gold_daily_revenue_unique.sql`:
```sql
select business_date, product_category
from {{ ref('daily_revenue_by_category') }}
group by business_date, product_category
having count() > 1
```

- [ ] **Step 6: Run all tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all passed

- [ ] **Step 7: Commit**

```bash
git add dbt tests/test_gold_star.py
git commit -m "feat: Gold star schema (fact_order_items, dim_product, dim_date) and daily Revenue view"
```

---

### Task 9: Revenue reconciliation against the Shop Database

**Files:**
- Create: `src/pipeline/reconcile.py`
- Test: `tests/test_reconcile.py`

**Interfaces:**
- Consumes: Postgres `shop.orders` and `shop.order_items`; ClickHouse `gold.daily_revenue_by_category` and `silver.quarantine`
- Produces: `reconcile.Reconciliation` (frozen dataclass: `source_revenue: Decimal, gold_revenue: Decimal, quarantined_rows: int`, property `matches -> bool`); `reconcile.reconcile(start: date, end: date) -> Reconciliation`; `reconcile.main(argv: list[str] | None = None) -> int` (0 when Revenue matches, otherwise 1); CLI `--from YYYY-MM-DD --to YYYY-MM-DD`

- [ ] **Step 1: Write the failing tests**

`tests/test_reconcile.py`:
```python
from datetime import date
from decimal import Decimal

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
    assert reconcile(D, D2) == Reconciliation(Decimal("240.00"), Decimal("240.00"), 0)
    assert main(["--from", "2017-11-24", "--to", "2017-11-25"]) == 0


def test_missing_gold_rows_are_reported(bronze_loaded):
    _run(D, D2)
    bronze_loaded.command("alter table gold.fact_order_items drop partition tuple(toDate('2017-11-25'))")
    assert not reconcile(D, D2).matches
    assert main(["--from", "2017-11-24", "--to", "2017-11-25"]) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_reconcile.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'pipeline.reconcile'`

- [ ] **Step 3: Implement reconciliation**

`src/pipeline/reconcile.py`:
```python
"""Check that Gold Revenue matches Revenue computed directly from the Shop Database."""

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from pipeline import config
from pipeline.warehouse import connect

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
QUARANTINED_ROWS = """
    select count()
    from silver.quarantine
    where business_date between {start:Date} and {end:Date}
"""


@dataclass(frozen=True)
class Reconciliation:
    source_revenue: Decimal
    gold_revenue: Decimal
    quarantined_rows: int

    @property
    def matches(self) -> bool:
        return self.source_revenue == self.gold_revenue


def reconcile(start: date, end: date) -> Reconciliation:
    with psycopg.connect(config.shop_db().conninfo()) as shop:
        source = shop.execute(SOURCE_REVENUE, (start, end)).fetchone()[0]
    client = connect()
    try:
        params = {"start": start, "end": end}
        gold = client.query(GOLD_REVENUE, parameters=params).result_rows[0][0]
        quarantined = client.query(QUARANTINED_ROWS, parameters=params).result_rows[0][0]
    finally:
        client.close()
    return Reconciliation(Decimal(source), Decimal(gold), quarantined)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline.reconcile")
    parser.add_argument("--from", dest="start", required=True, type=date.fromisoformat)
    parser.add_argument("--to", dest="end", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)
    result = reconcile(args.start, args.end)
    print(f"Shop Database Revenue: {result.source_revenue}")
    print(f"Gold Revenue:          {result.gold_revenue}")
    print(f"Quarantined rows:      {result.quarantined_rows}")
    print("MATCH" if result.matches else "MISMATCH")
    return 0 if result.matches else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/reconcile.py tests/test_reconcile.py
git commit -m "feat: reconcile Gold Revenue against the Shop Database"
```

---

### Task 10: Airflow orchestration, real data, backfill

This task's deliverable is checked by running the real system, which covers done criteria 1–5 in the spec.

**Files:**
- Create: `airflow/Dockerfile`, `airflow/dags/daily_orders.py`, `README.md`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: CLI `python -m pipeline.run --business-date <date> --step <step>`
- Produces: Airflow DAG `daily_orders` with tasks `bronze >> silver >> facts >> dimensions`, `@daily` from 2016-09-04 to 2018-10-17, where `dimensions` runs in a 1-slot pool `dimensions`

- [ ] **Step 1: Build the Airflow image**

`airflow/Dockerfile` (build context is the repo root). The pipeline gets its own venv so its dependencies never conflict with Airflow's:
```dockerfile
FROM apache/airflow:3.1.0

COPY --chown=airflow:root pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow:root src /opt/pipeline/src
RUN python -m venv /home/airflow/pipeline-venv \
 && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -e /opt/pipeline
```

The editable install points at `/opt/pipeline/src`. Compose mounts the live `src/` over it, so code edits apply without rebuilding the image.

- [ ] **Step 2: Add Airflow to Compose**

Append to `services:` in `docker-compose.yml`:
```yaml
  airflow-db:
    image: postgres:17
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes: [airflow-data:/var/lib/postgresql/data]

  airflow:
    build:
      context: .
      dockerfile: airflow/Dockerfile
    command: standalone
    depends_on: [airflow-db, shop-db, warehouse]
    ports: ["8080:8080"]
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "True"
      SHOP_DB_HOST: shop-db
      SHOP_DB_PORT: "5432"
      WAREHOUSE_HOST: warehouse
      WAREHOUSE_PORT: "8123"
      CATALOG_PATH: /opt/pipeline/data/catalog/merchandising_catalog.xlsx
      DBT_DIR: /opt/pipeline/dbt
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/pipeline/src
      - ./dbt:/opt/pipeline/dbt
      - ./data:/opt/pipeline/data
```

Add `airflow-data:` under `volumes:`.

- [ ] **Step 3: Write the DAG**

`airflow/dags/daily_orders.py`:
```python
"""Daily Orders: Bronze -> Silver -> Gold facts -> Gold dimensions for one Business Date ({{ ds }})."""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

RUN_STEP = "/home/airflow/pipeline-venv/bin/python -m pipeline.run --business-date {{ ds }} --step "


@dag(
    dag_id="daily_orders",
    schedule="@daily",
    start_date=pendulum.datetime(2016, 9, 4, tz="America/Sao_Paulo"),
    end_date=pendulum.datetime(2018, 10, 17, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=4,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
)
def daily_orders():
    bronze = BashOperator(task_id="bronze", bash_command=RUN_STEP + "bronze")
    silver = BashOperator(task_id="silver", bash_command=RUN_STEP + "silver")
    facts = BashOperator(task_id="facts", bash_command=RUN_STEP + "facts")
    # Dimensions and report views are rebuilt whole; the 1-slot pool stops parallel runs colliding.
    dimensions = BashOperator(task_id="dimensions", bash_command=RUN_STEP + "dimensions", pool="dimensions")
    bronze >> silver >> facts >> dimensions


daily_orders()
```

- [ ] **Step 4: Start the stack and check the DAG loads (done criterion 1)**

```bash
docker compose up -d --build
docker compose exec airflow airflow pools set dimensions 1 "Rebuild Gold dimensions one run at a time"
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list | grep daily_orders
```
Expected: no import errors, and `daily_orders` is listed. The UI is at http://localhost:8080.
If `airflow.providers.standard` or `airflow.sdk` fails to import, check the installed Airflow version with `docker compose exec airflow airflow version`. Both modules exist from Airflow 3.0.

- [ ] **Step 5: Load the real Olist data (done criterion 2)**

1. Download the dataset from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce and unzip all 9 CSVs into `data/olist/raw/`.
2. From the host:
```bash
.venv/Scripts/python -m pipeline.seed shop-db
.venv/Scripts/python -m pipeline.seed catalog
```
Expected: shop counts of about `{'orders': 99441, 'order_items': 112650, 'order_payments': 103886, 'customers': 99441, 'sellers': 3095, 'order_reviews': 99224}`, then `wrote data/catalog/merchandising_catalog.xlsx`.

If seeding fails on a primary key or a cast, the real data disagrees with the contract. Stop and report it rather than loosening the DDL silently.

- [ ] **Step 6: Run one Business Date through Airflow (done criterion 3)**

```bash
docker compose exec airflow airflow dags test daily_orders 2017-11-24
docker compose exec warehouse clickhouse-client --user warehouse --password warehouse \
  -q "select * from gold.daily_revenue_by_category where business_date = '2017-11-24' order by revenue desc limit 5"
```
Expected: all four tasks succeed, with about 70 category rows for Black Friday 2017. This first single run also creates every Silver and Gold table, so the parallel backfill below never races to create them.

- [ ] **Step 7: Backfill every Olist date (done criterion 4)**

```bash
docker compose exec airflow airflow backfill create --dag-id daily_orders --from-date 2016-09-04 --to-date 2018-10-17 --max-active-runs 4
```
If the flags differ in this Airflow version, check `airflow backfill create --help`. Track progress in the UI. Expect a few hours, mostly dbt start-up time per run.

Then reconcile from the host:
```bash
.venv/Scripts/python -m pipeline.reconcile --from 2016-09-04 --to 2018-10-17
```
Expected: `MATCH`, exit code 0. Report the quarantined row count. If it isn't 0, query `silver.quarantine` grouped by `failure_reason` and record what the real data contains.

- [ ] **Step 8: Prove reruns change nothing (done criterion 5)**

```bash
HASH="select cityHash64(groupArray(tuple(*))) from (select * from gold.daily_revenue_by_category where business_date = '2017-11-24' order by product_category)"
docker compose exec warehouse clickhouse-client --user warehouse --password warehouse -q "$HASH"
docker compose exec airflow airflow dags test daily_orders 2017-11-24
docker compose exec warehouse clickhouse-client --user warehouse --password warehouse -q "$HASH"
.venv/Scripts/python -m pipeline.reconcile --from 2016-09-04 --to 2018-10-17
```
Expected: both hashes identical, and still `MATCH`.

- [ ] **Step 9: Write the runbook**

`README.md`:
````markdown
# Retail Data Pipeline

An end-to-end ELT learning pipeline for an e-commerce business (Olist). The vocabulary is in [CONTEXT.md](CONTEXT.md) and the decisions are in [docs/adr](docs/adr).

Shop Database (Postgres) + Merchandising Catalog (Excel) → **Bronze** → **Silver** (+ Quarantine) → **Gold** star schema, all in ClickHouse, one Business Date per run, orchestrated by Airflow.

## Setup

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"     # .venv/bin/... on macOS/Linux
docker compose up -d --build
docker compose exec airflow airflow pools set dimensions 1 "Rebuild Gold dimensions one run at a time"
```

Download https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce into `data/olist/raw/`, then:

```bash
.venv/Scripts/python -m pipeline.seed shop-db
.venv/Scripts/python -m pipeline.seed catalog
```

## Run

- One Business Date: `docker compose exec airflow airflow dags test daily_orders 2017-11-24`
- Without Airflow: `.venv/Scripts/python -m pipeline.run --business-date 2017-11-24 --step all`
- Backfill (run one date first so the tables exist):
  `docker compose exec airflow airflow backfill create --dag-id daily_orders --from-date 2016-09-04 --to-date 2018-10-17 --max-active-runs 4`
- Check Gold against the source: `.venv/Scripts/python -m pipeline.reconcile --from 2016-09-04 --to 2018-10-17`
- Query the warehouse: `docker compose exec warehouse clickhouse-client --user warehouse --password warehouse`

## Gold star schema

- `gold.fact_order_items`: one row per Order Item, partitioned by Business Date
- `gold.dim_product`: Product with its resolved Product Category
- `gold.dim_date`: calendar
- `gold.daily_revenue_by_category`: view over the star

## Tests

```bash
docker compose up -d shop-db warehouse-test
.venv/Scripts/python -m pytest
```

Tests use the `shop_test` Postgres database and the `warehouse-test` ClickHouse container, and never touch the dev data.
````

- [ ] **Step 10: Commit**

```bash
git add airflow docker-compose.yml README.md
git commit -m "feat: orchestrate Daily Orders with Airflow and document the runbook"
```
