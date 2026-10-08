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


@pytest.fixture
def seeded_shop():
    from pipeline.seed import seed_shop_db

    seed_shop_db(FIXTURES)


@pytest.fixture
def catalog_file(tmp_path) -> Path:
    from pipeline.seed import build_catalog

    path = tmp_path / "merchandising_catalog.xlsx"
    build_catalog(FIXTURES, path)
    return path


from datetime import date, timezone  # noqa: E402

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
            "select max(_loaded_at) from bronze.shop_loads where _business_date = {d:Date}",
            parameters={"d": business_date},
        ).result_rows[0][0]
        assert loaded_at.year > 1970, f"no Bronze load for {business_date}"  # max() of no rows is 1970
        # clickhouse-connect returns a naive datetime holding UTC; inserted back naive, it would be read
        # as local time and land in a load nobody reads, so every bad-row test would pass vacuously.
        loaded_at = loaded_at.replace(tzinfo=timezone.utc)
        for row in rows:
            warehouse_client.insert(
                table,
                [(*row.values(), business_date, loaded_at)],
                column_names=[*row, "_business_date", "_loaded_at"],
                database="bronze",
            )

    return add
