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
