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
