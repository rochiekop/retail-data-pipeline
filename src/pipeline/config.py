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
