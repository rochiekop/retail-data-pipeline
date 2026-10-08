import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

# Credentials live in a git-ignored .env file (see .env.example), never in code. Variables already set in the
# environment win, so tests and containers can override them.
load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set: copy .env.example to .env and fill it in")
    return value


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
        user=_required("SHOP_DB_USER"),
        password=_required("SHOP_DB_PASSWORD"),
    )


def warehouse() -> Db:
    return Db(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "8123")),
        dbname="default",
        user=_required("WAREHOUSE_USER"),
        password=_required("WAREHOUSE_PASSWORD"),
    )


def olist_dir() -> Path:
    return Path(os.getenv("OLIST_DIR", "data/olist/raw"))


def catalog_path() -> Path:
    return Path(os.getenv("CATALOG_PATH", "data/catalog/merchandising_catalog.xlsx"))
