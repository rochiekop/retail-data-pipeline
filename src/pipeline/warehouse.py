"""The ClickHouse warehouse that holds Bronze, Silver and Gold."""

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from pipeline import config


def connect() -> Client:
    db = config.warehouse()
    return clickhouse_connect.get_client(host=db.host, port=db.port, username=db.user, password=db.password)
