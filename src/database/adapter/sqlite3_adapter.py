from __future__ import annotations

import sqlite3
from sqlite3 import Connection

from src.data_store.data_store import DataStore
from src.database.connection_config import ConnectionConfig
from src.database.database import Database

from .database_adapter import DatabaseAdapter


class Sqlite3Adapter(DatabaseAdapter):
    _conn_config: ConnectionConfig
    _sql: Connection

    def __init__(self: Sqlite3Adapter, conn_config: ConnectionConfig) -> None:
        self._conn_config = conn_config
        self._sql = sqlite3.connect(conn_config.uri())
