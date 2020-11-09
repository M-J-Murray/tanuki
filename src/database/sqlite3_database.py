from __future__ import annotations
from src.data_backend.sqlite3_backend import Sqlite3Backend

from src.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter

from .connection_config import ConnectionConfig
from .database import Database


class Sqlite3Database(Database[Sqlite3Backend]):
    _conn_config: ConnectionConfig

    def __init__(self: Sqlite3Database, conn_config: ConnectionConfig) -> None:
        super(Sqlite3Database, self).__init__(Sqlite3Adapter(conn_config))
