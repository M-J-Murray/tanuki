from __future__ import annotations

from contextlib import closing
import sqlite3
from sqlite3 import Connection
from src.database.data_token import DataToken

from src.data_store.data_store import DataStore
from src.database.connection_config import ConnectionConfig
from src.database.database import Database

from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.adapter.statement.sql_statement import SqlStatement

from .sqlite3_schema import Sqlite3Schema


class Sqlite3Adapter(DatabaseAdapter):
    _conn_config: ConnectionConfig
    _sql: Connection

    def __init__(self: Sqlite3Adapter, conn_config: ConnectionConfig) -> None:
        self._conn_config = conn_config
        self._sql = sqlite3.connect(conn_config.uri())

    def create_table(
        self: "Sqlite3Adapter", data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        schema = Sqlite3Schema(data_store_type)
        with closing(self._sql.cursor()) as cursor:
            statement = SqlStatement().CREATE_TABLE(data_token, str(schema))
            cursor.execute(statement.compile())
            self._sql.commit()

    def insert(self: "DatabaseAdapter", data_token: DataToken, data_store: DataStore) -> None:
        pass

    def stop(self: "Sqlite3Adapter") -> None:
        self._sql.close()
