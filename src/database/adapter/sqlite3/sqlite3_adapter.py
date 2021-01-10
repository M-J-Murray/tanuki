from __future__ import annotations

from contextlib import closing
import sqlite3
from sqlite3 import Connection
from src.data_store.query_type import QueryType
from src.database.adapter.query.sql_query_compiler import SqlQueryCompiler
from src.database.adapter.database_schema import DatabaseSchema
from typing import Optional, TypeVar

from src.data_store.data_store import DataStore
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.adapter.statement.sql_statement import SqlStatement
from src.database.connection_config import ConnectionConfig
from src.database.data_token import DataToken

from .sqlite3_schema import Sqlite3Schema

T = TypeVar("T", bound=DataStore)


class Sqlite3Adapter(DatabaseAdapter):
    _conn_config: ConnectionConfig
    _sql: Connection

    _query_compiler = SqlQueryCompiler()

    def __init__(self: Sqlite3Adapter, conn_config: ConnectionConfig) -> None:
        self._conn_config = conn_config
        self._sql = sqlite3.connect(conn_config.uri())

    def schema(self: Sqlite3Adapter, data_store_type: type[T]) -> DatabaseSchema:
        return Sqlite3Schema(data_store_type)

    def create_table(
        self: "Sqlite3Adapter", data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        schema = Sqlite3Schema(data_store_type)
        with closing(self._sql.cursor()) as cursor:
            statement = SqlStatement().CREATE_TABLE(data_token, str(schema))
            cursor.execute(statement.compile())
            self._sql.commit()

    def query(
        self: Sqlite3Adapter,
        data_token: DataToken,
        query_type: Optional[QueryType],
        columns: Optional[list[str]] = None,
    ) -> DataStore:
        statement = SqlStatement()
        if columns is not None:
            statement.SELECT(*columns)
        else:
            statement.SELECT_ALL()

        statement.FROM(str(data_token))
        
        if query_type is not None:
            query = self._query_compiler.compile(query_type)
            statement.WHERE(str(query))
        
        with closing(self._sql.cursor()) as cursor:
            cursor.execute(statement.compile())
            data_rows = cursor.fetchall()
            return list(zip(*data_rows))
            

    def insert(
        self: "DatabaseAdapter", data_token: DataToken, data_store: DataStore
    ) -> None:
        pass

    def stop(self: "Sqlite3Adapter") -> None:
        self._sql.close()
