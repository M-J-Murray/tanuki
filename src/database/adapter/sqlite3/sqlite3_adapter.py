from __future__ import annotations

from contextlib import closing
import sqlite3
from sqlite3 import Connection
from typing import Optional, TypeVar

from src.data_store.data_store import DataStore
from src.data_store.query import Query
from src.database.adapter.database_adapter import DatabaseAdapter, Indexible
from src.database.adapter.database_schema import DatabaseSchema
from src.database.adapter.query.sql_query_compiler import SqlQueryCompiler
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
            statement = SqlStatement().CREATE_TABLE(f"\"{data_token}\"", str(schema))
            cursor.execute(statement.compile())
            self._sql.commit()

    def create_group(self: Sqlite3Adapter, data_group: str) -> None:
        with closing(self._sql.cursor()) as cursor:
            statement = SqlStatement().CREATE_SCHEMA(data_group)
            cursor.execute(statement.compile())
            self._sql.commit()

    def has_table(self: Sqlite3Adapter, data_token: DataToken) -> bool:
        with closing(self._sql.cursor()) as cursor:
            statement = (
                SqlStatement()
                .SELECT("count(name)")
                .FROM("sqlite_master")
                .WHERE(f"type='table' AND name='{data_token}'")
            )
            cursor.execute(statement.compile())
            return cursor.fetchone()[0] > 0

    def has_group(self: Sqlite3Adapter, data_group: str) -> bool:
        return super().has_group(data_group)

    def drop_table(self: Sqlite3Adapter, data_token: DataToken) -> None:
        with closing(self._sql.cursor()) as cursor:
            statement = SqlStatement().DROP_TABLE(f"\"{data_token}\"")
            cursor.execute(statement.compile())
            self._sql.commit()

    def drop_group(self: Sqlite3Adapter, data_group: str) -> None:
        raise NotImplementedError()


    def query(
        self: Sqlite3Adapter,
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[str]] = None,
    ) -> DataStore:
        statement = SqlStatement()
        if columns is not None:
            statement.SELECT(*columns)
        else:
            statement.SELECT_ALL()

        statement.FROM(str(data_token))

        if query is not None:
            query = self._query_compiler.compile(query)
            statement.WHERE(str(query))

        with closing(self._sql.cursor()) as cursor:
            cursor.execute(statement.compile())
            data_rows = cursor.fetchall()
            return list(zip(*data_rows))


    def insert(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        raise NotImplementedError()

    def update(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def upsert(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def delete(self: Sqlite3Adapter, data_token: DataToken, criteria: Query) -> None:
        raise NotImplementedError()

    def drop_indices(
        self: Sqlite3Adapter,
        data_token: DataToken,
        indices: Indexible,
    ) -> None:
        raise NotImplementedError()

    def stop(self: "Sqlite3Adapter") -> None:
        self._sql.close()
