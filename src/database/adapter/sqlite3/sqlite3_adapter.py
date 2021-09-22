from __future__ import annotations

from ast import literal_eval
from pathlib import Path
import sqlite3
from sqlite3 import Connection
from typing import Optional, Type, TypeVar

from src.data_store.data_store import DataStore
from src.data_store.data_type import Array, TypeAlias
from src.data_store.index import Index
from src.data_store.query import EqualsQuery, MultiAndQuery, Query
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.adapter.database_schema import DatabaseSchema
from src.database.adapter.query.sql_query_compiler import SqlQueryCompiler
from src.database.adapter.statement.sql_statement import SqlStatement
from src.database.connection_config import ConnectionConfig
from src.database.data_token import DataToken
from src.database.reference_tables import PROTECTED_GROUP

from .sqlite3_schema import Sqlite3Schema

T = TypeVar("T", bound=DataStore)


class Sqlite3Adapter(DatabaseAdapter):
    _conn_config: ConnectionConfig
    _connection: Connection

    def __init__(self: Sqlite3Adapter, conn_config: ConnectionConfig) -> None:
        self._conn_config = conn_config
        self._connection = sqlite3.connect(f"{conn_config.uri()}/{PROTECTED_GROUP}.db")
        for db_path in Path(conn_config.uri()).glob("*.db"):
            self.create_group(db_path.stem)

    def schema(self: Sqlite3Adapter, data_store_type: type[T]) -> DatabaseSchema:
        return Sqlite3Schema(data_store_type)

    def create_group_table(
        self: "Sqlite3Adapter", data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        schema = Sqlite3Schema(data_store_type)
        with self._connection:
            statement = SqlStatement().CREATE_TABLE(f"{data_token}", str(schema))
            self._connection.execute(statement.compile())

    def create_group(self: Sqlite3Adapter, data_group: str) -> None:
        with self._connection:
            statement = (
                SqlStatement()
                .ATTACH_DATABASE(self._conn_config.uri(), data_group)
                .AS(data_group)
            )
            self._connection.execute(statement.compile())

    def has_group_table(self: Sqlite3Adapter, data_token: DataToken) -> bool:
        if not self.has_group(data_token.data_group):
            return False

        with self._connection:
            statement = (
                SqlStatement()
                .SELECT("count(name)")
                .FROM(f"{data_token.data_group}.sqlite_master")
                .WHERE(f"type='table' AND name='{data_token.table_name}'")
            )
            cursor = self._connection.execute(statement.compile())
            return cursor.fetchone()[0] > 0

    def has_group(self: Sqlite3Adapter, data_group: str) -> bool:
        with self._connection:
            cursor = self._connection.execute("PRAGMA database_list;")
            groups = {row[1] for row in cursor.fetchall()}
            return data_group in groups

    def drop_group_table(self: Sqlite3Adapter, data_token: DataToken) -> None:
        with self._connection:
            statement = SqlStatement().DROP_TABLE(f"{data_token}")
            self._connection.execute(statement.compile())

    def drop_group(self: Sqlite3Adapter, data_group: str) -> None:
        with self._connection:
            statement = SqlStatement().DETACH_DATABASE(data_group)
            self._connection.execute(statement.compile())

        db_path = Path(f"{self._conn_config.uri()}/{data_group}.db")
        db_path.unlink()

    def create_index(self: Sqlite3Adapter, data_token: DataToken, index: Index) -> None:
        with self._connection:
            col_names = [str(col) for col in index.columns]
            statement = SqlStatement().CREATE_INDEX(index.name, data_token, col_names)
            self._connection.execute(statement.compile())

    def has_index(self: Sqlite3Adapter, data_token: DataToken, index: Index) -> bool:
        with self._connection:
            cursor = self._connection.execute(
                f"PRAGMA {data_token.data_group}.index_list({data_token.table_name});"
            )
            data_rows = cursor.fetchall()
            indices = set(item[1] for item in data_rows)
            if index.name not in indices:
                return False
            cursor = self._connection.execute(
                f"PRAGMA {data_token.data_group}.index_xinfo({data_token.table_name}_{index.name});"
            )
            data_rows = cursor.fetchall()
            cols = set(item[2] for item in data_rows)
            expected = set(str(col) for col in index.columns)
            return len(expected - cols) == 0

    def query(
        self: Sqlite3Adapter,
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[str]] = None,
    ) -> list[tuple]:
        statement = SqlStatement()
        if columns is not None:
            clean = []
            for column in columns:
                clean.append(column)
            statement.SELECT(*clean)
        else:
            statement.SELECT_ALL()

        statement.FROM(str(data_token))

        if query is not None:
            compiler = SqlQueryCompiler(quote=True)
            query = compiler.compile(query)
            statement.WHERE(str(query))

        with self._connection:
            cursor = self._connection.execute(statement.compile())
            data_rows = cursor.fetchall()
            return data_rows

    def _insert_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
    ) -> None:
        with self._connection:
            columns = [str(col) for col in data_store.columns]
            values = ["?" for _ in range(len(data_store.columns))]

            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *columns)
                .VALUES(values, quote=False)
            )
            self._connection.executemany(
                statement.compile(),
                data_store.itertuples(ignore_index=True),
            )

    def _update_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        with self._connection:
            local_alignment_columns = []
            db_alignment_columns = []
            for col in alignment_columns:
                col = str(col)
                local_alignment_columns.append(col)
                db_alignment_columns.append(col)

            all_columns = [str(col) for col in data_store.columns]
            db_alignment_values = ["?" for _ in db_alignment_columns]
            update_columns = list(set(all_columns) - set(local_alignment_columns))
            update_values = ["?" for _ in update_columns]

            query = MultiAndQuery(
                EqualsQuery, db_alignment_columns, db_alignment_values
            )
            compiler = SqlQueryCompiler(quote=False)
            query = compiler.compile(query)
            statement = (
                SqlStatement()
                .UPDATE_FROM_VALUES(data_token, update_columns, update_values)
                .WHERE(str(query))
            )

            statement_columns = update_columns + local_alignment_columns
            self._connection.executemany(
                statement.compile(),
                data_store[statement_columns].itertuples(ignore_index=True),
            )

    def _upsert_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        with self._connection:
            columns = [str(col) for col in data_store.columns]
            values = ["?" for _ in range(len(data_store.columns))]

            local_alignment_columns = []
            db_alignment_columns = []
            for col in alignment_columns:
                col = str(col)
                local_alignment_columns.append(col)
                db_alignment_columns.append(col)

            all_columns = [str(col) for col in data_store.columns]
            update_columns = list(set(all_columns) - set(local_alignment_columns))
            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *columns)
                .VALUES(values, quote=False)
                .UPDATE_CONFLICTS(db_alignment_columns, update_columns)
            )
            self._connection.executemany(
                statement.compile(),
                data_store.itertuples(ignore_index=True),
            )

    def delete(self: Sqlite3Adapter, data_token: DataToken, criteria: Query) -> None:
        with self._connection:
            compiler = SqlQueryCompiler(quote=True)
            query = compiler.compile(criteria)
            statement = SqlStatement().DELETE().FROM(data_token).WHERE(query)
            self._connection.execute(statement.compile())

    def row_count(self: Sqlite3Adapter, data_token: DataToken) -> int:
        with self._connection:
            statement = SqlStatement().SELECT().COUNT().FROM(data_token)
            cursor = self._connection.execute(statement.compile())
            return cursor.fetchone()[0]

    def stop(self: "Sqlite3Adapter") -> None:
        self._connection.close()
