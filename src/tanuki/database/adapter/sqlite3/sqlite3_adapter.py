from __future__ import annotations

import json
from pathlib import Path
import shutil
import sqlite3
from sqlite3 import Connection
from typing import Any, Optional, TypeVar

from tanuki.data_store.data_store import DataStore
from tanuki.data_store.index.index import Index
from tanuki.data_store.metadata import Metadata
from tanuki.data_store.query import AndGroupQuery, ColumnQuery, EqualsQuery, Query
from tanuki.database.adapter.database_adapter import DatabaseAdapter
from tanuki.database.adapter.database_schema import DatabaseSchema
from tanuki.database.adapter.query.sql_query_compiler import SqlQueryCompiler
from tanuki.database.adapter.statement.sql_statement import SqlStatement
from tanuki.database.connection_config import ConnectionConfig
from tanuki.database.data_token import DataToken
from tanuki.database.db_exceptions import (
    DatabaseAdapterError,
    DatabaseAdapterUsageError,
)
from tanuki.database.reference_tables import PROTECTED_GROUP

from .sqlite3_schema import Sqlite3Schema
from .uncommitted_change import (
    UncommittedChange,
    UncommittedGroupCreate,
    UncommittedGroupDelete,
    UncommittedMetadataCreate,
    UncommittedMetadataDelete,
    UncommittedMetadataGroupDelete,
)

T = TypeVar("T", bound=DataStore)


class Sqlite3Adapter(DatabaseAdapter):
    _conn_config: ConnectionConfig
    _metadata_dir: Path
    _checkpoint_dir: Path
    _connection: Optional[Connection]
    _enter_calls: int

    _uncommitted: list[UncommittedChange]

    def __init__(self: Sqlite3Adapter, conn_config: ConnectionConfig) -> None:
        self._conn_config = conn_config
        self._metadata_dir = Path(conn_config.uri()) / "metadata"
        self._metadata_dir.mkdir(exist_ok=True)
        self._checkpoint_dir = Path(conn_config.uri()) / "checkpoint"
        if self._checkpoint_dir.exists():
            shutil.rmtree(str(self._checkpoint_dir))
        self._checkpoint_dir.mkdir(exist_ok=False)

        self._enter_calls = 0
        self._connection = None

        self._uncommitted = []

    def new_connection(self) -> Connection:
        self._connection = sqlite3.connect(
            f"{self._conn_config.uri()}/{PROTECTED_GROUP}.db", isolation_level=None
        )
        self._connection.execute("BEGIN")
        with self:
            for db_path in Path(self._conn_config.uri()).glob("*.db"):
                self.create_group(db_path.stem)

    def __enter__(self):
        self._enter_calls += 1
        if self._connection is None:
            self.new_connection()

    def __exit__(self, etype, value, traceback):
        self._enter_calls -= 1
        if self._enter_calls == 0:
            self._connection.commit()
            self._connection.close()
            self._connection = None
            self._uncommitted = []
            shutil.rmtree(str(self._checkpoint_dir))
            self._checkpoint_dir.mkdir(exist_ok=False)

    def rollback(self):
        self._connection.rollback()
        for uncommitted in self._uncommitted:
            uncommitted.rollback()
        self._uncommitted = []
        shutil.rmtree(str(self._checkpoint_dir))
        self._checkpoint_dir.mkdir(exist_ok=False)

    def schema(self: Sqlite3Adapter, data_store_type: type[T]) -> DatabaseSchema:
        return Sqlite3Schema(data_store_type)

    def create_group_table(
        self: "Sqlite3Adapter", data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("create_group_table")
        try:
            schema = Sqlite3Schema(data_store_type)
            statement = (
                SqlStatement().CREATE_TABLE(f"{data_token}", str(schema)).compile()
            )
            self._connection.execute(statement)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("create_group_table failed", e)

    def create_group(self: Sqlite3Adapter, data_group: str) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("create_group")
        try:
            statement = (
                SqlStatement()
                .ATTACH_DATABASE(self._conn_config.uri(), data_group)
                .AS(data_group)
                .compile()
            )
            self._connection.execute(statement)

            data_group_path = Path(self._conn_config.uri()) / f"{data_group}.db"
            self._uncommitted.append(UncommittedGroupCreate(data_group_path))
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("create_group failed", e)

    def has_group_table(self: Sqlite3Adapter, data_token: DataToken) -> bool:
        if self._connection == None:
            raise DatabaseAdapterUsageError("has_group_table")
        try:
            if not self.has_group(data_token.data_group):
                return False

            statement = (
                SqlStatement()
                .SELECT("count(name)")
                .FROM(f"{data_token.data_group}.sqlite_master")
                .WHERE(f"type='table' AND name='{data_token.table_name}'")
                .compile()
            )

            cursor = self._connection.execute(statement)
            return cursor.fetchone()[0] > 0
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("has_group_table failed", e)

    def has_group(self: Sqlite3Adapter, data_group: str) -> bool:
        if self._connection == None:
            raise DatabaseAdapterUsageError("has_group")
        try:
            cursor = self._connection.execute("PRAGMA database_list;")
            groups = {row[1] for row in cursor.fetchall()}
            return data_group in groups
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("has_group failed", e)

    def drop_group_table(self: Sqlite3Adapter, data_token: DataToken) -> None:
        self.delete_metadata(data_token)
        if self._connection == None:
            raise DatabaseAdapterUsageError("drop_group_table")
        try:
            statement = SqlStatement().DROP_TABLE(f"{data_token}").compile()
            self._connection.execute(statement)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("drop_group_table failed", e)

    def drop_group(self: Sqlite3Adapter, data_group: str) -> None:
        self.delete_metadata_group(data_group)
        if self._connection == None:
            raise DatabaseAdapterUsageError("drop_group")
        try:
            statement = SqlStatement().DETACH_DATABASE(data_group).compile()
            self._connection.execute(statement)

            db_path = Path(self._conn_config.uri()) / f"{data_group}.db"
            uncommittment = UncommittedGroupDelete(db_path, data_group, self._checkpoint_dir)
            uncommittment.checkpoint()

            db_path.unlink()

            self._uncommitted.append(uncommittment)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("drop_group failed", e)

    def create_index(self: Sqlite3Adapter, data_token: DataToken, index: Index) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("create_index")
        try:
            col_names = [str(col) for col in index.columns]
            statement = (
                SqlStatement().CREATE_INDEX(index.name, data_token, col_names).compile()
            )

            self._connection.execute(statement)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("create_index failed", e)

    def has_index(self: Sqlite3Adapter, data_token: DataToken, index: Index) -> bool:
        if self._connection == None:
            raise DatabaseAdapterUsageError("has_index")
        try:
            cursor = self._connection.execute(
                f"PRAGMA {data_token.data_group}.index_list({data_token.table_name});"
            )
            data_rows = cursor.fetchall()
            indices = set(item[1] for item in data_rows)
            index_name = f"{data_token.table_name}_{index.name}"
            if index_name not in indices:
                return False
            cursor = self._connection.execute(
                f"PRAGMA {data_token.data_group}.index_xinfo({index_name});"
            )
            data_rows = cursor.fetchall()
            cols = set(item[2] for item in data_rows)
            expected = set(str(col) for col in index.columns)
            return len(expected - cols) == 0
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("has_index failed", e)

    def query(
        self: Sqlite3Adapter,
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[str]] = None,
    ) -> list[tuple]:
        if self._connection == None:
            raise DatabaseAdapterUsageError("query")
        try:
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
            statement = statement.compile()

            cursor = self._connection.execute(statement)
            data_rows = cursor.fetchall()
            return data_rows
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("query failed", e)

    def metadata_path(self, data_token: DataToken) -> Path:
        group_path = self._metadata_dir / data_token.data_group
        group_path.mkdir(exist_ok=True)
        return group_path / f"{data_token.table_name}.json"

    def update_metadata(self, data_token: DataToken, metadata: Metadata):
        if self._connection == None:
            raise DatabaseAdapterUsageError("update_metadata")
        try:
            data_dict = metadata.to_dict()
            metadata_path = self.metadata_path(data_token)
            with open(str(metadata_path), "w") as metadata_file:
                json.dump(data_dict, metadata_file)

            self._uncommitted.append(UncommittedMetadataCreate(metadata_path))
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("update_metadata failed", e)

    def delete_metadata(self, data_token: DataToken):
        if self._connection == None:
            raise DatabaseAdapterUsageError("delete_metadata")
        try:
            metadata_path = self.metadata_path(data_token)
            if not metadata_path.exists():
                return

            uncommittment = UncommittedMetadataDelete(metadata_path, data_token, self._checkpoint_dir)
            uncommittment.checkpoint()

            metadata_path.unlink()

            self._uncommitted.append(uncommittment)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("delete_metadata failed", e)

    def delete_metadata_group(self, data_group: str):
        if self._connection == None:
            raise DatabaseAdapterUsageError("delete_metadata_group")
        try:
            metadata_group_path = self._metadata_dir / data_group
            if not metadata_group_path.exists():
                return

            uncommittment = UncommittedMetadataGroupDelete(metadata_group_path, data_group, self._checkpoint_dir)
            uncommittment.checkpoint()

            shutil.rmtree(str(metadata_group_path))

            self._uncommitted.append(uncommittment)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("delete_metadata_group failed", e)


    def _insert_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
    ) -> None:
        if data_store.metadata is not None:
            self.update_metadata(data_token, data_store.metadata)
        if self._connection == None:
            raise DatabaseAdapterUsageError("_insert_from_values")
        try:
            columns = [str(col) for col in data_store.columns]
            values = ["?" for _ in range(len(data_store.columns))]

            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *columns)
                .VALUES(values, quote=False)
                .compile()
            )

            self._connection.executemany(
                statement,
                data_store.itertuples(ignore_index=True),
            )
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_insert_from_values failed", e)

    def _insert_from_link(
        self: Sqlite3Adapter, data_token: DataToken, data_store: T
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("_insert_from_link")
        try:
            link_token = data_store.link_token()

            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *data_store.columns)
                .SELECT(*data_store.columns)
                .FROM(link_token)
                .compile()
            )

            self._connection.execute(statement)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_insert_from_link failed", e)

    def _update_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("_update_from_values")
        try:
            alignment_columns = [str(col) for col in alignment_columns]
            all_columns = [str(col) for col in data_store.columns]
            db_alignment_values = ["?" for _ in alignment_columns]
            update_columns = list(set(all_columns) - set(alignment_columns))
            update_values = ["?" for _ in update_columns]

            query = ColumnQuery(
                EqualsQuery, AndGroupQuery, alignment_columns, db_alignment_values
            )
            compiler = SqlQueryCompiler(quote=False)
            query = compiler.compile(query)
            statement = (
                SqlStatement()
                .UPDATE_FROM_VALUES(data_token, update_columns, update_values)
                .WHERE(str(query))
                .compile()
            )

            statement_columns = update_columns + alignment_columns
            self._connection.executemany(
                statement,
                data_store[statement_columns].itertuples(ignore_index=True),
            )
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_update_from_values failed", e)

    def _update_from_link(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("_update_from_link")
        try:
            link_token = data_store.link_token()

            alignment_columns = [str(col) for col in alignment_columns]
            all_columns = [str(col) for col in data_store.columns]
            update_columns = list(set(all_columns) - set(alignment_columns))

            statement = SqlStatement().UPDATE_FROM_LINK(
                link_token, data_token, update_columns, alignment_columns
            )

            self._connection.execute(statement.compile())
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_update_from_link failed", e)

    def _upsert_from_values(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("_upsert_from_values")
        try:
            columns = [str(col) for col in data_store.columns]
            values = ["?" for _ in range(len(data_store.columns))]

            alignment_columns = [str(col) for col in alignment_columns]
            all_columns = [str(col) for col in data_store.columns]
            update_columns = list(set(all_columns) - set(alignment_columns))
            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *columns)
                .VALUES(values, quote=False)
                .UPDATE_CONFLICTS(alignment_columns, update_columns)
            )

            self._connection.executemany(
                statement.compile(),
                data_store.itertuples(ignore_index=True),
            )
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_upsert_from_values failed", e)

    def _upsert_from_link(
        self: Sqlite3Adapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("_upsert_from_link")
        try:
            link_token = data_store.link_token()

            alignment_columns = [str(col) for col in alignment_columns]
            all_columns = [str(col) for col in data_store.columns]
            update_columns = list(set(all_columns) - set(alignment_columns))

            statement = (
                SqlStatement()
                .INSERT_INTO(data_token, *data_store.columns)
                .SELECT(*data_store.columns)
                .FROM(link_token)
                .WHERE("true")
                .UPDATE_CONFLICTS(alignment_columns, update_columns)
                .compile()
            )

            self._connection.execute(statement)
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("_upsert_from_link failed", e)

    def delete(self: Sqlite3Adapter, data_token: DataToken, criteria: Query) -> None:
        if self._connection == None:
            raise DatabaseAdapterUsageError("delete")
        try:
            compiler = SqlQueryCompiler(quote=True)
            query = compiler.compile(criteria)
            statement = SqlStatement().DELETE().FROM(data_token).WHERE(query)
            self._connection.execute(statement.compile())
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("delete failed", e)

    def row_count(self: Sqlite3Adapter, data_token: DataToken) -> int:
        if self._connection == None:
            raise DatabaseAdapterUsageError("row_count")
        try:
            statement = SqlStatement().SELECT().COUNT().FROM(data_token)
            cursor = self._connection.execute(statement.compile())
            return cursor.fetchone()[0]
        except Exception as e:
            self.rollback()
            raise DatabaseAdapterError("row_count failed", e)

    def stop(self: "Sqlite3Adapter") -> None:
        if self._enter_calls > 0:
            self._enter_calls = 0
        if self._connection is not None:
            self.rollback()
            self._connection.close()
            self._connection = None
