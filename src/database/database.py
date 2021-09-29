from __future__ import annotations

from types import TracebackType
from typing import Any, cast, Optional, Type, TypeVar, Union

from src.data_store.column_alias import ColumnAlias
from src.data_store.data_type import DataType
from src.data_store.query import Query

from .adapter.database_adapter import DatabaseAdapter
from .data_token import DataToken
from .database_registrar import DatabaseRegistrar
from .db_exceptions import MissingTableError

class Database:
    _db_adapter: DatabaseAdapter
    _registrar: DatabaseRegistrar

    def __init__(self, database_adapter: DatabaseAdapter) -> None:
        self._db_adapter = database_adapter
        self._registrar = DatabaseRegistrar(database_adapter)

    def table_columns(self, data_token: DataToken) -> list[str]:
        with self._db_adapter:
            col_ids = self._registrar.store_class(data_token).columns
            return [str(col_id) for col_id in col_ids]

    def table_dtypes(self, data_token: DataToken) -> dict[str, DataType]:
        with self._db_adapter:
            store_class = self._registrar.store_class(data_token)
            return {column.name: column.dtype for column in store_class.columns}

    def has_table(self, data_token: DataToken) -> bool:
        with self._db_adapter:
            return self._registrar.has_table(data_token)

    def list_tables(self) -> list[DataToken]:
        with self._db_adapter:
            return self._registrar.list_tables()

    def has_group(self, data_group: str) -> bool:
        with self._db_adapter:
            return self._registrar.has_group(data_group)

    def list_groups(self) -> set[str]:
        with self._db_adapter:
            return self._registrar.list_groups()

    def list_group_tables(self, data_group: str) -> list[DataToken]:
        with self._db_adapter:
            return self._registrar.list_group_tables(data_group)

    def query(
        self: Database,
        store_type: Type[T],
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[ColumnAlias]] = None,
    ) -> T:
        with self._db_adapter:
            if not self.has_table(data_token):
                raise MissingTableError(data_token)
            columns = [str(col) for col in columns] if columns is not None else None
            table_data = self._db_adapter.query(data_token, query, columns)
            store_class: Type[T] = self._registrar.store_class(data_token)
            store = store_class.from_rows(table_data, columns=columns)
            return cast(store_type, store)

    def insert(
        self: Database, data_token: DataToken, data_store: T
    ) -> None:
        with self._db_adapter:
            if not self._registrar.has_table(data_token):
                self._registrar.create_table(data_token, data_store.__class__)
            self._db_adapter.insert(data_token, data_store)

    def update(
        self: Database,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[ColumnAlias],
    ) -> None:
        with self._db_adapter:
            if not self._registrar.has_table(data_token):
                raise MissingTableError(data_token)
            columns = [str(col) for col in alignment_columns]
            self._db_adapter.update(data_token, data_store, columns)

    def upsert(
        self: Database,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[ColumnAlias],
    ) -> None:
        with self._db_adapter:
            if not self._registrar.has_table(data_token):
                raise MissingTableError(data_token)
            columns = [str(col) for col in alignment_columns]
            self._db_adapter.upsert(data_token, data_store, columns)

    def delete(self: Database, data_token: DataToken, criteria: Query) -> None:
        with self._db_adapter:
            if not self._registrar.has_table(data_token):
                raise MissingTableError(data_token)
            self._db_adapter.delete(data_token, criteria)

    def drop_table(self: Database, data_token: DataToken) -> None:
        with self._db_adapter:
            self._registrar.drop_table(data_token)

    def drop_group(self: Database, data_group: str) -> None:
        with self._db_adapter:
            self._registrar.drop_group(data_group)

    def copy_table(
        self: Database,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        with self._db_adapter:
            self._registrar.copy_table(source_data_token, target_data_token)

    def move_table(
        self: Database,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        with self._db_adapter:
            self._registrar.copy_table(source_data_token, target_data_token)

    def copy_group(
        self: Database,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        with self._db_adapter:
            self._registrar.copy_table(source_data_token, target_data_token)

    def move_group(
        self: Database,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        with self._db_adapter:
            self._registrar.copy_table(source_data_token, target_data_token)

    def row_count(self, data_token: DataToken) -> int:
        with self._db_adapter:
            return self._db_adapter.row_count(data_token)

    def __enter__(self: Database) -> Database:
        return self

    def __exit__(
        self: Database,
        type: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        self._db_adapter.stop()


from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)
