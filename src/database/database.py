from __future__ import annotations

from types import TracebackType
from typing import cast, Optional, Type, TypeVar

from pandas import Index

from src.data_store.column_alias import ColumnAlias
from src.data_store.query_type import QueryType

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

    def has_table(self, data_token: DataToken) -> bool:
        return self._registrar.has_table(data_token)

    def list_tables(self) -> list[DataToken]:
        return self._registrar.list_tables()

    def has_group(self, data_group: str) -> bool:
        return self._registrar.has_group(data_group)

    def list_groups(self) -> set[str]:
        return self._registrar.list_groups()

    def list_group_tables(self, data_group: str) -> list[DataToken]:
        return self._registrar.list_group_tables(data_group)

    def query(
        self: Database,
        store_type: Type[T],
        data_token: DataToken,
        query_type: Optional[QueryType] = None,
        columns: Optional[list[ColumnAlias]] = None,
    ) -> T:
        if not self.has_table(data_token):
            raise MissingTableError(data_token)
        columns = [str(col) for col in columns] if columns is not None else None
        table_data = self._db_adapter.query(data_token, query_type, columns)
        store_class: Type[T] = self._registrar.store_class(data_token)
        store = store_class.from_rows(table_data)
        return cast(store_type, store.to_table())

    def insert(
        self: Database, data_token: DataToken, data_store: T, ignore_index: bool = False
    ) -> None:
        if not self._registrar.has_table(data_token):
            self._registrar.create_table(data_token, data_store.__class__)
        self._db_adapter.insert(data_token, data_store, ignore_index)

    def update(
        self: Database,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[ColumnAlias],
    ) -> None:
        if not self._registrar.has_table(data_token):
            raise MissingTableError(data_token)
        columns = [str(col) for col in alignment_columns]
        self._db_adapter.update(data_token, data_store, columns)

    def upsert(
        self: Database,
        data_token: DataToken,
        data_store: T,
        *columns: list[ColumnAlias]
    ) -> None:
        raise NotImplementedError()

    def drop(self: Database, data_token: DataToken, indices: Index) -> None:
        raise NotImplementedError()

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
