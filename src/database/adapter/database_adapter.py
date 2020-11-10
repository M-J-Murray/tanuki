from __future__ import annotations

from typing import Generic, Optional, TypeVar

from src.data_store.column import ColumnQuery
from src.data_store.data_store import DataStore
from src.database.adapter.database_schema import DatabaseSchema
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class DatabaseAdapter:
    def create_group(self: DatabaseAdapter, data_group: str) -> None:
        ...

    def has_table(self: DatabaseAdapter, data_token: DataToken) -> None:
        ...

    def create_table(
        self: DatabaseAdapter, data_token: DataToken, data_store_type: type[T]
    ) -> None:
        ...

    def drop_group(self: DatabaseAdapter, data_group: str) -> None:
        ...

    def drop_table(self: DatabaseAdapter, data_token: DataToken) -> None:
        ...

    def query(
        self: DatabaseAdapter, column_query: Optional[ColumnQuery] = None
    ) -> DataStore:
        ...

    def insert(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        ...

    def update(
        self: DatabaseAdapter, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def upsert(
        self: DatabaseAdapter, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def delete(self: DatabaseAdapter) -> None:
        ...

    def stop(self: DatabaseAdapter) -> None:
        ...
