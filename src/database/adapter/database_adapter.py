from __future__ import annotations
from src.data_store.query_type import QueryType

from typing import Any, Optional, TypeVar

from src.data_store.data_store import DataStore
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class DatabaseAdapter:
    def has_group(self: DatabaseAdapter, data_group: str) -> bool:
        ...

    def create_group(self: DatabaseAdapter, data_group: str) -> None:
        ...

    def has_table(self: DatabaseAdapter, data_token: DataToken) -> bool:
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
        self: DatabaseAdapter,
        data_token: DataToken,
        query_type: Optional[QueryType] = None,
        columns: Optional[list[str]] = None,
    ) -> list[tuple[Any, ...]]:
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
