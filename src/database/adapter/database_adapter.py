from __future__ import annotations

from typing import Optional, TypeVar

from src.data_store.query_type import QueryType
from src.database.data_token import DataToken


class DatabaseAdapter:
    def commit(self: DatabaseAdapter) -> None:
        ...

    def rollback(self: DatabaseAdapter) -> None:
        ...

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
    ) -> list[tuple]:
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

    def delete(
        self: DatabaseAdapter, data_token: DataToken, criteria: QueryType
    ) -> None:
        ...

    def stop(self: DatabaseAdapter) -> None:
        ...


from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)
