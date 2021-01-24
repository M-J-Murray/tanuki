from __future__ import annotations

from typing import Any, Optional, TypeVar, Union

from pandas.core.indexes.base import Index

from src.data_store.query import Query
from src.database.data_token import DataToken

Indexible = Union[Any, list, Index]


class DatabaseAdapter:
    def commit(self: DatabaseAdapter) -> None:
        raise NotImplementedError()

    def rollback(self: DatabaseAdapter) -> None:
        raise NotImplementedError()

    def has_group(self: DatabaseAdapter, data_group: str) -> bool:
        raise NotImplementedError()

    def create_group(self: DatabaseAdapter, data_group: str) -> None:
        raise NotImplementedError()

    def has_group_table(self: DatabaseAdapter, data_token: DataToken) -> bool:
        raise NotImplementedError()

    def create_group_table(
        self: DatabaseAdapter, data_token: DataToken, data_store_type: type[T]
    ) -> None:
        raise NotImplementedError()

    def drop_group(self: DatabaseAdapter, data_group: str) -> None:
        raise NotImplementedError()

    def drop_group_table(self: DatabaseAdapter, data_token: DataToken) -> None:
        raise NotImplementedError()

    def query(
        self: DatabaseAdapter,
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[str]] = None,
    ) -> list[tuple]:
        raise NotImplementedError()

    def insert(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        raise NotImplementedError()

    def update(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def upsert(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def delete(self: DatabaseAdapter, data_token: DataToken, criteria: Query) -> None:
        raise NotImplementedError()

    def drop_indices(
        self: DatabaseAdapter,
        data_token: DataToken,
        indices: Indexible,
    ) -> None:
        raise NotImplementedError()

    def stop(self: DatabaseAdapter) -> None:
        raise NotImplementedError()


from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)
