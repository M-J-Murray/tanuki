from __future__ import annotations

from typing import Optional, Type, TypeVar

from src.data_store.index import Index
from src.data_store.query import Query
from src.database.data_token import DataToken


class DatabaseAdapter:
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

    def create_index(self: DatabaseAdapter, data_token: DataToken, index: Index) -> None:
        raise NotImplementedError()

    def has_index(self: DatabaseAdapter, data_token: DataToken, index: Index) -> bool:
        return NotImplementedError()

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
    ) -> None:
        if data_store.is_link():
            self._insert_from_link(data_token, data_store)
        else:
            self._insert_from_values(data_token, data_store)

    def _insert_from_values(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
    ) -> None:
        raise NotImplementedError()

    def _insert_from_link(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
    ) -> None:
        raise NotImplementedError()

    def update(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        if data_store.is_link():
            self._update_from_link(data_token, data_store, alignment_columns)
        else:
            self._update_from_values(data_token, data_store, alignment_columns)

    def _update_from_values(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def _update_from_link(
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
        if data_store.is_link():
            self._upsert_from_link(
                data_token, data_store, alignment_columns
            )
        else:
            self._upsert_from_values(
                data_token, data_store, alignment_columns
            )

    def _upsert_from_values(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def _upsert_from_link(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        raise NotImplementedError()

    def delete(self: DatabaseAdapter, data_token: DataToken, criteria: Query) -> None:
        raise NotImplementedError()

    def row_count(self: DatabaseAdapter, data_token: DataToken) -> int:
        raise NotImplementedError()

    def stop(self: DatabaseAdapter) -> None:
        raise NotImplementedError()


from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)
