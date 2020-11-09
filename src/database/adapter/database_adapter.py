from __future__ import annotations
from src.database.adapter.database_schema import DatabaseSchema 

from typing import Generic, TypeVar

from src.data_backend.data_backend import DataBackend
from src.data_store.data_store import DataStore
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)
B = TypeVar("B", bound=DataBackend)

class DatabaseAdapter(Generic[B]):
    def backend(
        self: DatabaseAdapter, data_token: DataToken, read_only: bool = True
    ) -> B:
        ...

    def schema(self: DatabaseAdapter, data_store_type: type[T]) -> DatabaseSchema:
        ...

    def create_group(self: DatabaseAdapter, data_group: str) -> None:
        ...

    def create_table(
        self: DatabaseAdapter, data_token: DataToken, data_store_type: type[T]
    ) -> None:
        ...

    def drop_group(self: DatabaseAdapter, data_group: str) -> None:
        ...

    def drop_table(self: DatabaseAdapter, data_token: DataToken) -> None:
        ...

    def insert(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        ...

    def update(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        *columns: str
    ) -> None:
        ...

    def upsert(
        self: DatabaseAdapter,
        data_token: DataToken,
        data_store: T,
        *columns: str
    ) -> None:
        ...

    def query(self: DatabaseAdapter) -> T:
        ...

    def delete(self: DatabaseAdapter) -> None:
        ...

    def stop(self: DatabaseAdapter) -> None:
        ...
