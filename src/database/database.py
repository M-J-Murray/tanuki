from __future__ import annotations

from abc import abstractmethod
from types import TracebackType
from typing import Optional, Type

from pandas import Index

import src.data_store.data_store as ds
import src.database.data_token as dt


class Database:

    def __enter__(self: Database) -> Database:
        dt.DataToken._active_db = self
        return self

    def __exit__(
        self: Database,
        type: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        dt.DataToken._active_db = None

    @abstractmethod
    def insert(
        self: Database, data_store: ds.DataStore, ignore_index: bool = False
    ) -> None:
        ...

    @abstractmethod
    def update(
        self: Database, data_store: ds.DataStore, columns: Optional[list[str]]
    ) -> None:
        ...

    @abstractmethod
    def upsert(
        self: Database, data_store: ds.DataStore, columns: Optional[list[str]]
    ) -> None:
        ...

    @abstractmethod
    def drop_indices(self: Database, indices: Index) -> None:
        ...

    @abstractmethod
    def drop_table(self: Database, data_token: ds.DataToken) -> None:
        ...

    @abstractmethod
    def copy_table(
        self: Database, source_data_token: ds.DataToken, target_data_token: ds.DataToken
    ) -> None:
        ...
