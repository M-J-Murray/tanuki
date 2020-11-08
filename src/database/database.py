from __future__ import annotations

from src.data_store.data_store import DataStore
from src.data_store.column import Column
from src.database.adapter.database_adapter import DatabaseAdapter
from types import TracebackType
from typing import Optional, Type

from pandas import Index

import src.data_store.data_store as ds
import src.database.data_token as dt
from src.database.data_token import DataToken


PUBLIC_GROUP = "public"


class TableReference(DataStore):
    data_token = DataToken("table_reference", PUBLIC_GROUP)

    table_name: Column[str]
    data_group: Column[str]
    schema_name: Column[str]
    schema_version: Column[int]


class SchemaReference(DataStore):
    data_token = DataToken("schema_reference", PUBLIC_GROUP)

    schema_name: Column[str]
    schema_version: Column[int]
    schema_table_reference: Column[str]
    schema_constraints: Column[list[str]]


class SchemaTableReference(DataStore):
    column_name: Column[str]
    column_type: Column[str]
    column_constraints: Column[list[str]]


class Database:
    _db_adapter: DatabaseAdapter

    def __init__(self, database_adapter: DatabaseAdapter) -> None:
        self._db_adapter = database_adapter

    def __enter__(self: Database) -> Database:
        dt.DataToken._active_db = self._db_adapter
        return self

    def __exit__(
        self: Database,
        type: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        dt.DataToken._active_db = None

    def _setup_reference_tables(self):
        _db_adapter.create_table()

    def _validate_reference_tables(self):
        pass

    def has_table(self):
        pass

    def list_table(self):
        pass

    def has_group(self):
        pass

    def list_group(self):
        pass

    def list_group_tables(self):
        pass

    def _has_schema(self):
        pass

    def _list_schema(self):
        pass

    def insert(
        self: Database, data_store: ds.DataStore, ignore_index: bool = False
    ) -> None:
        ...

    def update(
        self: Database, data_store: ds.DataStore, columns: Optional[list[str]]
    ) -> None:
        ...

    def upsert(
        self: Database, data_store: ds.DataStore, columns: Optional[list[str]]
    ) -> None:
        ...

    def drop_indices(self: Database, indices: Index) -> None:
        ...

    def drop_table(self: Database, data_token: ds.DataToken) -> None:
        ...

    def copy_table(
        self: Database, source_data_token: ds.DataToken, target_data_token: ds.DataToken
    ) -> None:
        ...
