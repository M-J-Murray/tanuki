from __future__ import annotations
from io import UnsupportedOperation

import pickle
from types import TracebackType, new_class
from typing import Any, Generic, Optional, Type, TypeVar

from pandas import Index

from src.data_backend.data_backend import DataBackend
from src.data_backend.database_backend import DatabaseBackend
from src.data_store.column import Column, ColumnQuery
from src.data_store.data_store import DataStore
from src.data_store.data_type import Bytes
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


PUBLIC_GROUP = "public"


class TableReference(DataStore, version=1):
    data_token = DataToken("table_reference", PUBLIC_GROUP)

    table_name: Column[str]
    data_group: Column[str]
    data_store_type: Column[str]
    data_store_version: Column[int]

    def data_tokens(self: TableReference) -> list[DataToken]:
        return [DataToken(row.table_name, row.data_group) for _, row in self.iterrows()]

    @staticmethod
    def create_row(data_token: DataToken, data_store_type: Type[T]) -> TableReference:
        return TableReference(
            table_name=data_token.table_name,
            data_group=data_token.data_group,
            schema_name=data_store_type.__name__,
            schema_version=data_store_type.version,
        )


class DataStoreReference(DataStore, version=1):
    data_token = DataToken("data_store_reference", PUBLIC_GROUP)

    data_store_type: Column[str]
    data_store_version: Column[int]
    definition_reference: Column[str]
    definition_version: Column[int]

    def data_store_versions(self: DataStoreReference) -> dict[str, set[int]]:
        store_versions = {}
        for _, row in self.iterrows():
            if row.data_store_type not in store_versions:
                store_versions[row.data_store_type] = set()
            store_versions[row.data_store_type].add(row.version)
        return store_versions

    @staticmethod
    def create_row(data_store_type: Type[T]) -> TableReference:
        reference_token = DataStoreDefinition.data_token(data_store_type)
        return DataStoreReference(
            data_store_name=data_store_type.__name__,
            data_store_version=data_store_type.version,
            definition_reference=reference_token.table_name,
            definition_version=DataStoreDefinition.version,
        )


class DataStoreDefinition(DataStore, version=1):
    column_name: Column[str]
    column_type: Column[Bytes]

    @staticmethod
    def data_token(data_store_type: Type[T]) -> DataToken:
        return DataToken(
            f"{data_store_type.__name__}V{data_store_type.version}_definition",
            DataStoreDefinition.version,
        )

    def build(
        self: DataStoreDefinition, data_store_type: Type[T]
    ) -> DataStoreDefinition:
        builder = self.builder()
        for name, column in data_store_type._parse_columns().items():
            builder.append_row(column_name=name, column_type=pickle.dumps(column.dtype))
        return builder.build()

    def store_class(
        self: DataStoreDefinition, store_type: str, store_version: int
    ) -> Type[T]:
        annotations = {}
        for _, name, type in self.itertuples():
            annotations[name] = f"Column[{type}]"

        def add_columns(ns: dict[str, Any]) -> dict[str, Any]:
            ns["__annotations__"] = annotations

        return new_class(
            store_type, (DataStore,), {"version": store_version}, add_columns
        )


class DatabaseCorruptionError(IOError):
    def __init__(self, reason: str) -> None:
        super(DatabaseCorruptionError, self).__init__(
            f"Database in corrupted state: {reason}"
        )


class MissingTableError(IOError):
    def __init__(self, data_token: DataToken) -> None:
        super(DatabaseCorruptionError, self).__init__(
            f"Failed to find table for {data_token}"
        )


class Database:
    _db_adapter: DatabaseAdapter
    query: QueryAlias[T]

    def __init__(self, database_adapter: DatabaseAdapter) -> None:
        self._db_adapter = database_adapter
        if not self._has_reference_tables():
            self._setup_reference_tables()
        self._validate_reference_tables()
        self.query = Database.QueryAlias[T](self)

    @staticmethod
    def backend(data_token: DataToken, read_only: bool = True) -> DatabaseBackend:
        return DatabaseBackend(data_token, read_only=read_only)

    def _has_reference_tables(self):
        return self._db_adapter.has_table(
            TableReference.data_token
        ) and self._db_adapter.has_table(DataStoreReference.data_token)

    def _setup_reference_tables(self):
        self._db_adapter.create_table(TableReference.data_token, TableReference)
        self._db_adapter.create_table(DataStoreReference.data_token, DataStoreReference)
        self._register_table(TableReference.data_token)
        self._register_table(DataStoreReference.data_token)
        self._register_data_store_type(TableReference)
        self._register_data_store_type(DataStoreReference)

    def _validate_reference_tables(self):
        pass

    def _register_table(self, data_token: DataToken, data_store_type: Type[T]) -> None:
        if not self.has_table(data_token):
            reference = TableReference.create_row(data_token, data_store_type)
            self._db_adapter.insert(
                TableReference.data_token, reference, ignore_index=True
            )

    def _register_data_store_type(self, data_store_type: Type[T]) -> None:
        if not self._has_data_store_type(data_store_type):
            reference = DataStoreReference.create_row(data_store_type)
            self._db_adapter.insert(
                DataStoreReference.data_token, reference, ignore_index=True
            )
            reference_token = DataStoreDefinition.data_token(data_store_type)
            self._db_adapter.insert(
                reference_token, DataStoreDefinition.build(data_store_type)
            )

    def _create_table(self, data_token: DataToken, data_store_type: Type[T]) -> None:
        self._db_adapter.create_table(data_token, data_store_type)
        self._register_table(data_token)
        self._register_data_store_type(data_store_type)

    def _table_data_store_type_version(self, data_token: DataToken) -> tuple[str, int]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            table_data = self.query[TableReference](
                TableReference.data_token,
                (TableReference.table_name == data_token.table_name)
                & (TableReference.table_name == data_token.table_name),
            )
            if len(table_data) == 0:
                raise MissingTableError(data_token)
            if len(table_data) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate table references found for {data_token}"
                )
            return (table_data.data_store_type, table_data.data_store_version)

    def has_table(self, data_token: DataToken):
        return data_token in set(self.list_tables())

    def list_tables(self) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            table_data = self.query[TableReference](TableReference.data_token)
            return table_data.data_tokens()

    def has_group(self, data_group: str) -> bool:
        return data_group in set(self.list_groups())

    def list_groups(self) -> list[str]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            data = self.query[TableReference](TableReference.data_token)
            return data.data_group.tolist()

    def list_group_tables(self, data_group: str) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            criteria = TableReference.data_group == data_group
            data = self.query[TableReference](TableReference.data_token, criteria)
            return data.data_tokens()

    def _has_data_store_type(self, data_store_type: Type[T]):
        type_versions = self._data_store_type_versions()
        return (
            data_store_type.__name__ in type_versions
            and data_store_type.version in type_versions[data_store_type.__name__]
        )

    def _data_store_type_versions(self) -> dict[str, set[int]]:
        if not self._db_adapter.has_table(DataStoreReference.data_token):
            raise DatabaseCorruptionError("DataStoreReference table is missing")
        else:
            data = self.query[DataStoreReference](DataStoreReference.data_token)
            return data.data_store_versions()

    def _definition_reference_version(
        self, store_type: str, store_version: int
    ) -> tuple[DataToken, int]:
        if not self._db_adapter.has_table(DataStoreReference.data_token):
            raise DatabaseCorruptionError("DataStoreReference table is missing")
        else:
            store_data = self.query[DataStoreReference](
                DataStoreReference.data_token,
                (DataStoreReference.data_store_type == store_type)
                & (DataStoreReference.data_store_version == store_version),
            )
            if len(store_data) == 0:
                raise DatabaseCorruptionError(
                    f"Data store definition {store_type} V{store_version} missing from DataStoreReference"
                )
            if len(store_data) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate data store references found for {store_type} V{store_version}"
                )
            return (
                DataToken(store_data.definition_reference, PUBLIC_GROUP),
                store_data.definition_version,
            )

    def _data_store_definition(
        self, reference_token: DataToken, definition_version: int
    ) -> DataStoreDefinition:
        if not self._db_adapter.has_table(reference_token):
            raise DatabaseCorruptionError(f"{reference_token} table is missing")
        else:
            if definition_version > 1:
                raise UnsupportedOperation(
                    f"Cannot deserialise data store type for version {definition_version}"
                )
            self.query(reference_token)

    def _query(
        self: Database,
        data_token: DataToken,
        column_query: Optional[ColumnQuery] = None,
    ) -> T:
        table_data = self._db_adapter.query(data_token, column_query)
        store_type, store_version = self._table_data_store_type_version(data_token)
        reference_token, definition_version = self._definition_reference_version(
            store_type, store_version
        )
        store_definition = self._data_store_definition(
            reference_token, definition_version
        )
        store_class: Type[T] = store_definition.store_class(store_type, store_version)
        return store_class(table_data)

    def insert(
        self: Database, data_token: DataToken, data_store: T, ignore_index: bool = False
    ) -> None:
        ...

    def update(
        self: Database, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def upsert(
        self: Database, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def drop_indices(self: Database, data_token: DataToken, indices: Index) -> None:
        ...

    def drop_table(self: Database, data_token: DataToken) -> None:
        ...

    def drop_group(self: Database, data_group: str) -> None:
        ...

    def copy_table(
        self: Database, source_data_token: DataToken, target_data_token: DataToken
    ) -> None:
        ...

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

    class QueryAlias(Generic[T]):
        _database: Database
        _data_store_type: Type[T]

        def __init__(
            self,
            data_base: Database,
            data_store_type: Type[T] = Any,
        ) -> None:
            self._database = data_base
            self._data_store_type = data_store_type

        def __getitem__(self, data_store_type: Type[T]) -> Database.QueryAlias[T]:
            return Database.QueryAlias[T](self._database, data_store_type)

        def __call__(
            self, data_token: DataToken, column_query: Optional[ColumnQuery] = None
        ) -> T:
            return self._database._query(data_token, column_query)
