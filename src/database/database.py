from __future__ import annotations

from types import TracebackType
from typing import Generic, Optional, Type, TypeVar

from pandas import Index

from src import data_store
from src.data_backend.data_backend import DataBackend
from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.adapter.database_schema import DatabaseSchema, SchemaDefinition
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)
B = TypeVar("B", bound=DataBackend)


PUBLIC_GROUP = "public"


class TableReference(DataStore):
    data_token = DataToken("table_reference", PUBLIC_GROUP)

    table_name: Column[str]
    data_group: Column[str]
    schema_name: Column[str]
    schema_version: Column[int]

    @staticmethod
    def create_row(data_token: DataToken, data_store_type: Type[T]) -> TableReference:
        return TableReference(
            table_name=data_token.table_name,
            data_group=data_token.data_group,
            schema_name=data_store_type.__name__,
            schema_version=data_store_type.version,
        )


class SchemaReference(DataStore):
    data_token = DataToken("schema_reference", PUBLIC_GROUP)

    schema_name: Column[str]
    schema_version: Column[int]
    schema_table_reference: Column[str]

    @staticmethod
    def schema_definition_token(data_store_type: Type[T]) -> DataToken:
        return DataToken(
            f"{data_store_type.__name__}V{data_store_type.version}_schema",
            SchemaDefinition.version,
        )

    @staticmethod
    def create_row(data_store_type: Type[T]) -> TableReference:
        schema_token = SchemaReference.schema_definition_token()
        return SchemaReference(
            schema_name=data_store_type.__name__,
            schema_version=data_store_type.version,
            schema_table_reference=schema_token.table_name,
        )


class Database(Generic[B]):
    _db_adapter: DatabaseAdapter[B]

    def __init__(self, database_adapter: DatabaseAdapter[B]) -> None:
        self._db_adapter = database_adapter
        if not self._has_reference_tables():
            self._setup_reference_tables()
        self._validate_reference_tables()

    def backend(self, data_token: DataToken, read_only: bool = True) -> B:
        return self._db_adapter.backend(data_token, read_only=read_only)

    def _has_reference_tables(self):
        pass

    def _setup_reference_tables(self):
        self._db_adapter.create_table(TableReference.data_token, TableReference)
        self._db_adapter.create_table(SchemaReference.data_token, SchemaReference)
        self._register_table(TableReference.data_token)
        self._register_table(SchemaReference.data_token)
        self._register_schema(TableReference, self._db_adapter.schema(TableReference))
        self._register_schema(SchemaReference, self._db_adapter.schema(SchemaReference))

    def _validate_reference_tables(self):
        pass

    def _register_table(self, data_token: DataToken, data_store_type: Type[T]) -> None:
        if not self.has_table(data_token):
            reference = TableReference.create_row(data_token, data_store_type)
            self._db_adapter.insert(
                TableReference.data_token, reference, ignore_index=True
            )

    def _register_schema(
        self, data_store_type: Type[T], schema_definition: SchemaDefinition
    ) -> None:
        if not self._has_schema(data_store_type):
            reference = SchemaReference.create_row(data_store_type)
            self._db_adapter.insert(
                SchemaReference.data_token, reference, ignore_index=True
            )
            schema_token = SchemaReference.schema_definition_token(data_store_type)
            self._db_adapter.insert(schema_token, schema_definition)

    def _create_table(self, data_token: DataToken, data_store_type: Type[T]) -> None:
        self._db_adapter.create_table(data_token, data_store_type)
        self._register_table()
        self._register_schema()

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

    def _has_schema(self, data_store_type: Type[T]):
        pass

    def _list_schema(self):
        pass

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
