from __future__ import annotations

import pickle
from types import new_class
from typing import Any, Type, TypeVar

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.data_store.data_type import Bytes
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


PROTECTED_GROUP = "tanuki_protected"

class TableReference(DataStore, version=1):
    data_token = DataToken(f"table_reference", PROTECTED_GROUP)

    table_name: Column[str]
    data_group: Column[str]
    store_type: Column[str]
    store_version: Column[int]
    protected: Column[bool]

    def data_tokens(self: TableReference) -> list[DataToken]:
        return [DataToken(row.table_name, row.data_group) for _, row in self.iterrows()]

    @staticmethod
    def create_row(
        data_token: DataToken, store_type: Type[T], protected: bool = False
    ) -> TableReference:
        return TableReference(
            table_name=data_token.table_name,
            data_group=data_token.data_group,
            store_type=store_type.__name__,
            store_version=store_type.version,
            protected=protected,
        )


class StoreReference(DataStore, version=1):
    data_token = DataToken(f"store_reference", PROTECTED_GROUP)

    store_type: Column[str]
    store_version: Column[int]
    definition_reference: Column[str]
    definition_version: Column[int]

    @staticmethod
    def store_versions(data_rows: list[tuple]) -> dict[str, set[int]]:
        store_versions: dict[str, set[int]] = {}
        for type, version, _, _ in data_rows:
            if type not in store_versions:
                store_versions[type] = set()
            store_versions[type].add(version)
        return store_versions

    @staticmethod
    def create_row(store_type: Type[T]) -> TableReference:
        reference_token = StoreDefinition.data_token(store_type)
        return StoreReference(
            store_type=store_type.__name__,
            store_version=store_type.version,
            definition_reference=reference_token.table_name,
            definition_version=StoreDefinition.version,
        )


class StoreDefinition(DataStore, version=1):
    column_name: Column[str]
    column_type: Column[Bytes]

    @staticmethod
    def data_token(store_type: Type[T]) -> DataToken:
        return DataToken(
            f"{store_type.__name__}_v{store_type.version}_definition",
            PROTECTED_GROUP,
        )

    @staticmethod
    def from_type(store_type: Type[T]) -> StoreDefinition:
        builder = StoreDefinition.builder()
        for name, column in store_type._parse_columns().items():
            builder.append_row(column_name=name, column_type=pickle.dumps(column.dtype))
        return builder.build()

    def store_class(
        self: StoreDefinition,
        store_type: str,
        store_version: int,
    ) -> Type[T]:
        annotations = {}
        for _, name, type in self.itertuples():
            annotations[name] = Column[pickle.loads(type)]

        functions = {}
        if store_type in DataStore.registered_stores:
            active_type = DataStore.registered_stores[store_type]
            active_keys = set(dir(active_type)) - set(dir(DataStore))
            active_keys -= {str(col) for col in active_type.columns}
            active_keys.remove("columns")
            active_keys.remove("version")
            for key in active_keys:
                functions[key] = getattr(active_type, key)

        def add_columns(ns: dict[str, Any]) -> dict[str, Any]:
            ns["__annotations__"] = annotations
            ns |= functions

        return new_class(
            store_type, (DataStore,), {"version": store_version, "register": False}, add_columns
        )
