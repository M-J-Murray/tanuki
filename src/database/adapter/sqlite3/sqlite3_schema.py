from src.database.adapter.database_schema import DatabaseSchema
from typing import Type, TypeVar
from src.data_store.data_store import DataStore
from .sqlite3_type import Sqlite3Type

T = TypeVar("T", bound=DataStore)


class Sqlite3Schema(DatabaseSchema):
    _columns: list[str]

    def __init__(self, store_type: Type[T]) -> None:
        self._columns = [
            f"{col.name} {Sqlite3Type(col.dtype)}" for col in store_type.columns
        ]

    def __str__(self) -> str:
        return ", ".join(self._columns)