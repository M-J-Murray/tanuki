from src.database.adapter.database_schema import DatabaseSchema
from typing import Type, TypeVar
from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)


class Sqlite3Schema(DatabaseSchema):
    _columns: list[str]

    def __init__(self, data_store_type: Type[T]) -> None:
        store_cols = data_store_type._parse_columns()
        self._columns = []
        for name, alias in store_cols:
            self._columns.append(f"{name} {alias}")

    def __str__(self) -> str:
        return "\n".join(self._columns)