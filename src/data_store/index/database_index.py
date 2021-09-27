from __future__ import annotations

from typing import TypeVar

import numpy as np

from src.data_store.column_alias import ColumnAlias

from .index import Index
from .pandas_index import PandasIndex

C = TypeVar("C", bound=tuple[ColumnAlias, ...])


class DatabaseIndex(Index[C]):
    _name: str
    _data: DatabaseBackend

    def __init__(self, name: str, data: DatabaseBackend) -> None:
        self._name = name
        self._data = data

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> list[str]:
        return self._data.columns

    def __getitem__(self, item) -> Index[C]:
        index_data = np.array(self.tolist())
        return PandasIndex(index_data[item], self.columns)

    def tolist(self) -> list:
        return self._data.index.tolist()

    def __str__(self) -> str:
        result = f"Index {self.name}"
        if len(self._data) == 0:
            return f"{result}([])"
        else:
            return str(self._data)

    def __repr__(self) -> str:
        return str(self)


from src.data_backend.database_backend import DatabaseBackend
