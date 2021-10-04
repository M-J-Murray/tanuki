from __future__ import annotations

from typing import Any, TYPE_CHECKING, TypeVar

import numpy as np

from .index import Index
from .pandas_index import PandasIndex, PIndex

if TYPE_CHECKING:
    from tanuki.data_backend.database_backend import DatabaseBackend
    from tanuki.data_store.column_alias import ColumnAlias

C = TypeVar("C", bound=tuple["ColumnAlias", ...])


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

    def to_pandas(self) -> PandasIndex[C]:
        df = self._data.to_pandas()
        index = df.set_index(self.columns).index
        index.name = self.name
        return PandasIndex(index, self.columns)

    def __getitem__(self, item) -> Index[C]:
        return self.to_pandas()[item]

    @property
    def values(self) -> np.ndarray:
        return self.to_pandas().values

    def tolist(self) -> list:
        return self.to_pandas().tolist()

    def equals(self, other: DatabaseIndex) -> bool:
        if type(other) is not DatabaseIndex:
            return False
        return np.array_equal(self._data.values, other._data.values)

    def __eq__(self, other) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data == other

    def __ne__(self, other: Any) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data != other

    def __gt__(self, other: Any) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data > other

    def __ge__(self, other: Any) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data >= other

    def __lt__(self, other: Any) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data < other

    def __le__(self, other: Any) -> DatabaseIndex:
        if type(other) is DatabaseIndex:
            other = other._data
        return self._data <= other

    def __len__(self) -> int:
        return len(self._data)

    def __str__(self) -> str:
        result = f"Index {self.name}"
        if len(self._data) == 0:
            return f"{result}([])"
        else:
            return str(self._data)

    def __repr__(self) -> str:
        return str(self)
