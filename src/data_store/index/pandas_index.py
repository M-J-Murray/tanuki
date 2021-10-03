from __future__ import annotations
import re

from typing import Any, Iterable, TYPE_CHECKING, TypeVar, Union

import numpy as np
from pandas import Index as PIndex

from .index import Index

if TYPE_CHECKING:
    from src.data_store.column_alias import ColumnAlias

C = TypeVar("C", bound=tuple["ColumnAlias", ...])


class PandasIndex(Index[C]):
    _data: PIndex
    _columns: list[str]

    def __init__(self, data: PIndex = PIndex([]), columns: list[str] = []) -> None:
        if not isinstance(data, Iterable) or isinstance(data, str):
            data = [data]
        if not isinstance(data, PIndex):
            data = PIndex(data, name="index")
        self._data = data
        self._columns = columns

    @property
    def name(self) -> Union[str, list[str]]:
        return self._data.name

    @property
    def columns(self) -> list[str]:
        return self._columns

    def to_pandas(self) -> PandasIndex[C]:
        return PandasIndex(self._data, self._columns)

    def __getitem__(self, item) -> Index[C]:
        result = self._data[item]
        if issubclass(type(result), PIndex):
            result.name = self.name
            return PandasIndex(result, self._columns)
        else:
            return result
        

    @property
    def values(self) -> np.ndarray:
        return self._data.values

    def tolist(self) -> list:
        return self._data.values.tolist()

    def equals(self, other: PandasIndex) -> bool:
        return (
            self.name == other.name
            and self._data.equals(other._data)
            and self.columns == other.columns
        )

    def __eq__(self, other) -> PandasIndex:
        if type(other) is PandasIndex:
            other = other._data
        return self._data == other

    def __ne__(self, other: Any) -> PandasIndex:
        if type(other) is PandasIndex:
            other = other._data
        return self._data != other

    def __gt__(self, other: Any) -> PandasIndex:
        if type(other) is PandasIndex:
            other = other._data
        return self._data > other

    def __ge__(self, other: Any) -> PandasIndex:
        if type(other) is PandasIndex:
            other = other._data
        return self._data >= other

    def __lt__(self, other: Any) -> PandasIndex:
        if type(other) is PandasIndex:
            other = other._data
        return self._data < other

    def __le__(self, other: Any) -> PandasIndex:
        if type(other) is PandasIndex:
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
