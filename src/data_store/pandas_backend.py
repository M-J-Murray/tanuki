from __future__ import annotations

from typing import Any, Optional, Union

from numpy import ndarray
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.column import Column

from .data_backend import DataBackend, ILocIndexer, LocIndexer


class PandasBackend(DataBackend):
    _data: Union[Series, DataFrame]
    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self, data: Optional[Union(Series, DataFrame, dict[str, Column])] = None
    ) -> None:
        if data is None:
            self._data = DataFrame(dtype="object")
        elif type(data) is Series or type(data) is DataFrame:
            self._data = data
        elif type(data) is dict:
            values = next(iter(data.values()))
            if type(values) is not list:
                self._data = Series(data)
            elif len(values) > 1:
                self._data = DataFrame(data)
            else:
                self._data = DataFrame(data).iloc[0]
        else:
            raise ValueError(f"Received unexpected value type {type(data)}: {data}")
        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def columns(self) -> set[str]:
        if self.is_row():
            return self._data.index
        else:
            return self._data.columns

    def is_row(self) -> bool:
        return type(self._data) == Series

    def index(self) -> ndarray:
        return self._data.index

    def loc(self: PandasBackend) -> LocIndexer[PandasBackend]:
        return self._loc

    def iloc(self: PandasBackend) -> ILocIndexer[PandasBackend]:
        return self._iloc

    def __eq__(self, other):
        return self._data == other

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, item: str) -> Any:
        if self.is_row():
            return self._data[item]
        else:
            return Column(self._data[item])

    def __setitem__(self, item: str, value: Column) -> Column:
        self._data[item] = value.series

    def reset_index(self: PandasBackend, drop: bool = False) -> PandasBackend:
        return PandasBackend(self._data.reset_index(drop=drop))

    def __str__(self) -> str:
        if type(self._data) == Series:
            return " Row\n" + str(self._data)
        else:
            return "\n" + str(self._data)

    def __repr__(self) -> str:
        return str(self)


class _ILocIndexer(ILocIndexer[PandasBackend]):
    _data_backend: PandasBackend

    def __init__(self, data_backend: PandasBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[int, list, slice]) -> PandasBackend:
        return PandasBackend(self._data_backend._data.iloc[item])


class _LocIndexer(LocIndexer[PandasBackend]):
    _data_backend: PandasBackend

    def __init__(self, data_backend: PandasBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[Any, list, slice]) -> PandasBackend:
        return PandasBackend(self._data_backend._data.iloc[item])
