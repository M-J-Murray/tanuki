from __future__ import annotations

from typing import Any, cast, Iterable, Optional, Union

import pandas as pd
from pandas import Index
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.column import Column

from .data_backend import DataBackend, ILocIndexer, LocIndexer


class PandasBackend(DataBackend):
    _data: Union[Series, DataFrame]
    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self,
        data: Optional[Union(Series, DataFrame, dict[str, Column])] = None,
        index: Optional[Iterable] = None,
    ) -> None:
        if data is None:
            self._data = DataFrame(dtype="object", index=index)
        elif type(data) is Series:
            self._data = Series(data, index=index)
        elif type(data) is DataFrame:
            self._data = DataFrame(data, index=index)
        elif type(data) is dict:
            values = next(iter(data.values()))
            if type(values) is not list:
                self._data = Series(data, index=index)
            elif len(values) > 1:
                self._data = DataFrame(data, index=index)
            else:
                self._data = DataFrame(data, index=index).iloc[0]
        else:
            raise ValueError(f"Received unexpected value type {type(data)}: {data}")
        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    @property
    def columns(self) -> list[str]:
        if self.is_row():
            return self._data.index.tolist()
        else:
            return self._data.columns.tolist()

    def to_dict(self, orient) -> dict[str, any]:
        if self.is_row():
            return self._data.to_dict()
        else:
            return self._data.to_dict(orient)

    def is_row(self) -> bool:
        return type(self._data) == Series

    @property
    def index(self) -> Index:
        return self._data.index

    @property
    def loc(self: PandasBackend) -> LocIndexer[PandasBackend]:
        return self._loc

    @property
    def iloc(self: PandasBackend) -> ILocIndexer[PandasBackend]:
        return self._iloc

    def __eq__(self, other):
        if type(other) is not PandasBackend:
            return False
        oc = cast(PandasBackend, other)
        return self._data == oc._data

    def equals(self, other):
        if type(other) is not PandasBackend:
            return False
        oc = cast(PandasBackend, other)
        return self._data.equals(oc._data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def iterrows(self):
        for i, row in self._data.iterrows():
            yield (i, PandasBackend(row))

    def itertuples(self):
        return self._data.itertuples()

    def __getitem__(self, item: str) -> Any:
        if self.is_row():
            return self._data[item]
        else:
            return Column(self._data[item])

    def __setitem__(self, item: str, value: Column) -> Column:
        self._data[item] = value.series

    def set_index(self: PandasBackend, column: Union[str, Iterable]) -> PandasBackend:
        return PandasBackend(self._data.set_index(column))

    def reset_index(self: PandasBackend, drop: bool = False) -> PandasBackend:
        return PandasBackend(self._data.reset_index(drop=drop))

    @classmethod
    def concat(
        cls: type[PandasBackend], all_backends: list[PandasBackend], ignore_index: bool = False
    ) -> PandasBackend:
        all_data = [backend._data for backend in all_backends]
        return cls(pd.concat(all_data, ignore_index=ignore_index))

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
        return PandasBackend(self._data_backend._data.loc[item])