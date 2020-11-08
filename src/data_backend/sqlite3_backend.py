from __future__ import annotations

from typing import Any, cast, Optional, Union

from pandas import Index
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.column import Column
from src.database.data_token import DataToken

from .data_backend import DataBackend, ILocIndexer, LocIndexer


class Sqlite3Backend(DataBackend):
    _data_token: DataToken
    _read_only: bool
    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(self, data_token: DataToken, read_only: bool) -> None:
        self._data_token = data_token
        self._read_only = read_only
        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def columns(self) -> list[str]:
        if self.is_row():
            return self._data.index
        else:
            return self._data.columns

    def to_dict(self, orient) -> dict[str, any]:
        if self.is_row():
            return self._data.to_dict()
        else:
            return self._data.to_dict(orient)

    def is_row(self) -> bool:
        return type(self._data) == Series

    def index(self) -> Index:
        return self._data.index

    def loc(self: Sqlite3Backend) -> LocIndexer[Sqlite3Backend]:
        return self._loc

    def iloc(self: Sqlite3Backend) -> ILocIndexer[Sqlite3Backend]:
        return self._iloc

    def __eq__(self, other):
        if type(other) is not Sqlite3Backend:
            return False
        oc = cast(Sqlite3Backend, other)
        return self._data == oc._data

    def equals(self, other):
        if type(other) is not Sqlite3Backend:
            return False
        oc = cast(Sqlite3Backend, other)
        return self._data.equals(oc._data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def iterrows(self):
        for i, row in self._data.iterrows():
            yield (i, Sqlite3Backend(row))

    def itertuples(self):
        return self._data.itertuples()

    def __getitem__(self, item: str) -> Any:
        if self.is_row():
            return self._data[item]
        else:
            return Column(self._data[item])

    def __setitem__(self, item: str, value: Column) -> Column:
        self._data[item] = value.series

    def reset_index(self: Sqlite3Backend, drop: bool = False) -> Sqlite3Backend:
        return Sqlite3Backend(self._data.reset_index(drop=drop))

    def __str__(self) -> str:
        if type(self._data) == Series:
            return " Row\n" + str(self._data)
        else:
            return "\n" + str(self._data)

    def __repr__(self) -> str:
        return str(self)


class _ILocIndexer(ILocIndexer[Sqlite3Backend]):
    _data_backend: Sqlite3Backend

    def __init__(self, data_backend: Sqlite3Backend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[int, list, slice]) -> Sqlite3Backend:
        return Sqlite3Backend(self._data_backend._data.iloc[item])


class _LocIndexer(LocIndexer[Sqlite3Backend]):
    _data_backend: Sqlite3Backend

    def __init__(self, data_backend: Sqlite3Backend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[Any, list, slice]) -> Sqlite3Backend:
        return Sqlite3Backend(self._data_backend._data.iloc[item])
