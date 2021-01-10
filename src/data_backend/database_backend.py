from __future__ import annotations

from typing import Any, Iterable, Type, Union

from pandas import Index
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.column import Column
from src.data_store.query_type import QueryType
from src.database.data_token import DataToken
from src.database.database import Database

from .data_backend import DataBackend, ILocIndexer, LocIndexer


class DatabaseBackend(DataBackend):
    _database: Database
    _data_token: DataToken
    _read_only: bool

    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self: "DatabaseBackend",
        database: Database,
        data_token: DataToken,
        read_only: bool = True,
    ) -> None:
        self._database = database
        self._data_token = data_token
        self._read_only = read_only

        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def to_pandas(self) -> Union[Series, DataFrame]:
        ...

    def columns(self) -> list[str]:
        ...

    def to_dict(self, orient) -> dict[str, any]:
        ...

    def is_row(self) -> bool:
        ...

    def to_table(self) -> DatabaseBackend:
        ...

    def to_row(self) -> DatabaseBackend:
        ...

    @property
    def index(self) -> Index:
        return self._data.index

    @property
    def loc(self: DatabaseBackend) -> LocIndexer[DatabaseBackend]:
        return self._loc

    @property
    def iloc(self: DatabaseBackend) -> ILocIndexer[DatabaseBackend]:
        return self._iloc

    def __eq__(self, other):
        ...

    def equals(self, other):
        ...

    def __len__(self):
        ...

    def __iter__(self):
        ...

    def iterrows(self):
        ...

    def itertuples(self):
        ...

    def __getitem__(self, item: str) -> Any:
        ...

    def query(self, query_type: QueryType) -> DatabaseBackend:
        ...

    def __setitem__(self, item: str, value: Column) -> Column:
        ...

    def set_index(
        self: DatabaseBackend, column: Union[str, Iterable]
    ) -> DatabaseBackend:
        ...

    def reset_index(self: DatabaseBackend, drop: bool = False) -> DatabaseBackend:
        ...

    def append(
        clt: DatabaseBackend, new_backend: DatabaseBackend, ignore_index: bool = False
    ) -> DatabaseBackend:
        ...

    @classmethod
    def concat(
        cls: Type[DatabaseBackend],
        all_backends: list[DatabaseBackend],
        ignore_index: bool = False,
    ) -> DatabaseBackend:
        ...


class _ILocIndexer(ILocIndexer[DatabaseBackend]):
    _data_backend: DatabaseBackend

    def __init__(self, data_backend: DatabaseBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[int, list, slice]) -> DatabaseBackend:
        return DatabaseBackend(self._data_backend._data.iloc[item])


class _LocIndexer(LocIndexer[DatabaseBackend]):
    _data_backend: DatabaseBackend

    def __init__(self, data_backend: DatabaseBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[Any, list, slice]) -> DatabaseBackend:
        return DatabaseBackend(self._data_backend._data.loc[item])
