from __future__ import annotations

from typing import Any, Generic, Iterable, Optional, Type, TypeVar, Union

import numpy as np
from pandas import Index
from pandas.core.frame import DataFrame

from src.data_backend.pandas_backend import PandasBackend
from src.data_store.data_store import DataStore
from src.data_store.query import (
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    MultiAndQuery,
    NotEqualsQuery,
    Query,
)
from src.database.data_token import DataToken
from src.database.database import Database

from .data_backend import DataBackend, ILocIndexer, LocIndexer

T = TypeVar("T", bound=DataStore)


class DatabaseBackend(Generic[T], DataBackend):
    _store_class: Type[T]
    _database: Database
    _data_token: DataToken
    _read_only: bool

    _selected_columns: list[str]

    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self: "DatabaseBackend",
        store_class: Type[T],
        database: Database,
        data_token: DataToken,
        selected_columns: Optional[list[str]] = None,
        read_only: bool = True,
    ) -> None:
        self._store_class = store_class
        self._database = database
        self._data_token = data_token
        self._read_only = read_only

        if selected_columns is None:
            self._selected_columns = self._database.table_columns(self._data_token)
        else:
            self._selected_columns = selected_columns

        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def is_link(self) -> bool:
        return True

    def link_token(self) -> Optional[DataToken]:
        return self._data_token

    def to_pandas(self) -> DataFrame:
        return self.query().to_pandas()

    @property
    def values(self) -> np.ndarray:
        df = self.to_pandas()
        if self._selected_columns == ["index"]:
            df = df.index
        return df.values

    @property
    def columns(self) -> list[str]:
        return self._selected_columns

    @property
    def dtypes(self) -> dict[str, type]:
        return self._database.table_dtypes(self._data_token)

    def to_dict(self) -> dict[str, any]:
        return self.query().to_dict()

    @property
    def index(self) -> Index:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            ["index"],
            self._read_only,
        )

    @property
    def loc(self: DatabaseBackend) -> LocIndexer[DatabaseBackend]:
        return self._loc

    @property
    def iloc(self: DatabaseBackend) -> ILocIndexer[DatabaseBackend]:
        return self._iloc

    def equals(self, other):
        raise NotImplementedError()

    def __eq__(self, other: Any) -> Query:
        return MultiAndQuery(EqualsQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __ne__(self, other: Any) -> Query:
        return MultiAndQuery(NotEqualsQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __gt__(self, other: Any) -> Query:
        return MultiAndQuery(GreaterThanQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __ge__(self, other: Any) -> Query:
        return MultiAndQuery(GreaterEqualQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __lt__(self, other: Any) -> Query:
        return MultiAndQuery(LessThanQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __le__(self, other: Any) -> Query:
        return MultiAndQuery(LessEqualQuery, self._selected_columns, [other for _ in self._selected_columns])

    def __len__(self):
        return self._database.row_count(self._data_token)

    def __iter__(self):
        raise NotImplementedError()

    def iterrows(self):
        raise NotImplementedError()

    def itertuples(self):
        raise NotImplementedError()

    def __getitem__(self, item: str) -> Any:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            [item],
            self._read_only,
        )

    def getitems(self, items: list[str]) -> DatabaseBackend:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            items,
            self._read_only,
        )

    def getmask(self, mask: list[bool]) -> DatabaseBackend:
        indices = np.where(mask)[0]
        return self.query(self.index == indices)

    def query(self, query: Optional[Query] = None) -> PandasBackend:
        return self._database.query(
            self._store_class, self._data_token, query, self._selected_columns
        )._data_backend

    def __setitem__(self, item: str, value: Any) -> None:
        raise NotImplementedError()

    def append(
        clt: DatabaseBackend, new_backend: DatabaseBackend, ignore_index: bool = False
    ) -> DatabaseBackend:
        raise NotImplementedError()

    def __str__(self: DatabaseBackend) -> str:
        return f"Database Link: {self._data_token}\nActive Columns: {self._selected_columns}"

    def __repr__(self: DatabaseBackend) -> str:
        return str(self)

    @classmethod
    def concat(
        cls: Type[DatabaseBackend],
        all_backends: list[DatabaseBackend],
        ignore_index: bool = False,
    ) -> DatabaseBackend:
        raise NotImplementedError()


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
