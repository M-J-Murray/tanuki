from __future__ import annotations

from typing import Any, Generic, Optional, Type, TYPE_CHECKING, TypeVar, Union

import numpy as np
from pandas import Index as PIndex
from pandas.core.frame import DataFrame

from src.data_backend.pandas_backend import PandasBackend
from src.data_store.data_store import DataStore
from src.data_store.index.database_index import DatabaseIndex
from src.data_store.index.index import Index
from src.data_store.index.index_alias import IndexAlias
from src.data_store.index.pandas_index import PandasIndex
from src.data_store.query import (
    AndGroupQuery,
    ColumnQuery,
    DataStoreQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrGroupQuery,
    Query,
)
from src.database.data_token import DataToken
from src.database.database import Database

from .data_backend import DataBackend, ILocIndexer, LocIndexer

T = TypeVar("T", bound="DataStore")


class DatabaseBackend(Generic[T], DataBackend):
    _store_class: Type[T]
    _database: Database
    _data_token: DataToken
    _read_only: bool

    _selected_columns: list[str]

    _index: DatabaseIndex
    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self: "DatabaseBackend",
        store_class: Type[T],
        database: Database,
        data_token: DataToken,
        index: Optional[DatabaseIndex] = None,
        selected_columns: Optional[list[str]] = None,
        read_only: bool = True,
    ) -> None:
        if not read_only:
            raise NotImplementedError("The current version of Tanuki does not support Store to DB writing")
        self._store_class = store_class
        self._database = database
        self._data_token = data_token
        self._read_only = read_only

        if selected_columns is None:
            self._selected_columns = self._database.table_columns(self._data_token)
        else:
            self._selected_columns = selected_columns

        if index is None:
            pindex = PIndex(np.arange(0, len(self)), name="index")
            self._index = PandasIndex(pindex, [])
        else:
            if not isinstance(index, DatabaseIndex) and not isinstance(
                index, PandasIndex
            ):
                index = PandasIndex(index)
            self._index = index
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
        return self.to_pandas().values

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
        return self._index

    @property
    def loc(self: DatabaseBackend) -> LocIndexer[DatabaseBackend]:
        return self._loc

    @property
    def iloc(self: DatabaseBackend) -> ILocIndexer[DatabaseBackend]:
        return self._iloc

    def equals(self, other: DatabaseBackend):
        if type(other) is not DatabaseBackend:
            return False
        return (
            self._data_token == other._data_token
            and self._store_class == other._store_class
            and self._selected_columns == other._selected_columns
        )

    def __eq__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                EqualsQuery, AndGroupQuery, OrGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                EqualsQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __ne__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                NotEqualsQuery, OrGroupQuery, AndGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                NotEqualsQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __gt__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                GreaterThanQuery, AndGroupQuery, OrGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                GreaterThanQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __ge__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                GreaterEqualQuery, AndGroupQuery, OrGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                GreaterEqualQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __lt__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                LessThanQuery, AndGroupQuery, OrGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                LessThanQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __le__(self, other: Any) -> Query:
        if type(other) is PandasBackend:
            return DataStoreQuery(
                LessEqualQuery, AndGroupQuery, OrGroupQuery, other.to_pandas()
            )
        else:
            return ColumnQuery(
                LessEqualQuery,
                AndGroupQuery,
                self._selected_columns,
                other,
            )

    def __len__(self):
        return self._database.row_count(self._data_token)

    def __iter__(self):
        return iter(self._selected_columns)

    def iterrows(self):
        return self.query().iterrows()

    def itertuples(self):
        return self.query().itertuples()

    def __getitem__(self, item: str) -> Any:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            selected_columns=[item],
            read_only=self._read_only,
        )

    def getitems(self, items: list[str]) -> DatabaseBackend:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            selected_columns=items,
            read_only=self._read_only,
        )

    def __setitem__(self, item: str, value: Any) -> None:
        raise NotImplementedError("The current version of Tanuki does not support Store to DB writing")

    def getmask(self, mask: list[bool]) -> DatabaseBackend:
        indices = np.where(mask)[0]
        return self.query().iloc[indices]

    def get_index(self, index_alias: IndexAlias) -> Index:
        cols = [str(col) for col in index_alias.columns]
        return DatabaseIndex(index_alias.name, self.getitems(cols))

    def set_index(self, index: Union[Index, IndexAlias]) -> PandasBackend:
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            index=self.get_index(index),
            selected_columns=self._selected_columns,
            read_only=self._read_only,
        )

    def reset_index(self) -> PandasBackend:
        pindex = PIndex(np.arange(0, len(self)), name="index")
        return DatabaseBackend(
            self._store_class,
            self._database,
            self._data_token,
            index=PandasIndex(pindex, []),
            selected_columns=self._selected_columns,
            read_only=self._read_only,
        )

    def drop_indices(self, indices: list[int]) -> DatabaseBackend:
        raise NotImplementedError("The current version of Tanuki does not support Store to DB writing")

    def query(self, query: Optional[Query] = None) -> PandasBackend:
        datastore = self._database.query(
            self._store_class, self._data_token, query, self._selected_columns
        )
        if len(self.index.columns) > 0:
            datastore.set_index(self.index)
        return datastore._data_backend

    def append(
        clt: DatabaseBackend, new_backend: DatabaseBackend, ignore_index: bool = False
    ) -> DatabaseBackend:
        raise NotImplementedError("The current version of Tanuki does not support Store to DB writing")

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
        raise NotImplementedError("The current version of Tanuki does not support Store to DB writing")


class _ILocIndexer(ILocIndexer[DatabaseBackend]):
    _data_backend: DatabaseBackend

    def __init__(self, data_backend: DatabaseBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[int, list, slice]) -> DatabaseBackend:
        return self._data_backend.query().iloc[item]


class _LocIndexer(LocIndexer[DatabaseBackend]):
    _data_backend: DatabaseBackend

    def __init__(self, data_backend: DatabaseBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[Any, list, slice]) -> DatabaseBackend:
        return self._data_backend.query().loc[item]
