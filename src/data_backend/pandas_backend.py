from __future__ import annotations

from typing import Any, cast, Generator, Iterable, Optional, Union

import numpy as np
import pandas as pd
from pandas import Index
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from .data_backend import DataBackend, ILocIndexer, LocIndexer


class PandasBackend(DataBackend):
    _data: DataFrame
    _loc: _LocIndexer
    _iloc: _ILocIndexer

    def __init__(
        self,
        data: Optional[Union(Series, DataFrame, dict[str, list])] = None,
        index: Optional[Union[Iterable]] = None,
    ) -> None:
        if data is None:
            self._data = DataFrame(dtype="object")
            self._data.index.rename("index", inplace=True)
        elif type(data) is Series:
            self._data = cast(Series, data).to_frame().transpose()
            self._data.index.rename("index", inplace=True)
        elif type(data) is DataFrame:
            self._data = DataFrame(data)
        elif type(data) is dict:
            sample_value = next(iter(data.values()))
            if (
                not isinstance(sample_value, Iterable)
                or isinstance(sample_value, str)
            ):
                if index is None:
                    index = 0
                data["index"] = index
                self._data = Series(data).to_frame().transpose()
                self._data.set_index("index", inplace=True)
            else:
                self._data = DataFrame(data, index=index)
                self._data.index.rename("index", inplace=True)
        else:
            raise ValueError(f"Received unexpected value type {type(data)}: {data}")
        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def to_pandas(self) -> Union[Series, DataFrame]:
        return self._data

    @property
    def columns(self) -> list[str]:
        return self._data.columns.tolist()

    @property
    def values(self) -> np.ndarray:
        data_values = self._data.values
        shape = data_values.shape
        if shape[1] == 1:
            return np.squeeze(data_values, axis=1)
        elif shape[0] == 1:
            return np.squeeze(data_values, axis=0)
        else:
            return data_values

    @property
    def dtypes(self) -> dict[str, type]:
        return self._data.dtypes

    def cast_columns(self, column_dtypes: dict[str, type]) -> PandasBackend:
        return PandasBackend(self._data.astype(column_dtypes))

    def to_dict(self) -> dict[str, any]:
        return self._data.to_dict("list")

    @property
    def index(self) -> Index:
        return self._data.index

    @property
    def index_name(self) -> Union[str, list[str]]:
        return self._data.index.name

    @property
    def loc(self: PandasBackend) -> LocIndexer[PandasBackend]:
        return self._loc

    @property
    def iloc(self: PandasBackend) -> ILocIndexer[PandasBackend]:
        return self._iloc

    def equals(self, other) -> bool:
        if type(other) is not PandasBackend:
            return False
        oc = cast(PandasBackend, other)
        return np.array_equal(self._data.values, oc._data.values)

    def __eq__(self, other) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data == other

    def __ne__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data != other

    def __gt__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data > other

    def __ge__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data >= other

    def __lt__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data < other

    def __le__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = other._data
        return self._data <= other

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Generator[str, None, None]:
        return iter(self._data)

    def iterrows(self) -> Generator[tuple[int, PandasBackend], None, None]:
        for i, row in self._data.iterrows():
            yield (i, PandasBackend(row.to_frame().transpose()))

    def itertuples(self, ignore_index: bool = False):
        for values in self._data.itertuples(index=not ignore_index):
            yield values

    def __getitem__(self, item: str) -> Any:
        return PandasBackend(self._data[[item]])

    def getitems(self, items: list[str]) -> PandasBackend:
        return PandasBackend(self._data[items])

    def getmask(self, mask: list[bool]) -> PandasBackend:
        return PandasBackend(self._data[mask])

    def query(self, query: "Query") -> PandasBackend:
        from src.database.adapter.query.pandas_query_compiler import PandasQueryCompiler
        query_compiler = PandasQueryCompiler(self._data)
        query = query_compiler.compile(query)
        return PandasBackend(self._data[query])

    def __setitem__(self, items: str, value: Any) -> None:
        if isinstance(value, PandasBackend):
            value = value._data
        self._data[items] = value

    def set_index(self: PandasBackend, column: Union[str, Iterable]) -> PandasBackend:
        return PandasBackend(self._data.set_index(column))

    def reset_index(self: PandasBackend, drop: bool = False) -> PandasBackend:
        return PandasBackend(self._data.reset_index(drop=drop))

    def append(
        self: PandasBackend, new_backend: PandasBackend, ignore_index: bool
    ) -> PandasBackend:
        return PandasBackend(
            self._data.append(new_backend._data, ignore_index=ignore_index)
        )

    @classmethod
    def concat(
        cls: type[PandasBackend],
        all_backends: list[PandasBackend],
        ignore_index: bool = False,
    ) -> PandasBackend:
        all_data = [backend._data for backend in all_backends]
        return PandasBackend(pd.concat(all_data, ignore_index=ignore_index))

    def __str__(self) -> str:
        return str(self._data)

    def __repr__(self) -> str:
        return str(self)


class _ILocIndexer(ILocIndexer[PandasBackend]):
    _data_backend: PandasBackend

    def __init__(self, data_backend: PandasBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[int, list, slice]) -> PandasBackend:
        if not isinstance(item, Iterable) or isinstance(item, str):
            item = [item]
        result = self._data_backend._data.iloc[item]
        return PandasBackend(result)


class _LocIndexer(LocIndexer[PandasBackend]):
    _data_backend: PandasBackend

    def __init__(self, data_backend: PandasBackend) -> None:
        self._data_backend = data_backend

    def __getitem__(self, item: Union[Any, list, slice]) -> PandasBackend:
        if not isinstance(item, Iterable) or isinstance(item, str):
            item = [item]
        result = self._data_backend._data.loc[item]
        return PandasBackend(result)

from src.data_store.query import Query
