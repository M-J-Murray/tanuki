from __future__ import annotations
from io import UnsupportedOperation
from src.database.adapter.query.pandas_query_compiler import PandasQueryCompiler
from src.data_store.query_type import QueryType

from typing import Any, Generator, cast, Iterable, Optional, Union

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
            elif len(values) == 1:
                self._data = DataFrame(data, index=index).iloc[0]
            else:
                self._data = DataFrame(data, index=index)
        else:
            raise ValueError(f"Received unexpected value type {type(data)}: {data}")
        self._loc = _LocIndexer(self)
        self._iloc = _ILocIndexer(self)

    def to_pandas(self) -> Union[Series, DataFrame]:
        return self._data

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

    def to_table(self) -> PandasBackend:
        if self.is_row():
            return PandasBackend(DataFrame([self._data], columns=self._data.index))
        else:
            return PandasBackend(DataFrame(self._data))

    def to_row(self) -> PandasBackend:
        if self.is_row():
            return PandasBackend(Series(self._data))
        elif len(self) == 1:
            return self.iloc[0]
        else:
            raise UnsupportedOperation(
                f"Cannot convert table with {len(self)} rows to singular row"
            )

    @property
    def index(self) -> Index:
        return self._data.index

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
        if self.is_row() != oc.is_row():
            if self.is_row():
                if len(oc) > 1:
                    return False
                else:
                    return self._data.equals(oc.to_row()._data)
            else:
                if len(self) > 1:
                    return False
                else:
                    return self.to_row()._data.equals(oc._data)
        else:
            return self._data.equals(oc._data)

    def __eq__(self, other) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data == other

    def __ne__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data != other

    def __gt__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data > other

    def __ge__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data >= other

    def __lt__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data < other

    def __le__(self, other: Any) -> DataFrame:
        if issubclass(type(other), PandasBackend):
            other = self._data
        return self._data <= other

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Generator[str, None, None]:
        return iter(self._data)

    def iterrows(self) -> Generator[tuple[int, PandasBackend], None, None]:
        for i, row in self._data.iterrows():
            yield (i, PandasBackend(row))

    def itertuples(self):
        return self._data.itertuples()

    def __getitem__(self, item: Union[str, list[bool]]) -> Any:
        result = self._data[item]
        if type(result) is DataFrame:
            result = PandasBackend(result)
        return result

    def getitems(self, item: list[str]) -> PandasBackend:
        return PandasBackend(self._data[item])

    def query(self, query_type: QueryType) -> PandasBackend:
        if self.is_row():
            raise ValueError("Cannot query from backend when data is in row format")
        query_compiler = PandasQueryCompiler(self._data)
        query = query_compiler.compile(query_type)
        return PandasBackend(self._data[query])

    def __setitem__(self, item: str, value: Union[Any, Column]) -> Column:
        if self.is_row():
            if type(value) is Column:
                self._data[item] = value.series.iloc[0]
            else:
                self._data[item] = value
        else:
            self._data[item] = value.series

    def set_index(self: PandasBackend, column: Union[str, Iterable]) -> PandasBackend:
        return PandasBackend(self._data.set_index(column))

    def reset_index(self: PandasBackend, drop: bool = False) -> PandasBackend:
        return PandasBackend(self._data.reset_index(drop=drop))

    @classmethod
    def concat(
        cls: type[PandasBackend],
        all_backends: list[PandasBackend],
        ignore_index: bool = False,
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
