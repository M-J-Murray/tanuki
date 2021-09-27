from __future__ import annotations

from types import GenericAlias
from typing import (
    Any,
    cast,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Type,
    TypeVar,
    Union,
)

import numpy as np
from pandas import Index

from src.data_store.data_type import Boolean, DataType, Object, TypeAlias

T = TypeVar("T")
NT = TypeVar("NT")
Indexible = Union[Any, list, Index]


B = TypeVar("B", bound="DataBackend")


class Column(Generic[T]):
    name: str
    _data_backend: B
    loc: Column._LocIndexer[T]
    iloc: Column._ILocIndexer[T]
    dtype: DataType

    @classmethod
    def __class_getitem__(_, dtype: T) -> Type[Column]:
        from .column_alias import ColumnAlias

        return ColumnAlias(dtype)

    def __init__(
        self: Column[T],
        name: str,
        data: Optional[list[T]] = None,
        index: Optional[list] = None,
        dtype: Optional[T] = None,
    ) -> None:
        self.name = name
        if data is not None:
            if issubclass(type(data), DataBackend):
                self._data_backend = data
            else:
                from src.data_backend.pandas_backend import PandasBackend
                self._data_backend = PandasBackend({name: data}, index=index)
            self.dtype = (
                self.infer_dtype(self.name, self._data_backend)
                if dtype is None
                else DataType(dtype)
            )
        else:
            from src.data_backend.pandas_backend import PandasBackend
            self._data_backend = PandasBackend(index=index)
            self.dtype = Object

        if data is not None:
            self._validate_column()
            self.loc = Column._LocIndexer[T](self)
            self.iloc = Column._ILocIndexer[T](self)

    @staticmethod
    def determine_nested_dtype(data: Iterable) -> DataType:
        if len(data) == 0:
            return Object
        sample = next(iter(data))
        stype = type(sample)
        if sample == data:
            return type(data)
        elif stype is list or stype is set:
            nested_type = Column.determine_nested_dtype(sample)
        else:
            nested_type = stype
        return GenericAlias(type(data), nested_type)

    @staticmethod
    def infer_dtype(column_name: str, data_backend: DataBackend) -> DataType:
        dtype: type = data_backend.dtypes[column_name]
        if dtype == Object:
            if len(data_backend) == 0:
                dtype = Object
            else:
                sample = data_backend.iloc[0].values[0]
                stype = type(sample)
                if stype is list or stype is set:
                    dtype = DataType(GenericAlias(stype, Column.determine_nested_dtype(sample)))
                else:
                    dtype = stype
        return DataType(dtype)

    def _validate_column(self: Column[T]) -> None:
        if self._data_backend.is_link() or isinstance(self.dtype, TypeAlias):
            return
        data_type = self.infer_dtype(self.name, self._data_backend)
        if self.dtype != data_type:
            try:
                self._data_backend = self._data_backend.cast_columns(
                    {self.name: self.dtype.pdtype()}
                )
            except Exception as e:
                raise TypeError(
                    f"Failed to cast '{data_type.__name__}' to '{self.dtype.__name__}'",
                    e,
                )

    @property
    def index(self: Column[T]) -> Index:
        return self._data_backend.index

    def tolist(self: Column[T]) -> list:
        return self._data_backend.values.tolist()

    @property
    def values(self: Column[T]) -> np.ndarray:
        return self._data_backend.values

    @classmethod
    def _new_data_copy(
        cls: type[Column], name: str, data_backend: DataBackend, dtype: type[NT]
    ) -> "Column[NT]":
        instance: Column[dtype] = cls(name)
        instance._data_backend = data_backend
        instance.dtype = DataType(dtype)
        return instance

    def astype(self: Column[T], new_dtype: type[NT]) -> "Column[NT]":
        return Column(
            self.name,
            self._data_backend.cast_columns({self.name: new_dtype}),
            dtype=new_dtype,
        )

    def first(
        self: Column[T], n: Optional[int] = 1, offset: Optional[int] = 0
    ) -> Column[T]:
        return Column(
            self.name,
            self._data_backend.iloc[self.index[offset : offset + n]],
            dtype=self.dtype,
        )

    def nunique(self) -> int:
        return self._data_backend.nunique()[self.name]

    def equals(self: Column[T], other: Any) -> bool:
        if type(other) is Column:
            return self._data_backend.equals(cast(Column, other)._data_backend)
        else:
            return self._data_backend.equals(other)

    def __eq__(self: Column[T], other: Any) -> Column[Boolean]:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend == other

    def __ne__(self: Column[T], other: Any) -> bool:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend != other

    def __gt__(self: Column[T], other: Any) -> bool:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend > other

    def __ge__(self: Column[T], other: Any) -> bool:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend >= other

    def __lt__(self: Column[T], other: Any) -> bool:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend < other

    def __le__(self: Column[T], other: Any) -> bool:
        if isinstance(other, Column):
            other = other._data_backend
        return self._data_backend <= other

    def __len__(self: Column[T]):
        return len(self._data_backend)

    def __iter__(self: Column[T]) -> Iterator[T]:
        return self._data_backend.itertuples()

    def item(self) -> Any:
        if len(self) > 1:
            raise RuntimeError("Cannot call `item` on list of column values")
        return self.values[0]

    def __getitem__(self: Column[T], indexable: Indexible) -> Column[T]:
        return self.loc[indexable]

    def __str__(self: Column[T]) -> str:
        result = f"Column {self.name}"
        if len(self._data_backend) == 0:
            return f"{result}([], dtype: {self.dtype.__name__})"
        else:
            return str(self._data_backend)

    def __repr__(self: Column[T]) -> str:
        return str(self)

    class _ILocIndexer(Generic[T]):
        _column: Column[T]

        def __init__(self, column: Column[T]) -> None:
            self._column = column

        def __getitem__(self, item: Union[int, list, slice]) -> Column[T]:
            data = self._column._data_backend.iloc[item]
            return Column._new_data_copy(self._column.name, data, self._column.dtype)

    class _LocIndexer(Generic[T]):
        _column: Column[T]

        def __init__(self, column: Column[T]) -> None:
            self._column = column

        def __getitem__(self, item: Union[Any, list, slice]) -> Column[T]:
            data = self._column._data_backend.loc[item]
            return Column._new_data_copy(self._column.name, data, self._column.dtype)


from src.data_backend.data_backend import DataBackend

