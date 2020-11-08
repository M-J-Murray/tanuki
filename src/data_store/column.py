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

from hamcrest.core.core.isinstanceof import instance_of
import numpy as np
from numpy.lib.function_base import iterable
from pandas import Index, Series

from src.data_store.data_type import DataType, Object

T = TypeVar("T")
NT = TypeVar("NT")
Indexible = Union[Any, list, Index]


class Column(Generic[T]):
    series: Series
    dtype: DataType

    @classmethod
    def __class_getitem__(_, dtype: type) -> Type["Column"]:
        return ColumnAlias(dtype)

    def __init__(
        self: "Column[T]", data: Optional[list[T]] = None, dtype: Optional[T] = None
    ) -> None:
        if data is not None:
            self.series = Series(data)
        else:
            self.series = Series(dtype="object")

        self.dtype = (
            self._series_dtype()
            if dtype is None
            else DataType(dtype)
        )

        if data is not None:
            self._validate_column()

    @staticmethod
    def _determine_nested_dtype(data: Iterable) -> DataType:
        sample = next(iter(data))
        if sample == data:
            return type(data)
        elif isinstance(sample, Iterable):
            nested_type = Column._determine_nested_dtype(sample)
        else:
            nested_type = type(sample)
        return GenericAlias(type(data), nested_type)

    def _series_dtype(self: "Column[T]") -> DataType:
        dtype: type
        if self.series.dtype == np.object:
            if len(self.series) == 0:
                dtype = Object
            else:
                sample = self.series.iloc[0]
                if isinstance(sample, Iterable):
                    dtype = self._determine_nested_dtype(sample)
                else:
                    dtype = type(sample)

        else:
            dtype = self.series.dtype
        return DataType(dtype)

    def _validate_column(self: "Column[T]") -> None:
        if self.dtype != self._series_dtype():
            try:
                self.series = self.series.astype(self.dtype.pdtype())
            except Exception as e:
                raise TypeError(
                    f"Failed to cast '{self._series_dtype().__name__}' to '{self.dtype.__name__}'",
                    e,
                )

    @property
    def index(self: "Column[T]") -> Index:
        return self.series.index

    def tolist(self: "Column[T]") -> list:
        return self.series.values.tolist()

    @classmethod
    def _new_data_copy(
        cls: type["Column"], series: Series, dtype: type[NT]
    ) -> "Column[NT]":
        instance: Column[dtype] = cls[dtype]()
        instance.series = series
        return instance

    def astype(self: "Column[T]", new_dtype: type[NT]) -> "Column[NT]":
        return self._new_data_copy(self.series.astype(new_dtype), new_dtype)

    def reset_index(self: "Column[T]", drop: bool = True) -> "Column[T]":
        return self._new_data_copy(self.series.reset_index(drop=drop), self.dtype)

    def first(self: "Column[T]", n: Optional[int] = 1, offset: Optional[int] = 0) -> T:
        return self._new_data_copy(
            self.series[self.index[offset : offset + n]], self.dtype
        )

    def __eq__(self: "Column[T]", other: Any) -> bool:
        if type(other) is not Column:
            return False
        oc = cast(Column[T], other)
        return self.dtype == oc.dtype and self.series.equals(oc.series)

    def __len__(self: "Column[T]"):
        return len(self.series)

    def __iter__(self: "Column[T]") -> Iterator[T]:
        return iter(self.series)

    def __getitem__(self: "Column[T]", indexable: Indexible) -> "Column[T]":
        return self._new_data_copy(self.series[indexable], self.dtype)

    def __str__(self: "Column[T]") -> str:
        if len(self.series) == 0:
            return f"Column([], dtype: {self.dtype.__name__})"
        else:
            str_def = str(self.series)
            dtype_ind = str_def.index("dtype:")
            str_def = str_def[: dtype_ind + 7] + str(self.dtype.__name__)
            return str_def

    def __repr__(self: "Column[T]") -> str:
        return str(self)


class ColumnAlias:
    _name: Optional[str]
    dtype: DataType
    __origin__: type = Column
    __args__: tuple[type]
    __parameters__: tuple[type]

    def __init__(self, dtype: type, name: Optional[str] = None) -> None:
        self.dtype = DataType(dtype)
        self.__args__ = (self.dtype,)
        self.__parameters__ = (self.dtype,)
        self._name = name

    def __call__(self, data: Optional[list] = None) -> Column:
        return Column(data=data, dtype=self.dtype)

    def __str__(self) -> str:
        if self._name is None:
            raise ValueError("Column name not set")
        return self._name

    def __repr__(self) -> str:
        repr_def = f"{Column.__module__}.{Column.__name__}[{self.dtype.__name__}]"
        if self._name is not None:
            repr_def = f"{self._name}: {repr_def}"
        return repr_def
