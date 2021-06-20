from __future__ import annotations

import builtins
from io import UnsupportedOperation
from typing import Any, Iterable, Optional, Union

from src.data_store.data_type import DataType
from src.data_store.query import Query

from .column import Column

old_len = builtins.len
old_sum = builtins.sum


def len(a: Union[Iterable, Query, ColumnAlias]) -> Union[Any, Query]:
    if issubclass(type(a), Query) or type(a) is ColumnAlias:
        return a.__len__()
    else:
        return old_len(a)


def sum(a: Union[Iterable, Query, ColumnAlias]) -> Union[Any, Query]:
    if issubclass(type(a), Query) or type(a) is ColumnAlias:
        return a.__sum__()
    else:
        return old_sum(a)


builtins.len = len
builtins.sum = sum


class ColumnAlias:
    name: Optional[str]
    dtype: DataType
    __origin__: type = Column
    __args__: tuple[DataType]
    __parameters__: tuple[DataType]

    def __init__(self, dtype: type, name: Optional[str] = None) -> None:
        self.dtype = DataType(dtype)
        self.__args__ = (self.dtype,)
        self.__parameters__ = (self.dtype,)
        self.name = name

    def __call__(
        self, name: str, data: Optional[list] = None, index: Optional[list] = None
    ) -> Column:
        return Column(name=name, data=data, dtype=self.dtype, index=index)

    def __str__(self) -> str:
        if self.name is None:
            raise ValueError("Column name not set")
        return self.name

    def __repr__(self) -> str:
        repr_def = f"{Column.__name__}[{self.dtype.__name__}]"
        if self.name is not None:
            repr_def = f"{self.name}: {repr_def}"
        return repr_def

    def __hash__(self) -> int:
        if self.name is None:
            raise UnsupportedOperation("Cannot hash Column outside of DataStore")
        return hash(str(self))

    def __eq__(self, o: object) -> EqualsQuery:
        if getattr(o, "__module__", None) == "typing":
            return False
        return EqualsQuery(self, o)

    def __ne__(self, o: object) -> NotEqualsQuery:
        return NotEqualsQuery(self, o)

    def __gt__(self, o: object) -> GreaterThanQuery:
        return GreaterThanQuery(self, o)

    def __ge__(self, o: object) -> GreaterEqualQuery:
        return GreaterEqualQuery(self, o)

    def __lt__(self, o: object) -> LessThanQuery:
        return LessThanQuery(self, o)

    def __le__(self, o: object) -> LessEqualQuery:
        return LessEqualQuery(self, o)

    def __len__(self) -> RowCountQuery:
        return RowCountQuery(self)

    def __sum__(self) -> SumQuery:
        return SumQuery(self)

    def __and__(self, o: object) -> AndQuery:
        return AndQuery(self, o)

    def __or__(self, o: object) -> OrQuery:
        return OrQuery(self, o)


from .query import (
    AndQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrQuery,
    RowCountQuery,
    SumQuery,
)
