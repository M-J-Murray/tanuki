from __future__ import annotations

from typing import Optional

from src.data_store.data_type import DataType

from .column import Column


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

    def __eq__(self, o: object) -> QueryType:
        return EqualsType(self, o)


from .query_type import EqualsType, QueryType