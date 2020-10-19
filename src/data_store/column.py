from typing import Generic, Type, TypeVar, Optional

import numpy as np
from pandas import Series


T = TypeVar("T")


class Column(Generic[T], Series):
    _data_type: T

    @classmethod
    def __class_getitem__(_, data_type: type) -> Type["Column"]:
        return ColumnAlias(data_type)

    @staticmethod
    def needs_cast(dtype: type, arg_type: type) -> bool:
        if arg_type == float and dtype == np.float64:
            return False
        if arg_type == int and dtype == np.int64:
            return False
        if arg_type == bool and dtype == np.bool_:
            return False
        return dtype != arg_type

    def __init__(self: "Column", data: list[T], data_type: Optional[T] = None) -> None:
        super(Column, self).__init__(data)
        self._data_type = data_type
        if data_type is not None and self.needs_cast(self.dtype, data_type):
            try:
                cast_data = self.astype(data_type)
            except Exception as e:
                raise TypeError(
                    f"Failed to cast '{self.dtype.__name__}' to '{data_type.__name__}'",
                    e,
                )
            super(Column, self).__init__(cast_data)

    @property
    def dtype(self: "Column") -> type:
        return type(self.values[0])

class ColumnAlias:
    _name: Optional[str]
    __origin__: type = Column
    __args__: tuple[type]
    __parameters__: tuple[type]

    def __init__(self, data_type: type, name: Optional[str] = None) -> None:
        self.__args__ = (data_type,)
        self.__parameters__ = (data_type,)
        self._name = name

    def __call__(self, data: list) -> Column:
        return Column(data, self.__args__[0])

    def __str__(self) -> str:
        if self._name is None:
            raise ValueError("Column name not set")
        return self._name

    def __repr__(self) -> str:
        repr = f"{Column.__module__}.{Column.__name__}[{self.__args__[0].__name__}]"
        if self._name is not None:
            repr = f"{self._name}:{repr}"
        return repr