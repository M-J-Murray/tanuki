from __future__ import annotations

from typing import Generic, Optional, TypeVar

from .column_alias import ColumnAlias

C = TypeVar("C", bound=tuple[ColumnAlias, ...])
B = TypeVar("B", bound="DataBackend")


class Index(Generic[C]):
    name: str
    _data_backend: B

    def __init__(self, name: str, data: Optional[list] = None) -> None:
        self.name = name
        if data is not None:
            if issubclass(type(data), DataBackend):
                self._data_backend = data
            elif isinstance(data, dict):
                self._data_backend = PandasBackend(data)
            else:
                self._data_backend = PandasBackend({name: data})
        else:
            self._data_backend = PandasBackend()

    @property
    def columns(self: Index[C]) -> list[str]:
        return self._data_backend.columns

    def tolist(self: Index[C]) -> list:
        return self._data_backend.values.tolist()

    def __str__(self: Index[C]) -> str:
        result = f"Index {self.name}"
        if len(self._data_backend) == 0:
            return f"{result}([])"
        else:
            return str(self._data_backend)

    def __repr__(self: Index[C]) -> str:
        return str(self)


from src.data_backend.data_backend import DataBackend
from src.data_backend.pandas_backend import PandasBackend
