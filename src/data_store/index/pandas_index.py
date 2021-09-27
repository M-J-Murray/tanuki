from .index import Index
from pandas import Index as PIndex

from typing import TypeVar, Union

from src.data_store.column_alias import ColumnAlias

C = TypeVar("C", bound=tuple[ColumnAlias, ...])

class PandasIndex(Index[C]):
    _data: PIndex
    _columns: list[str]

    def __init__(self, data: PIndex = PIndex([]), columns: list[str] = []) -> None:
        self._data = data
        self._columns = columns

    @property
    def name(self) -> Union[str, list[str]]:
        if len(self._data.names) > 1:
            return self._data.names
        else:
            return self._data.name

    @property
    def columns(self) -> list[str]:
        return self._columns

    def __getitem__(self, item) -> Index[C]:
        return PandasIndex(self._data[item], self._columns)

    def tolist(self) -> list:
        return self._data.values.tolist()

    def __str__(self) -> str:
        result = f"Index {self.name}"
        if len(self._data) == 0:
            return f"{result}([])"
        else:
            return str(self._data)

    def __repr__(self) -> str:
        return str(self)