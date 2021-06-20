from __future__ import annotations
from abc import abstractclassmethod, abstractmethod, abstractproperty
from typing import Any, Generator, Generic, Iterable, Type, TypeVar, Union

import numpy as np
from pandas import Index
from pandas.core.frame import DataFrame

B = TypeVar("B", bound="DataBackend")


class LocIndexer(Generic[B]):
    @abstractmethod
    def __getitem__(self, item: Union[int, list, slice]) -> B:
        raise NotImplementedError()


class ILocIndexer(Generic[B]):
    @abstractmethod
    def __getitem__(self, item: Union[Any, list, slice]) -> B:
        raise NotImplementedError()


class DataBackend:
    def to_pandas(self) -> DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def columns(self) -> list[str]:
        raise NotImplementedError()

    @abstractproperty
    def values(self) -> np.ndarray:
        raise NotImplementedError()

    @abstractproperty
    def dtypes(self) -> dict[str, type]:
        raise NotImplementedError()

    @abstractmethod
    def cast_columns(self, column_dtypes: dict[str, type]) -> DataBackend:
        raise NotImplementedError()

    @abstractmethod
    def to_dict(self, orient) -> dict[str, any]:
        raise NotImplementedError()

    @abstractproperty
    def index(self) -> Index:
        raise NotImplementedError()

    @abstractproperty
    def index_name(self) -> Union[str, list[str]]:
        raise NotImplementedError()

    @abstractproperty
    def loc(self: B) -> LocIndexer[B]:
        raise NotImplementedError()

    @abstractproperty
    def iloc(self: B) -> ILocIndexer[B]:
        raise NotImplementedError()

    @abstractmethod
    def equals(self, other: Any) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __eq__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __ne__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __gt__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __ge__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __lt__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __le__(self, other: Any) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self) -> Generator[str, None, None]:
        raise NotImplementedError()

    @abstractmethod
    def iterrows(self) -> Generator[tuple[int, B], None, None]:
        raise NotImplementedError()

    @abstractmethod
    def itertuples(self) -> Generator[tuple, None, None]:
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, item: Union[str, list[bool]]) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def getitems(self, item: list[str]) -> B:
        raise NotImplementedError()

    @abstractmethod
    def getmask(self, mask: list[bool]) -> B:
        raise NotImplementedError()

    @abstractmethod
    def query(self, query: Query) -> B:
        raise NotImplementedError()

    @abstractmethod
    def __setitem__(self, item: str, value: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def set_index(self: B, column: Union[str, Iterable]) -> B:
        raise NotImplementedError()

    @abstractmethod
    def reset_index(self: B, drop: bool = False) -> B:
        raise NotImplementedError()

    @abstractmethod
    def append(self: B, new_backend: B, ignore_index: bool = False) -> B:
        raise NotImplementedError()

    @abstractclassmethod
    def concat(cls: Type[B], all_backends: list[B], ignore_index: bool = False) -> B:
        raise NotImplementedError()

from src.data_store.query import Query
