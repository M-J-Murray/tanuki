from abc import abstractmethod, abstractproperty
from typing import Any, Generic, Iterable, TypeVar, Union

from pandas import Index

from src.data_store.column import Column

T = TypeVar("T", bound="DataBackend")


class LocIndexer(Generic[T]):
    @abstractmethod
    def __getitem__(self, item: Union[int, list, slice]) -> T:
        ...


class ILocIndexer(Generic[T]):
    @abstractmethod
    def __getitem__(self, item: Union[Any, list, slice]) -> T:
        ...


class DataBackend:
    @abstractproperty
    def columns(self) -> list[str]:
        ...

    @abstractmethod
    def to_dict(self, orient) -> dict[str, any]:
        ...

    @abstractmethod
    def is_row(self) -> bool:
        ...

    @abstractproperty
    def index(self) -> Index:
        ...

    @abstractproperty
    def loc(self: T) -> LocIndexer[T]:
        ...

    @abstractproperty
    def iloc(self: T) -> ILocIndexer[T]:
        ...

    @abstractmethod
    def __eq__(self, other):
        ...

    @abstractmethod
    def equals(self, other):
        ...

    @abstractmethod
    def __len__(self):
        ...

    @abstractmethod
    def __iter__(self):
        ...

    @abstractmethod
    def iterrows(self):
        ...

    @abstractmethod
    def itertuples(self):
        ...

    @abstractmethod
    def __getitem__(self, item: str) -> Any:
        ...

    @abstractmethod
    def __setitem__(self, item: str, value: Column) -> Column:
        ...

    @abstractmethod
    def set_index(self: T, column: Union[str, Iterable]) -> T:
        ...

    @abstractmethod
    def reset_index(self: T, drop: bool = True) -> T:
        ...
