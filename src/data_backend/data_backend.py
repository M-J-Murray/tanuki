from abc import abstractclassmethod, abstractmethod, abstractproperty
from typing import Any, Generic, Iterable, Type, TypeVar, Union

from pandas import Index

from src.data_store.column import Column

B = TypeVar("B", bound="DataBackend")


class LocIndexer(Generic[B]):
    @abstractmethod
    def __getitem__(self, item: Union[int, list, slice]) -> B:
        ...


class ILocIndexer(Generic[B]):
    @abstractmethod
    def __getitem__(self, item: Union[Any, list, slice]) -> B:
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
    def loc(self: B) -> LocIndexer[B]:
        ...

    @abstractproperty
    def iloc(self: B) -> ILocIndexer[B]:
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
    def set_index(self: B, column: Union[str, Iterable]) -> B:
        ...

    @abstractmethod
    def reset_index(self: B, drop: bool = False) -> B:
        ...

    @abstractclassmethod
    def concat(cls: Type[B], all_backends: list[B], ignore_index: bool = False) -> B:
        ...