from abc import abstractmethod
from typing import Any, Generic, TypeVar, Union

from numpy import ndarray

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
    @abstractmethod
    def columns(self) -> set[str]:
        ...

    @abstractmethod
    def is_row(self) -> bool:
        ...

    @abstractmethod
    def index(self) -> ndarray:
        ...

    @abstractmethod
    def loc(self: T) -> LocIndexer[T]:
        ...

    @abstractmethod
    def iloc(self: T) -> ILocIndexer[T]:
        ...

    @abstractmethod
    def __eq__(self, other):
        ...

    @abstractmethod
    def __len__(self):
        ...

    @abstractmethod
    def __iter__(self):
        ...

    @abstractmethod
    def __getitem__(self, item: str) -> Any:
        ...

    @abstractmethod
    def __setitem__(self, item: str, value: Column) -> Column:
        ...

    @abstractmethod
    def reset_index(self: T, drop: bool = True) -> T:
        ...