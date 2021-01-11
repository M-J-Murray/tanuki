from abc import abstractclassmethod, abstractmethod, abstractproperty
from typing import Any, Generator, Generic, Iterable, Type, TypeVar, Union

from pandas import Index
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.column import Column
from src.data_store.query_type import QueryType

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
    def to_pandas(self) -> Union[Series, DataFrame]:
        raise NotImplementedError()

    @abstractproperty
    def columns(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def to_dict(self, orient) -> dict[str, any]:
        raise NotImplementedError()

    @abstractmethod
    def is_row(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def to_table(self) -> B:
        raise NotImplementedError()

    @abstractmethod
    def to_row(self) -> B:
        raise NotImplementedError()

    @abstractproperty
    def index(self) -> Index:
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
    def query(self, query_type: QueryType) -> B:
        raise NotImplementedError()

    @abstractmethod
    def __setitem__(self, item: str, value: Column) -> Column:
        raise NotImplementedError()

    @abstractmethod
    def set_index(self: B, column: Union[str, Iterable]) -> B:
        raise NotImplementedError()

    @abstractmethod
    def reset_index(self: B, drop: bool = False) -> B:
        raise NotImplementedError()

    @abstractmethod
    def append(clt: B, new_backend: B, ignore_index: bool = False) -> B:
        raise NotImplementedError()

    @abstractclassmethod
    def concat(cls: Type[B], all_backends: list[B], ignore_index: bool = False) -> B:
        raise NotImplementedError()
