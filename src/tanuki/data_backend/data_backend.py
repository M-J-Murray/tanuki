from __future__ import annotations

from abc import abstractclassmethod, abstractmethod, abstractproperty
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

import numpy as np
from pandas import DataFrame

from src.tanuki.data_store.data_type import DataType
from src.tanuki.database.data_token import DataToken

if TYPE_CHECKING:
    from src.tanuki.data_store.index.index import Index
    from src.tanuki.data_store.index.index_alias import IndexAlias
    from src.tanuki.data_store.query import Query


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
    @abstractmethod
    def is_link(self: B) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def link_token(self: B) -> Optional[DataToken]:
        raise NotImplementedError()

    @abstractmethod
    def to_pandas(self) -> DataFrame:
        raise NotImplementedError()

    @abstractproperty
    def columns(self) -> list[str]:
        raise NotImplementedError()

    @abstractproperty
    def values(self) -> np.ndarray:
        raise NotImplementedError()

    @abstractproperty
    def dtypes(self) -> dict[str, DataType]:
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
    def itertuples(self, ignore_index: bool = False) -> Generator[tuple, None, None]:
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
    def get_index(self, index_alias: IndexAlias) -> Index:
        raise NotImplementedError()

    @abstractmethod
    def set_index(self: B, index: Union[Index, IndexAlias]) -> B:
        raise NotImplementedError()

    @abstractmethod
    def reset_index(self: B) -> B:
        raise NotImplementedError()

    @abstractmethod
    def append(self: B, new_backend: B, ignore_index: bool = False) -> B:
        raise NotImplementedError()

    @abstractmethod
    def drop_indices(self: B, indices: list[int]) -> B:
        raise NotImplementedError()

    @abstractclassmethod
    def concat(cls: Type[B], all_backends: list[B], ignore_index: bool = False) -> B:
        raise NotImplementedError()

    @abstractmethod
    def nunique(self: B) -> int:
        raise NotImplementedError()

    @abstractmethod
    def __str__(self: B) -> str:
        raise NotImplementedError()

    @abstractmethod
    def __repr__(self: B) -> str:
        raise NotImplementedError()
