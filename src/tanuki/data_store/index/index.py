from __future__ import annotations

from abc import abstractmethod, abstractproperty
from typing import Any, Generic, TYPE_CHECKING, TypeVar, Union

import numpy as np

if TYPE_CHECKING:
    from tanuki.data_store.column_alias import ColumnAlias
    from tanuki.data_store.index.pandas_index import PandasIndex



C = TypeVar("C", bound=tuple["ColumnAlias", ...])


class Index(Generic[C]):
    @abstractproperty
    def name(self: Index[C]) -> Union[str, list[str]]:
        raise NotImplementedError()

    @abstractproperty
    def columns(self: Index[C]) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def to_pandas(self) -> PandasIndex[C]:
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, item) -> Index[C]:
        raise NotImplementedError()

    @abstractproperty
    def values(self: Index[C]) -> np.ndarray:
        raise NotImplementedError()

    @abstractmethod
    def tolist(self: Index[C]) -> list:
        raise NotImplementedError()

    @abstractmethod
    def equals(self, other: Any) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __eq__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __ne__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __gt__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __ge__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __lt__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __le__(self, other: Any) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def __str__(self: Index[C]) -> str:
        raise NotImplementedError()

    @abstractmethod
    def __repr__(self: Index[C]) -> str:
        raise NotImplementedError()
