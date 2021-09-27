from __future__ import annotations

from abc import abstractmethod, abstractproperty
from typing import Generic, TypeVar, Union

C = TypeVar("C", bound=tuple["ColumnAlias", ...])


class Index(Generic[C]):
    @abstractproperty
    def name(self: Index[C]) -> Union[str, list[str]]:
        raise NotImplementedError()

    @abstractproperty
    def columns(self: Index[C]) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, item) -> Index[C]:
        raise NotImplementedError()

    @abstractmethod
    def tolist(self: Index[C]) -> list:
        raise NotImplementedError()

    @abstractmethod
    def __str__(self: Index[C]) -> str:
        raise NotImplementedError()

    @abstractmethod
    def __repr__(self: Index[C]) -> str:
        raise NotImplementedError()

from src.data_store.column_alias import ColumnAlias
