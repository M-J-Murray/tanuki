from __future__ import annotations

from typing import Any, cast, Generic, TypeVar, Union

T = TypeVar("T")


class QueryCompiler(Generic[T]):
    def EQUALS(self: "QueryCompiler", equals_type: EqualsType) -> T:
        ...

    def NOT_EQUALS(self: "QueryCompiler", not_equals_type: NotEqualsType) -> T:
        ...

    def COUNT(self: "QueryCompiler", count_type: CountType) -> T:
        ...

    def AND(self: "QueryCompiler", and_type: AndType) -> T:
        ...

    def OR(self: "QueryCompiler", or_type: OrType) -> T:
        ...

    def compile(self: "QueryCompiler", query_type: Union[Any, QueryType]) -> T:
        if not isinstance(query_type, QueryType):
            return query_type
        return query_type.compile(self)


from src.data_store.query_type import (
    AndType,
    CountType,
    EqualsType,
    NotEqualsType,
    OrType,
    QueryType,
)
