from __future__ import annotations

from typing import Any, Generic, TypeVar, Union

T = TypeVar("T")


class QueryCompiler(Generic[T]):
    def EQUALS(self: "QueryCompiler", equals_type: EqualsType) -> T:
        raise NotImplementedError()

    def NOT_EQUALS(self: "QueryCompiler", not_equals_type: NotEqualsType) -> T:
        raise NotImplementedError()

    def GREATER_THAN(self: "QueryCompiler", gt_type: GreaterThanType) -> T:
        raise NotImplementedError()

    def GREATER_EQUAL(self: "QueryCompiler", ge_type: GreaterEqualType) -> T:
        raise NotImplementedError()

    def LESS_THAN(self: "QueryCompiler", lt_type: LessThanType) -> T:
        raise NotImplementedError()

    def LESS_EQUAL(self: "QueryCompiler", le_type: LessEqualType) -> T:
        raise NotImplementedError()

    def COUNT(self: "QueryCompiler", count_type: CountType) -> T:
        raise NotImplementedError()

    def AND(self: "QueryCompiler", and_type: AndType) -> T:
        raise NotImplementedError()

    def OR(self: "QueryCompiler", or_type: OrType) -> T:
        raise NotImplementedError()

    def compile(self: "QueryCompiler", query_type: Union[Any, QueryType]) -> T:
        if not isinstance(query_type, QueryType):
            return query_type
        return query_type.compile(self)


from src.data_store.query_type import (
    AndType,
    CountType,
    EqualsType,
    GreaterEqualType,
    GreaterThanType,
    LessEqualType,
    LessThanType,
    NotEqualsType,
    OrType,
    QueryType,
)
