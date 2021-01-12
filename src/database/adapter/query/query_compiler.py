from __future__ import annotations

from typing import Any, Generic, TypeVar, Union

T = TypeVar("T")


class QueryCompiler(Generic[T]):
    def EQUALS(self: "QueryCompiler", equals_query: EqualsQuery) -> T:
        raise NotImplementedError()

    def NOT_EQUALS(self: "QueryCompiler", not_equals_query: NotEqualsQuery) -> T:
        raise NotImplementedError()

    def GREATER_THAN(self: "QueryCompiler", gt_query: GreaterThanQuery) -> T:
        raise NotImplementedError()

    def GREATER_EQUAL(self: "QueryCompiler", ge_query: GreaterEqualQuery) -> T:
        raise NotImplementedError()

    def LESS_THAN(self: "QueryCompiler", lt_query: LessThanQuery) -> T:
        raise NotImplementedError()

    def LESS_EQUAL(self: "QueryCompiler", le_query: LessEqualQuery) -> T:
        raise NotImplementedError()

    def ROW_COUNT(self: "QueryCompiler", row_count_query: RowCountQuery) -> T:
        raise NotImplementedError()

    def SUM(self: "QueryCompiler", sum_query: SumQuery) -> T:
        raise NotImplementedError()

    def AND(self: "QueryCompiler", and_query: AndQuery) -> T:
        raise NotImplementedError()

    def OR(self: "QueryCompiler", or_query: OrQuery) -> T:
        raise NotImplementedError()

    def compile(self: "QueryCompiler", query: Union[Any, Query]) -> T:
        if not isinstance(query, Query):
            return query
        return query.compile(self)


from src.data_store.query_type import (
    AndQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrQuery,
    Query,
    RowCountQuery,
    SumQuery,
)
