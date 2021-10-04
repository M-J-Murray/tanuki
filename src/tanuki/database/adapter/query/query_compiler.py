from __future__ import annotations

from typing import Any, Generic, TYPE_CHECKING, TypeVar, Union

T = TypeVar("T")

from tanuki.data_store.query import Query

if TYPE_CHECKING:
    from tanuki.data_store.query import (
        AndGroupQuery,
        AndQuery,
        EqualsQuery,
        GreaterEqualQuery,
        GreaterThanQuery,
        LessEqualQuery,
        LessThanQuery,
        NotEqualsQuery,
        OrGroupQuery,
        OrQuery,
        RowCountQuery,
        SumQuery,
    )


class QueryCompiler(Generic[T]):
    def EQUALS(self: "QueryCompiler", query: EqualsQuery) -> T:
        raise NotImplementedError()

    def NOT_EQUALS(self: "QueryCompiler", query: NotEqualsQuery) -> T:
        raise NotImplementedError()

    def GREATER_THAN(self: "QueryCompiler", query: GreaterThanQuery) -> T:
        raise NotImplementedError()

    def GREATER_EQUAL(self: "QueryCompiler", query: GreaterEqualQuery) -> T:
        raise NotImplementedError()

    def LESS_THAN(self: "QueryCompiler", query: LessThanQuery) -> T:
        raise NotImplementedError()

    def LESS_EQUAL(self: "QueryCompiler", query: LessEqualQuery) -> T:
        raise NotImplementedError()

    def ROW_COUNT(self: "QueryCompiler", query: RowCountQuery) -> T:
        raise NotImplementedError()

    def SUM(self: "QueryCompiler", query: SumQuery) -> T:
        raise NotImplementedError()

    def AND(self: "QueryCompiler", query: AndQuery) -> T:
        raise NotImplementedError()

    def AND_GROUP(self: "QueryCompiler", query: AndGroupQuery) -> T:
        raise NotImplementedError()

    def OR(self: "QueryCompiler", query: OrQuery) -> T:
        raise NotImplementedError()

    def OR_GROUP(self: "QueryCompiler", query: OrGroupQuery) -> T:
        raise NotImplementedError()

    def compile(self: "QueryCompiler", query: Union[Any, Query]) -> T:
        if not isinstance(query, Query):
            return query
        return query.compile(self)
