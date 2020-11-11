from typing import Generic, TypeVar

from src.data_store.query_type import (
    AndType,
    EqualsType,
    NotEqualsType,
    OrType,
    QueryType,
)

T = TypeVar("T")


class QueryCompiler(Generic[T]):
    def EQUALS(self: "QueryCompiler", equals_type: EqualsType) -> T:
        ...

    def NOT_EQUALS(self: "QueryCompiler", not_equals_type: NotEqualsType) -> T:
        ...

    def AND(self: "QueryCompiler", and_type: AndType) -> T:
        ...

    def OR(self: "QueryCompiler", or_type: OrType) -> T:
        ...

    def compile(self: "QueryCompiler", query_type: QueryType) -> T:
        ...
