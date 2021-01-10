from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, TypeVar, Union

T = TypeVar("T")


class QueryType:

    def __eq__(self, o: object) -> EqualsType:
        return EqualsType(self, o)

    def __ne__(self, o: object) -> NotEqualsType:
        return NotEqualsType(self, o)

    def __len__(self) -> CountType:
        return CountType(self)

    def __and__(self, o: object) -> AndType:
        return AndType(self, o)

    def __or__(self, o: object) -> OrType:
        return OrType(self, o)

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        ...


@dataclass
class EqualsType(QueryType):
    a: Union[Any, ColumnAlias, QueryType]
    b: Union[Any, ColumnAlias, QueryType]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.EQUALS(EqualsType(a, b))


@dataclass
class NotEqualsType(QueryType):
    a: Union[Any, ColumnAlias]
    b: Union[Any, ColumnAlias]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.NOT_EQUALS(NotEqualsType(a, b))


@dataclass
class CountType(QueryType):
    a: Union[Iterable, ColumnAlias, QueryType]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        return query_compiler.COUNT(CountType(a))

    def __len__(self) -> CountType:
        return self

    def __int__(self) -> CountType:
        return self

    def __index__(self) -> CountType:
        return self


@dataclass
class AndType(QueryType):
    a: Union[bool, QueryType]
    b: Union[bool, QueryType]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.AND(AndType(a, b))


@dataclass
class OrType(QueryType):
    a: Union[bool, QueryType]
    b: Union[bool, QueryType]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.OR(OrType(a, b))


from src.data_store.column_alias import ColumnAlias
from src.database.adapter.query.query_compiler import QueryCompiler
