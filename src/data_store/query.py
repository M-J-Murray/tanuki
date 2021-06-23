from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, TypeVar, Union

T = TypeVar("T")


class Query:
    def __eq__(self, o: object) -> EqualsQuery:
        return EqualsQuery(self, o)

    def __ne__(self, o: object) -> NotEqualsQuery:
        return NotEqualsQuery(self, o)

    def __gt__(self, o: object) -> GreaterThanQuery:
        return GreaterThanQuery(self, o)

    def __ge__(self, o: object) -> GreaterEqualQuery:
        return GreaterEqualQuery(self, o)

    def __lt__(self, o: object) -> LessThanQuery:
        return LessThanQuery(self, o)

    def __le__(self, o: object) -> LessEqualQuery:
        return LessEqualQuery(self, o)

    def __len__(self) -> RowCountQuery:
        return RowCountQuery(self)

    def __sum__(self) -> SumQuery:
        return SumQuery(self)

    def __and__(self, o: object) -> AndQuery:
        return AndQuery(self, o)

    def __or__(self, o: object) -> OrQuery:
        return OrQuery(self, o)

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        raise NotImplementedError()

    def __str__(self) -> str:
        raise NotImplementedError()

    def __repr__(self) -> str:
        raise NotImplementedError()


class MultiAndQuery(Query):
    a: Query

    def __init__(self, query_type: type[Query], a_items: list, b_items: list) -> None:
        if len(a_items) != len(b_items):
            raise RuntimeError("Cannot build multi query with items of different lengths")
        query: Query = None
        for a, b in zip(a_items, b_items):
            new_query = query_type(a, b)
            if query is None:
                query = new_query
            else:
                query = query and new_query
        self.a = query

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        return query_compiler.compile(self.a)

    def __str__(self) -> str:
        return str(self.a)

    def __repr__(self) -> str:
        return str(self)

@dataclass
class EqualsQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.EQUALS(EqualsQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} == {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class NotEqualsQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.NOT_EQUALS(NotEqualsQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} != {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class GreaterThanQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.GREATER_THAN(GreaterThanQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} > {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class GreaterEqualQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.GREATER_EQUAL(GreaterEqualQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} >= {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class LessThanQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.LESS_THAN(LessThanQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} < {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class LessEqualQuery(Query):
    a: Union[Any, Query]
    b: Union[Any, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.LESS_EQUAL(LessEqualQuery(a, b))

    def __str__(self) -> str:
        return f"{self.a} <= {self.b}"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class RowCountQuery(Query):
    a: Union[Iterable, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        return query_compiler.COUNT(RowCountQuery(a))

    def __str__(self) -> str:
        return f"len({self.a})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class SumQuery(Query):
    a: Union[Iterable, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        return query_compiler.SUM(RowCountQuery(a))

    def __str__(self) -> str:
        return f"sum({self.a})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class AndQuery(Query):
    a: Union[bool, Query]
    b: Union[bool, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.AND(AndQuery(a, b))

    def __str__(self) -> str:
        return f"({self.a}) and ({self.b})"

    def __repr__(self) -> str:
        return str(self)


@dataclass
class OrQuery(Query):
    a: Union[bool, Query]
    b: Union[bool, Query]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        a = query_compiler.compile(self.a)
        b = query_compiler.compile(self.b)
        return query_compiler.OR(OrQuery(a, b))

    def __str__(self) -> str:
        return f"({self.a}) or ({self.b})"

    def __repr__(self) -> str:
        return str(self)


from src.database.adapter.query.query_compiler import QueryCompiler
