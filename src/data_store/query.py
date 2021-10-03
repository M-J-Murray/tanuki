from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, TypeVar, Union
from pandas import DataFrame, Series

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


class DataStoreQuery(Query):
    a: Query

    def __init__(
        self,
        query_type: type[Query],
        column_join_type: type[Query],
        row_join_type: type[Query],
        dataframe: DataFrame,
    ) -> None:
        sub_queries = [RowQuery(query_type, column_join_type, row) for _, row in dataframe.iterrows()]
        self.a = row_join_type(sub_queries)

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        return query_compiler.compile(self.a)

    def __str__(self) -> str:
        return str(self.a)

    def __repr__(self) -> str:
        return str(self)


class RowQuery(Query):
    a: Query

    def __init__(
        self, query_type: type[Query], join_type: type[Query], row: Series
    ) -> None:
        sub_queries = [query_type(col, value) for col, value in row.iteritems()]
        self.a = join_type(sub_queries)

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        return query_compiler.compile(self.a)

    def __str__(self) -> str:
        return str(self.a)

    def __repr__(self) -> str:
        return str(self)


class ColumnQuery(Query):
    a: Query

    def __init__(
        self,
        query_type: type[Query],
        join_type: type[Query],
        a_items: list,
        b_items: list,
    ) -> None:
        if not isinstance(b_items, Iterable) or isinstance(b_items, str):
            b_items = [b_items for _ in range(len(a_items))]
        sub_queries = [query_type(a, b) for a, b in zip(a_items, b_items) if b is not None]
        self.a = join_type(sub_queries)

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
class AndGroupQuery(Query):
    items: list[Union[bool, Query]]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        compiled = [query_compiler.compile(item) for item in self.items]
        return query_compiler.AND_GROUP(AndGroupQuery(compiled))

    def __str__(self) -> str:
        combined = " and ".join([str(item) for item in self.items])
        return f"({combined})"

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


@dataclass
class OrGroupQuery(Query):
    items: list[Union[bool, Query]]

    def compile(self, query_compiler: QueryCompiler[T]) -> T:
        compiled = [query_compiler.compile(item) for item in self.items]
        return query_compiler.OR_GROUP(OrGroupQuery(compiled))

    def __str__(self) -> str:
        combined = " or ".join([str(item) for item in self.items])
        return f"({combined})"

    def __repr__(self) -> str:
        return str(self)


from src.database.adapter.query.query_compiler import QueryCompiler
