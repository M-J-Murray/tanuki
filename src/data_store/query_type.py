from dataclasses import dataclass
from typing import Any, Union

from src.data_store.column_alias import ColumnAlias


class QueryType:
    ...


@dataclass
class EqualsType(QueryType):
    a: Union[Any, ColumnAlias]
    b: Union[Any, QueryType]


@dataclass
class NotEqualsType(QueryType):
    a: Union[Any, ColumnAlias]
    b: Union[Any, QueryType]


@dataclass
class AndType(QueryType):
    a: QueryType
    b: QueryType


@dataclass
class OrType(QueryType):
    a: QueryType
    b: QueryType
