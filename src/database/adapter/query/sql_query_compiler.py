from dataclasses import dataclass
from src.data_store.query import (
    AndQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    AndGroupQuery,
    OrGroupQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrQuery,
    RowCountQuery,
    SumQuery,
)
from src.database.adapter.query.query_compiler import QueryCompiler


@dataclass
class SqlQueryCompiler(QueryCompiler[str]):
    quote: bool = False

    def try_quote(self, value: str) -> str:
        if type(value) is bool:
            value = int(value)
        if self.quote:
            return f"'{value}'"
        else:
            return value

    def EQUALS(self: "SqlQueryCompiler", query: EqualsQuery) -> str:
        return f"{query.a}={self.try_quote(query.b)}"

    def NOT_EQUALS(self: "SqlQueryCompiler", query: NotEqualsQuery) -> str:
        return f"{query.a}!={self.try_quote(query.b)}"

    def GREATER_THAN(self: "SqlQueryCompiler", query: GreaterThanQuery) -> str:
        return f"{query.a}>{self.try_quote(query.b)}"

    def GREATER_EQUAL(self: "SqlQueryCompiler", query: GreaterEqualQuery) -> str:
        return f"{query.a}>={self.try_quote(query.b)}"

    def LESS_THAN(self: "SqlQueryCompiler", query: LessThanQuery) -> str:
        return f"{query.a}<{self.try_quote(query.b)}"

    def LESS_EQUAL(self: "SqlQueryCompiler", query: LessEqualQuery) -> str:
        return f"{query.a}<={self.try_quote(query.b)}"

    def ROW_COUNT(self: "SqlQueryCompiler", row_count_query: RowCountQuery) -> str:
        return f"COUNT({row_count_query.a})"

    def SUM(self: "SqlQueryCompiler", sum_query: SumQuery) -> str:
        return f"SUM({sum_query.a})"

    def AND(self: "SqlQueryCompiler", and_type: AndQuery) -> str:
        return f"({and_type.a} and {and_type.b})"

    def AND_GROUP(self: "SqlQueryCompiler", query: AndGroupQuery) -> str:
        combined = " and ".join(query.items)
        return f"({combined})"

    def OR(self: "SqlQueryCompiler", or_type: OrQuery) -> str:
        return f"({or_type.a} or {or_type.b})"

    def OR_GROUP(self: "SqlQueryCompiler", query: OrGroupQuery) -> str:
        combined = " or ".join(query.items)
        return f"({combined})"
