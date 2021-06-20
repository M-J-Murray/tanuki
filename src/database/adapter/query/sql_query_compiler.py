from src.data_store.query import (
    AndQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrQuery,
    RowCountQuery,
    SumQuery,
)
from src.database.adapter.query.query_compiler import QueryCompiler


class SqlQueryCompiler(QueryCompiler[str]):
    def EQUALS(self: "SqlQueryCompiler", equals_type: EqualsQuery) -> str:
        return f"{equals_type.a}='{equals_type.b}'"

    def NOT_EQUALS(self: "SqlQueryCompiler", not_equals_type: NotEqualsQuery) -> str:
        return f"{not_equals_type.a}!='{not_equals_type.b}'"

    def GREATER_THAN(self: "QueryCompiler", gt_query: GreaterThanQuery) -> str:
        return f"{gt_query.a}>'{gt_query.b}'"

    def GREATER_EQUAL(self: "QueryCompiler", ge_query: GreaterEqualQuery) -> str:
        return f"{ge_query.a}>='{ge_query.b}'"

    def LESS_THAN(self: "QueryCompiler", lt_query: LessThanQuery) -> str:
        return f"{lt_query.a}<'{lt_query.b}'"

    def LESS_EQUAL(self: "QueryCompiler", le_query: LessEqualQuery) -> str:
        return f"{le_query.a}<='{le_query.b}'"

    def ROW_COUNT(self: "QueryCompiler", row_count_query: RowCountQuery) -> str:
        return f"COUNT({row_count_query.a})"

    def SUM(self: "QueryCompiler", sum_query: SumQuery) -> str:
        return f"SUM({sum_query.a})"

    def AND(self: "SqlQueryCompiler", and_type: AndQuery) -> str:
        return f"({and_type.a} and {and_type.b})"

    def OR(self: "SqlQueryCompiler", or_type: OrQuery) -> str:
        return f"({or_type.a} or {or_type.b})"
