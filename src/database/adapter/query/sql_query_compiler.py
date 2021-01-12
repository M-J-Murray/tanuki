from src.data_store.query_type import AndQuery, EqualsQuery, NotEqualsQuery, OrQuery
from src.database.adapter.query.query_compiler import QueryCompiler


class SqlQueryCompiler(QueryCompiler[str]):
    def EQUALS(self: "SqlQueryCompiler", equals_type: EqualsQuery) -> str:
        return f"{equals_type.a}='{equals_type.b}'"

    def NOT_EQUALS(self: "SqlQueryCompiler", not_equals_type: NotEqualsQuery) -> str:
        return f"{not_equals_type.a}!='{not_equals_type.b}'"

    def AND(self: "SqlQueryCompiler", and_type: AndQuery) -> str:
        return f"({and_type.a} and {and_type.b})"

    def OR(self: "SqlQueryCompiler", or_type: OrQuery) -> str:
        return f"({or_type.a} or {or_type.b})"
