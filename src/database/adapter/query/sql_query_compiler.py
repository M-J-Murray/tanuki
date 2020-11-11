from src.data_store.query_type import AndType, EqualsType, NotEqualsType, OrType
from src.database.adapter.query.query_compiler import QueryCompiler


class SqlQueryCompiler(QueryCompiler[str]):
    def EQUALS(self: "SqlQueryCompiler", equals_type: EqualsType) -> str:
        return f"{equals_type.a}='{equals_type.b}'"

    def NOT_EQUALS(self: "SqlQueryCompiler", not_equals_type: NotEqualsType) -> str:
        return f"{not_equals_type.a}!='{not_equals_type.b}'"

    def AND(self: "SqlQueryCompiler", and_type: AndType) -> str:
        return f"({and_type.a} and {and_type.b})"

    def OR(self: "SqlQueryCompiler", or_type: OrType) -> str:
        return f"({or_type.a} or {or_type.b})"
