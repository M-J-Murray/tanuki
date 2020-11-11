from pandas import DataFrame

from src.database.adapter.query.query_compiler import QueryCompiler


class MockQueryCompiler(QueryCompiler[DataFrame]):
    pass
