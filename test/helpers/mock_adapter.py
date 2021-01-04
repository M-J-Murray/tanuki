from typing import Optional, TypeVar

from pandas.core.frame import DataFrame
import pandas as pd

from src.data_store.data_store import DataStore
from src.data_store.query_type import QueryType
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.adapter.query.pandas_query_compiler import PandasQueryCompiler
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class MockAdapter(DatabaseAdapter):
    group_tables: dict[str, dict[str, DataFrame]]

    def __init__(self) -> None:
        self.group_tables = {}

    def has_group(self: "MockAdapter", data_group: str) -> bool:
        return data_group in self.group_tables

    def create_group(self: "MockAdapter", data_group: str) -> None:
        self.group_tables[data_group] = {}

    def has_table(self: "MockAdapter", data_token: DataToken) -> bool:
        return (
            self.has_group(data_token.data_group)
            and data_token.table_name in self.group_tables[data_token.data_group]
        )

    def create_table(
        self: "MockAdapter", data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        self.group_tables[data_token.data_group][
            data_token.table_name
        ] = data_store_type().to_pandas()

    def drop_group(self: "MockAdapter", data_group: str) -> None:
        del self.group_tables[data_group]

    def drop_table(self: "MockAdapter", data_token: DataToken) -> None:
        del self.group_tables[data_token.data_group][data_token.table_name]

    def insert(
        self: "MockAdapter",
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        existing = self.group_tables[data_token.data_group][data_token.table_name]
        if data_store.is_row():
            data_store = data_store.to_table()
        combined = pd.concat([existing, data_store.to_pandas()], ignore_index=ignore_index)
        self.group_tables[data_token.data_group][data_token.table_name] = combined

    def query(
        self: "MockAdapter",
        data_token: DataToken,
        query_type: Optional[QueryType] = None,
        columns: Optional[list[str]] = None,
    ) -> list[tuple]:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        if len(data) == 0:
            return []

        if query_type is not None:
            query_compiler = PandasQueryCompiler(data)
            query = query_compiler.compile(query_type)
            data = data[query]
        if columns is not None:
            data = data[columns]
        return [row[1:] for row in data.itertuples()]

    def delete(self: "MockAdapter", data_token: DataToken, criteria: QueryType) -> None:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        query_compiler = PandasQueryCompiler(data)
        query = query_compiler.compile(criteria)
        remaining = data.drop(query)
        self.group_tables[data_token.data_group][data_token.table_name] = remaining
