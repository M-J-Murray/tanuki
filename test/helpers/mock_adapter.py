from __future__ import annotations

from functools import reduce
from typing import Any, Optional, TypeVar

import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_store.data_store import DataStore
from src.data_store.query import Query
from src.database.adapter.database_adapter import DatabaseAdapter, Indexible
from src.database.adapter.query.pandas_query_compiler import PandasQueryCompiler
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class MockAdapter(DatabaseAdapter):
    group_tables: dict[str, dict[str, DataFrame]]

    def __init__(self) -> None:
        self.group_tables = {}

    def has_group(self: MockAdapter, data_group: str) -> bool:
        return data_group in self.group_tables

    def create_group(self: MockAdapter, data_group: str) -> None:
        self.group_tables[data_group] = {}

    def has_group_table(self: MockAdapter, data_token: DataToken) -> bool:
        return (
            self.has_group(data_token.data_group)
            and data_token.table_name in self.group_tables[data_token.data_group]
        )

    def create_group_table(
        self: MockAdapter, data_token: DataToken, data_store_type: type[DataStore]
    ) -> None:
        self.group_tables[data_token.data_group][
            data_token.table_name
        ] = data_store_type().to_pandas()

    
    def drop_group(self: MockAdapter, data_group: str) -> None:
        del self.group_tables[data_group]

    def drop_group_table(self: MockAdapter, data_token: DataToken) -> None:
        del self.group_tables[data_token.data_group][data_token.table_name]

    def insert(
        self: MockAdapter,
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        existing = self.group_tables[data_token.data_group][data_token.table_name]
        combined = pd.concat(
            [existing, data_store.to_pandas()], ignore_index=ignore_index
        )
        combined.index.name = data_store.index.name
        self.group_tables[data_token.data_group][data_token.table_name] = combined

    def query(
        self: MockAdapter,
        data_token: DataToken,
        query: Optional[Query] = None,
        columns: Optional[list[str]] = None,
    ) -> Any:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        if len(data) == 0:
            return []

        if query is not None:
            query_compiler = PandasQueryCompiler(data)
            query = query_compiler.compile(query)
            if type(query) is not Series and type(query) is not DataFrame:
                return query
            else:
                data = data[query]
        if columns is not None:
            data = data[columns]
        if columns is None or "index" in columns:
            return [row for row in data.itertuples()]
        else:
            result = []
            for row in data.itertuples():
                if len(row) == 2:
                    result.append((row[1],))
                else:
                    result.append(row[1:])
            return result

    def update(
        self: MockAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        data = data.set_index(alignment_columns)
        new_data = data_store.to_pandas().set_index(alignment_columns)
        data.update(new_data)
        data = data.reset_index()
        self.group_tables[data_token.data_group][data_token.table_name] = data

    def upsert(
        self: MockAdapter,
        data_token: DataToken,
        data_store: T,
        alignment_columns: list[str],
    ) -> None:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        data = data.set_index(alignment_columns)
        new_data = data_store.to_pandas().set_index(alignment_columns)
        data = pd.concat([data, new_data[~new_data.index.isin(data.index)]])
        data.update(new_data)
        data = data.reset_index()
        self.group_tables[data_token.data_group][data_token.table_name] = data

    def delete(self: MockAdapter, data_token: DataToken, criteria: Query) -> None:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        query_compiler = PandasQueryCompiler(data)
        query = query_compiler.compile(criteria)
        indices = query.index[query]
        remaining = data.drop(indices)
        self.group_tables[data_token.data_group][data_token.table_name] = remaining

    def drop_indices(
        self: MockAdapter, data_token: DataToken, indices: Indexible
    ) -> None:
        data = self.group_tables[data_token.data_group][data_token.table_name]
        self.group_tables[data_token.data_group][data_token.table_name] = data.drop(indices)
