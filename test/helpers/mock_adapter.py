from src.data_store.query_type import QueryType
from typing import Any, Optional, TypeVar

from src.data_store.data_store import DataStore
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class MockAdapter(DatabaseAdapter):
    group_tables: dict[str, dict[str, DataStore]]

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
        ] = data_store_type()

    def drop_group(self: "MockAdapter", data_group: str) -> None:
        del self.group_tables[data_group]

    def drop_table(self: "MockAdapter", data_token: DataToken) -> None:
        del self.group_tables[data_token.data_group][data_token.table_name]

    def query(
        self: "MockAdapter",
        data_token: DataToken,
        query_type: Optional[QueryType],
        columns: Optional[list[str]],
    ) -> list[tuple[Any, ...]]:
        data = self.group_tables[data_token.data_group][data_token.table_name]
