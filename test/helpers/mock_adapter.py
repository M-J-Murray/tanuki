from typing import TypeVar

from src.data_store.data_store import DataStore
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.data_token import DataToken

T = TypeVar("T", bound=DataStore)


class MockAdapter(DatabaseAdapter):
    group_tables: dict[str, dict[str, DataStore]]

    def __init__(self) -> None:
        self.group_tables = {}

    def create_group(self: "MockAdapter", data_group: str) -> None:
        self.group_tables[data_group] = {}

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

    def insert(
        self: "MockAdapter",
        data_token: DataToken,
        data_store: T,
        ignore_index: bool = False,
    ) -> None:
        current_data = self.group_tables[data_token.data_group][data_token.table_name]
        store_class = data_store.__class__
        new_data = store_class.concat(
            [current_data, data_store], ignore_index=ignore_index
        )
        self.group_tables[data_token.data_group][data_token.table_name] = new_data

    def update(
        self: "MockAdapter",
        data_token: DataToken,
        data_store: DataStore,
        *columns: str
    ) -> None:
        ...

    def upsert(
        self: "DatabaseAdapter",
        data_token: DataToken,
        data_store: DataStore,
        *columns: str
    ) -> None:
        ...

    def query(self: "MockAdapter") -> DataStore:
        ...

    def delete(self: "MockAdapter") -> None:
        ...

    def stop(self: "MockAdapter") -> None:
        ...
