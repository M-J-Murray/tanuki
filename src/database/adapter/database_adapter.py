from src.data_store.data_store import DataStore
from src.database.data_token import DataToken


class DatabaseAdapter:

    def create_group(self: "DatabaseAdapter", data_group: str) -> None:
        ...

    def create_table(self: "DatabaseAdapter", data_token: DataToken, data_store_type: type[DataStore]) -> None:
        ...

    def drop_group(self: "DatabaseAdapter", data_group: str) -> None:
        ...

    def drop_table(self: "DatabaseAdapter", data_token: DataToken) -> None:
        ...

    def insert(self: "DatabaseAdapter", data_token: DataToken, data_store: DataStore) -> None:
        ...

    def update(self: "DatabaseAdapter", data_token: DataToken, data_store: DataStore) -> None:
        ...

    def upsert(self: "DatabaseAdapter") -> None:
        ...

    def query(self: "DatabaseAdapter") -> DataStore:
        ...

    def delete(self: "DatabaseAdapter") -> None:
        ...

    def stop(self: "DatabaseAdapter") -> None:
        ...
