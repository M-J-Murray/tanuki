from dataclasses import dataclass
from typing import ClassVar

from src.database.adapter.database_adapter import DatabaseAdapter


@dataclass
class DataToken:
    _active_db: ClassVar[DatabaseAdapter]

    table_name: str
    data_group: str

    def __init__(self, table_name: str, data_group: str) -> None:
        self.table_name = table_name
        self.data_group = data_group

    def to_sql(self: "DataToken") -> str:
        return f"{self.data_group}.{self.table_name}"
