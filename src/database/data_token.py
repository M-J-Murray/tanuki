from dataclasses import dataclass
from typing import ClassVar

from src.database.database import Database


@dataclass
class DataToken:
    _active_db: ClassVar[Database]

    table_name: str
    data_group: str

    def __init__(self, table_name: str, data_group: str) -> None:
        self.table_name = table_name
        self.data_group = data_group
