from dataclasses import dataclass


@dataclass
class DataToken:
    table_name: str
    data_group: str

    def __init__(self: "DataToken", table_name: str, data_group: str) -> None:
        self.table_name = table_name
        self.data_group = data_group

    def to_sql(self: "DataToken") -> str:
        return f"{self.data_group}.{self.table_name}"
