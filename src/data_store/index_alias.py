from .column_alias import ColumnAlias

class IndexAlias:
    name: str
    columns: list[ColumnAlias]

    def __init__(self, name: str, columns: list[ColumnAlias]) -> None:
        self.name = name
        self.columns = columns

    def __str__(self) -> str:
        return f"{self.name}: Index{self.columns}]"

    def __repr__(self) -> str:
        return str(self)