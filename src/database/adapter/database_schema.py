from src.data_store.column import Column
from src.data_store.data_store import DataStore


class SchemaDefinition(DataStore):
    column_name: Column[str]
    column_type: Column[str]
    column_constraints: Column[str]


class DatabaseSchema:
    def schema_definition(self) -> SchemaDefinition:
        ...
