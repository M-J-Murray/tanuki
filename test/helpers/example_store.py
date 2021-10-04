from __future__ import annotations
from src.tanuki.data_store.column import Column
from src.tanuki.data_store.data_store import DataStore
from src.tanuki.database.data_token import DataToken
from src.tanuki.data_store.index.index import Index

RAW_GROUP = "raw"

class ExampleStore(DataStore):
    data_token = DataToken("test", RAW_GROUP)

    a: Column[str]
    b: Column[int]
    c: Column[bool]

    a_index: Index[a]
    ab_index: Index[a, b]

    def test_method(self: "ExampleStore") -> str:
        return "test_result"
