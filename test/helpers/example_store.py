from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.data_token import DataToken

RAW_GROUP = "raw"


class ExampleStore(DataStore):
    data_token = DataToken("test", RAW_GROUP)

    a: Column[str]
    b: Column[int]
    c: Column[bool]

    def test_method(self: "ExampleStore") -> str:
        return "test_result"
