from datetime import datetime

from tanuki.data_store.metadata import Metadata


class ExampleMetadata(Metadata):
    test_str: str
    test_int: int
    test_float: float
    test_bool: bool
    test_timestamp: datetime

    def test_func(self) -> str:
        return "foo"
