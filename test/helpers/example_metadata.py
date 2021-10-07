from dataclasses import dataclass
from datetime import datetime

from tanuki.data_store.metadata import Metadata


@dataclass
class ExampleMetadata(Metadata):
    test_str: str
    test_int: int
    test_float: float
    test_bool: bool
    test_timestamp: datetime
