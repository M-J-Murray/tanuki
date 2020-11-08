from attr import dataclass
from hamcrest import assert_that, close_to, equal_to
import pytest

from src.data_store.column import Column
from src.data_store.data_type import (
    Array,
    Boolean,
    Bytes,
    DataType,
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Object,
    String,
    Timedelta,
    Timestamp,
    TypeAlias,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)


@dataclass
class CustomObject:
    value: str


test_data = [
    (String, ["a", "b", "c"]),
    (Boolean, [True, False, True]),
    (Float64, [1.2, 2.3, 3.4]),
    (Float32, [1.2, 2.3, 3.4]),
    (Float16, [1.2, 2.3, 3.4]),
    (Int64, [1, 2, 3]),
    (Int32, [1, 2, 3]),
    (Int16, [1, 2, 3]),
    (Int8, [1, 2, 3]),
    (UInt64, [1, 2, 3]),
    (UInt32, [1, 2, 3]),
    (UInt16, [1, 2, 3]),
    (UInt8, [1, 2, 3]),
    (Timestamp, [0, 1, 2]),
    (Timedelta, [1, 2, 3]),
    (Bytes, [b"a", b"b", b"c"]),
    (Array, [[1], [2], [3]]),
    (TypeAlias(list[int]), [[1], [2], [3]]),
    (DataType(list[int]), [[1], [2], [3]]),
    (Object, [CustomObject("a"), CustomObject("b"), CustomObject("c")]),
]


@pytest.mark.parametrize("data_type, data", test_data)
def test_all_column_types(data_type: DataType, data: list) -> None:
    for equiv in data_type.equivalents():
        test_col = Column[equiv](data)
        assert_that(test_col.dtype, equal_to(data_type))
        assert_that(test_col.dtype.pdtype(), equal_to(data_type.pdtype()))
        col_data = test_col.tolist()
        if isinstance(col_data[0], float):
            for col_value, value in zip(col_data, data):
                assert_that(col_value, close_to(value, 1e-3))
        else:
            assert_that(col_data, equal_to(data))
