from enum import Enum

from src.data_store.data_type import (
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
    String,
    Timedelta,
    Timestamp, TypeAlias,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Array,
)


class Sqlite3Type(str, Enum):
    TEXT = "TEXT"
    INTEGER = "INTEGER"
    REAL = "REAL"
    BLOB = "BLOB"

    _mappings: dict[DataType, str] = {
        String: TEXT,
        Boolean: INTEGER,
        Float64: REAL,
        Float32: REAL,
        Float16: REAL,
        Int64: INTEGER,
        Int32: INTEGER,
        Int16: INTEGER,
        Int8: INTEGER,
        UInt64: INTEGER,
        UInt32: INTEGER,
        UInt16: INTEGER,
        UInt8: INTEGER,
        Timestamp: INTEGER,
        Timedelta: INTEGER,
        Bytes: BLOB,
        Array: TEXT,
        object: BLOB,
    }

    def __call__(self, data_type: DataType) -> str:
        while isinstance(data_type, TypeAlias):
            data_type = data_type.pdtype()
        if data_type not in self._mappings:
            raise ValueError(f"Sqlite3 does not support type {data_type}")
        return self._mappings[data_type]
