from typing import Any, ClassVar
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from abc import abstractclassmethod

class DataType:
    _type_mappings: ClassVar[dict[type:"DataType"]] = {}

    def __init_subclass__(cls) -> None:
        super(DataType, cls).__init_subclass__()
        for type_equiv in cls.equivalents():
            cls._type_mappings[type_equiv] = cls

    def __new__(cls, data_type: type) -> "DataType":
        if data_type in cls._type_mappings:
            return cls._type_mappings[data_type]
        else:
            return data_type

    @abstractclassmethod
    def pdtype(cls) -> type:
        ...

    @abstractclassmethod
    def equivalents(cls) -> tuple[type]:
        ...

    @classmethod
    def __eq__(cls, o: type) -> bool:
        return cls == o or cls.pdtype() == 0


class String(DataType):
    @classmethod
    def pdtype(cls):
        return np.str

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.str), np.str, str)


class Boolean(DataType):
    @classmethod
    def pdtype(cls):
        return np.bool

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.bool), np.bool, bool)


class Float64(DataType):
    @classmethod
    def pdtype(cls):
        return np.float64

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.float64), np.float64, np.float, float)


class Float32(DataType):
    @classmethod
    def pdtype(cls):
        return np.float32

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.float32),
            np.float32,
        )


class Float16(DataType):
    @classmethod
    def pdtype(cls):
        return np.float16

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.float16),
            np.float16,
        )


class Int64(DataType):
    @classmethod
    def pdtype(cls):
        return np.int64

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.int64), np.int64, np.int, int)


class Int32(DataType):
    @classmethod
    def pdtype(cls):
        return np.int32

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.int32),
            np.int32,
        )


class Int16(DataType):
    @classmethod
    def pdtype(cls):
        return np.int16

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.int16),
            np.int16,
        )


class Int8(DataType):
    @classmethod
    def pdtype(cls):
        return np.int8

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.int8),
            np.int8,
        )


class UInt64(DataType):
    @classmethod
    def pdtype(cls):
        return np.uint64

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.uint64),
            np.uint64,
        )


class UInt32(DataType):
    @classmethod
    def pdtype(cls):
        return np.uint32

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.uint32),
            np.uint32,
        )


class UInt16(DataType):
    @classmethod
    def pdtype(cls):
        return np.uint16

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.uint16),
            np.uint16,
        )


class UInt8(DataType):
    @classmethod
    def pdtype(cls):
        return np.uint8

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (
            np.dtype(np.uint8),
            np.uint8,
        )


class Timestamp(DataType):
    @classmethod
    def pdtype(cls):
        return pd.Timestamp

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (pd.Timestamp, datetime)


class Timedelta(DataType):
    @classmethod
    def pdtype(cls):
        return pd.Timedelta

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (pd.Timedelta, timedelta)


class Array(DataType):
    @classmethod
    def pdtype(cls):
        return np.ndarray

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.ndarray, list, tuple)


class Object(DataType):
    @classmethod
    def pdtype(cls):
        return np.object

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.object), np.object, object, Any)
