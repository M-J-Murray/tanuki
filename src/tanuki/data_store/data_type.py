from __future__ import annotations

from abc import abstractmethod
from datetime import datetime, timedelta
from types import GenericAlias
from typing import Any, cast, ClassVar, TypeVar

import numpy as np
import pandas as pd


class MetaDataType(type):
    _type_mappings: ClassVar[dict[type, DataType]] = {}

    def __call__(cls, data_type: type) -> type:
        if cls is TypeAlias:
            instance = TypeAlias.__new__(cls)
            instance.__init__(data_type)
            return instance
        elif type(data_type) is GenericAlias:
            return TypeAlias(data_type)
        elif data_type in cls._type_mappings:
            return cls._type_mappings[data_type]
        else:
            return data_type

    def __init__(cls, name: str, *_) -> None:
        if name != "DataType" and name != "TypeAlias":
            for type_equiv in cls.equivalents():
                cls._type_mappings[type_equiv] = cls

    @abstractmethod
    def pdtype(cls) -> type:
        raise NotImplementedError()

    @abstractmethod
    def equivalents(cls) -> tuple[type]:
        raise NotImplementedError()

    def __str__(cls) -> str:
        return cls.__name__

    def __repr__(cls) -> str:
        return cls.__name__


class DataType(metaclass=MetaDataType):
    @classmethod
    def __eq__(cls, o: type) -> bool:
        return cls == o or cls.pdtype() == o


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
        return (np.dtype(np.bool), np.bool, np.bool_, bool)


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
        return np.dtype("<M8[ns]")

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype("<M8[ns]"), np.datetime64, pd.Timestamp, datetime)


class Timedelta(DataType):
    @classmethod
    def pdtype(cls):
        return np.dtype("<m8[ns]")

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype("<m8[ns]"), np.timedelta64, pd.Timedelta, timedelta)


class Bytes(DataType):
    @classmethod
    def pdtype(cls):
        return bytes

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (bytes,)


DT = TypeVar("DT", bound=DataType)

class Array(DataType):
    @classmethod
    def pdtype(cls):
        return np.ndarray

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.ndarray, list, tuple)


class TypeAlias(DataType):
    _alias_type: type
    _nested_dt: type

    def __init__(self, data_type: GenericAlias) -> None:
        self._alias_type = DataType(data_type.__origin__)
        self._nested_dt = DataType(data_type.__args__[0])

    def pdtype(self):
        return self._alias_type

    def equivalents(self) -> tuple[type]:
        return (self,)

    def __eq__(self, t: type) -> bool:
        if type(t) is not TypeAlias:
            return t is Array and self._alias_type == Array
        ct = cast(TypeAlias, t)
        return self._alias_type is ct._alias_type and self._nested_dt == ct._nested_dt

    def __ne__(self, t: type) -> bool:
        return not self.__eq__(t)

    def __str__(self) -> str:
        alias_name = (
            str(self._alias_type)
            if self._alias_type is DataType
            else self._alias_type.__name__
        )
        nested_name = (
            str(self._nested_dt)
            if self._nested_dt is DataType
            else self._nested_dt.__name__
        )
        return f"{alias_name}[{nested_name}]"

    def __repr__(self) -> str:
        return str(self)

    @property
    def __name__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash(str(self))


class Object(DataType):
    @classmethod
    def pdtype(cls):
        return np.object

    @classmethod
    def equivalents(cls) -> tuple[type]:
        return (np.dtype(np.object), np.object, object, Any)
