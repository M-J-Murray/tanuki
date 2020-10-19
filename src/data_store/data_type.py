from pandas import Series

from src.util.time_util import (
    datetime,
    format_datetime,
    format_delta,
    parse_datetime,
    parse_delta,
    timedelta,
)


class DataType(type):
    @staticmethod
    def sql_def() -> str:
        ...

    @staticmethod
    def dataframe_cast(data: Series) -> Series:
        return data

    @staticmethod
    def database_cast(data: Series) -> Series:
        return data


class String(str, metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "TEXT"


class Boolean(metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "INTEGER"


class Float32(metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "REAL"


class Float64(metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "REAL"


class Int64(metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "INTEGER"


class Int32(metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "INTEGER"


class Timestamp(datetime, metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "TEXT"

    @staticmethod
    def dataframe_cast(data: Series) -> Series:
        return data.apply(parse_datetime)

    @staticmethod
    def database_cast(data: Series) -> Series:
        return data.apply(format_datetime)


class Timedelta(timedelta, metaclass=DataType):
    @staticmethod
    def sql_def() -> str:
        return "TEXT"

    @staticmethod
    def dataframe_cast(data: Series) -> Series:
        return data.apply(parse_delta)

    @staticmethod
    def database_cast(data: Series) -> Series:
        return data.apply(format_delta)
