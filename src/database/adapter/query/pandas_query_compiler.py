from typing import Any

from pandas import DataFrame

from src.data_store.column_alias import ColumnAlias
from src.data_store.query import (
    AndGroupQuery,
    AndQuery,
    EqualsQuery,
    GreaterEqualQuery,
    GreaterThanQuery,
    LessEqualQuery,
    LessThanQuery,
    NotEqualsQuery,
    OrGroupQuery,
    OrQuery,
    RowCountQuery,
)
from src.database.adapter.query.query_compiler import QueryCompiler


class PandasQueryCompiler(QueryCompiler[DataFrame]):
    _data_frame: DataFrame

    def __init__(self, data_frame: DataFrame) -> None:
        self._data_frame = data_frame

    def _get_value(
        self: "PandasQueryCompiler", parameter: Any
    ) -> Any:
        if type(parameter) is ColumnAlias:
            return self._data_frame[parameter.name]
        else:
            return parameter

    def EQUALS(self: "PandasQueryCompiler", equals_type: EqualsQuery) -> DataFrame:
        return self._get_value(equals_type.a) == self._get_value(equals_type.b)

    def NOT_EQUALS(
        self: "PandasQueryCompiler", not_equals_type: NotEqualsQuery
    ) -> DataFrame:
        return self._get_value(not_equals_type.a) != self._get_value(not_equals_type.b)

    def GREATER_THAN(
        self: "PandasQueryCompiler", gt_type: GreaterThanQuery
    ) -> DataFrame:
        return self._get_value(gt_type.a) > self._get_value(gt_type.b)

    def GREATER_EQUAL(
        self: "PandasQueryCompiler", ge_type: GreaterEqualQuery
    ) -> DataFrame:
        return self._get_value(ge_type.a) >= self._get_value(ge_type.b)

    def LESS_THAN(self: "PandasQueryCompiler", lt_type: LessThanQuery) -> DataFrame:
        return self._get_value(lt_type.a) < self._get_value(lt_type.b)

    def LESS_EQUAL(self: "PandasQueryCompiler", le_type: LessEqualQuery) -> DataFrame:
        return self._get_value(le_type.a) <= self._get_value(le_type.b)

    def ROW_COUNT(self: "PandasQueryCompiler", count_type: RowCountQuery) -> DataFrame:
        return len(self._get_value(count_type.a))

    def SUM(self: "PandasQueryCompiler", count_type: RowCountQuery) -> DataFrame:
        return sum(self._get_value(count_type.a))

    def AND(self: "PandasQueryCompiler", and_type: AndQuery) -> DataFrame:
        return self._get_value(and_type.a) & self._get_value(and_type.b)

    def AND_GROUP(self: "PandasQueryCompiler", and_group_type: AndGroupQuery) -> DataFrame:
        result = None
        for item in and_group_type.items:
            if result is None:
                result = self._get_value(item)
            else:
                result = result & self._get_value(item)
        return result

    def OR(self: "PandasQueryCompiler", or_type: OrQuery) -> DataFrame:
        return self._get_value(or_type.a) | self._get_value(or_type.b)

    def OR_GROUP(self: "PandasQueryCompiler", and_group_type: OrGroupQuery) -> DataFrame:
        result = None
        for item in and_group_type.items:
            if result is None:
                result = self._get_value(item)
            else:
                result = result | self._get_value(item)
        return result
