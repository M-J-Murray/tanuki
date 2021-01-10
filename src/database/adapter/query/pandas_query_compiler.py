from typing import Any, Union

from pandas import DataFrame

from src.data_store.column_alias import ColumnAlias
from src.data_store.query_type import (
    AndType,
    CountType,
    EqualsType,
    GreaterEqualType,
    GreaterThanType,
    LessEqualType,
    LessThanType,
    NotEqualsType,
    OrType,
)
from src.database.adapter.query.query_compiler import QueryCompiler


class PandasQueryCompiler(QueryCompiler[DataFrame]):
    _data_frame: DataFrame

    def __init__(self, data_frame: DataFrame) -> None:
        self._data_frame = data_frame

    def _get_value(
        self: "PandasQueryCompiler", parameter: Union[Any, ColumnAlias]
    ) -> Any:
        if type(parameter) is ColumnAlias:
            return self._data_frame[parameter._name]
        else:
            return parameter

    def EQUALS(self: "PandasQueryCompiler", equals_type: EqualsType) -> DataFrame:
        return self._get_value(equals_type.a) == self._get_value(equals_type.b)

    def NOT_EQUALS(
        self: "PandasQueryCompiler", not_equals_type: NotEqualsType
    ) -> DataFrame:
        return self._get_value(not_equals_type.a) != self._get_value(not_equals_type.b)

    def GREATER_THAN(
        self: "PandasQueryCompiler", gt_type: GreaterThanType
    ) -> DataFrame:
        return self._get_value(gt_type.a) > self._get_value(gt_type.b)

    def GREATER_EQUAL(
        self: "PandasQueryCompiler", ge_type: GreaterEqualType
    ) -> DataFrame:
        return self._get_value(ge_type.a) >= self._get_value(ge_type.b)

    def LESS_THAN(self: "PandasQueryCompiler", lt_type: LessThanType) -> DataFrame:
        return self._get_value(lt_type.a) < self._get_value(lt_type.b)

    def LESS_EQUAL(self: "PandasQueryCompiler", le_type: LessEqualType) -> DataFrame:
        return self._get_value(le_type.a) <= self._get_value(le_type.b)

    def COUNT(self: "PandasQueryCompiler", count_type: CountType) -> DataFrame:
        return len(self._get_value(count_type.a))

    def AND(self: "PandasQueryCompiler", and_type: AndType) -> DataFrame:
        return self._get_value(and_type.a) & self._get_value(and_type.b)

    def OR(self: "PandasQueryCompiler", or_type: OrType) -> DataFrame:
        return self._get_value(or_type.a) | self._get_value(or_type.b)
