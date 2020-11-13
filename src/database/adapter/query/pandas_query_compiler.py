from typing import Any, Union
from pandas import DataFrame

from src.data_store.query_type import AndType, EqualsType, NotEqualsType, OrType
from src.database.adapter.query.query_compiler import QueryCompiler
from src.data_store.column_alias import ColumnAlias


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

    def NOT_EQUALS(self: "PandasQueryCompiler", not_equals_type: NotEqualsType) -> DataFrame:
        return self._get_value(not_equals_type.a) != self._get_value(not_equals_type.b)

    def AND(self: "PandasQueryCompiler", and_type: AndType) -> DataFrame:
        return self._get_value(and_type.a) & self._get_value(and_type.b)

    def OR(self: "PandasQueryCompiler", or_type: OrType) -> DataFrame:
        return self._get_value(or_type.a) | self._get_value(or_type.b)
