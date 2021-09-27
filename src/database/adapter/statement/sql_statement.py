from typing import Any, cast, List, Optional, Tuple, Union

from src.database.data_token import DataToken


class SqlStatement:
    _commands: List[str]

    def __init__(self: "SqlStatement") -> None:
        self._commands = []

    def SELECT(self: "SqlStatement", *columns: str) -> "SqlStatement":
        columns_str = ", ".join([str(col) for col in columns])
        self._commands.append(f"SELECT {columns_str}")
        return self

    def SELECT_DISTINCT(self: "SqlStatement", *columns: str) -> "SqlStatement":
        columns_str = ", ".join([str(col) for col in columns])
        self._commands.append(f"SELECT DISTINCT {columns_str}")
        return self

    def SELECT_ALL(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("SELECT *")
        return self

    def FROM(self: "SqlStatement", source: str) -> "SqlStatement":
        self._commands.append(f"FROM {str(source)}")
        return self

    def INSERT_INTO(
        self: "SqlStatement",
        data_token: DataToken,
        *columns: str,
    ) -> "SqlStatement":
        cols_str = ", ".join([str(col) for col in columns])
        self._commands.append(f"INSERT INTO {data_token} ({cols_str})")
        return self

    def INSERT_ALL(
        self: "SqlStatement",
        data_token: DataToken,
    ) -> "SqlStatement":
        self._commands.append(f"INSERT INTO {data_token}")
        return self

    def VALUES(self: "SqlStatement", values: Tuple[Any, ...], quote: bool = True) -> "SqlStatement":
        adapted_values = []
        for val in values:
            if val is None:
                adapted_values.append("NULL")
            elif quote:
                adapted_values.append(f"'{val}'")
            else:
                adapted_values.append(str(val))
        str_def = ", ".join(adapted_values)
        self._commands.append(f"VALUES ({str_def})")
        return self

    def IN(self: "SqlStatement", values: Tuple[Any, ...], quote: bool = True) -> "SqlStatement":
        adapted_values = []
        for val in values:
            if val is None:
                adapted_values.append("NULL")
            elif quote:
                adapted_values.append(f"'{val}'")
            else:
                adapted_values.append(str(val))
        str_def = ", ".join(adapted_values)
        self._commands.append(f"IN ({str_def})")
        return self

    def UPDATE_FROM_LINK(
        self: "SqlStatement",
        source_token: Union[str, DataToken],
        target_token: Union[str, DataToken],
        update_columns: list[str],
        alignment_columns: list[str],
    ) -> "SqlStatement":
        cols = [str(col) for col in update_columns]
        
        cols_str = ",".join(cols)
        table_cols = [f"{source_token}.{col}" for col in cols]
        alignment_str = ",".join([f"{source_token}.{col}={target_token}.{col}" for col in alignment_columns])
        sub_statement = SqlStatement().SELECT(*table_cols).FROM(source_token).WHERE(alignment_str).compile(append_colon=False)

        self._commands.append(f"UPDATE {target_token} SET ({cols_str}) = ({sub_statement})")
        sub_statement2 = SqlStatement().SELECT_ALL().FROM(source_token).WHERE(alignment_str).compile(append_colon=False)
        self.WHERE(f"EXISTS({sub_statement2})")
        return self

    def UPDATE_FROM_VALUES(
        self: "SqlStatement",
        target_token: Union[str, DataToken],
        columns: list[str],
        values: list[str],
    ) -> "SqlStatement":
        col_names = [str(col) for col in columns]
        col_str = ", ".join([f"{col}={value}" for col, value in zip(col_names, values)])
        self._commands.append(f"UPDATE {target_token} SET {col_str}")
        return self

    def UPSERT_FROM_LINK(
        self: "SqlStatement",
        source_token: Union[str, DataToken],
        target_token: Union[str, DataToken],
        update_columns: list[str],
        alignment_columns: list[str],
    ) -> "SqlStatement":
        cols = [str(col) for col in update_columns]
        
        cols_str = ",".join(cols)
        table_cols = [f"{source_token}.{col}" for col in cols]
        alignment_str = ",".join([f"{source_token}.{col}={target_token}.{col}" for col in alignment_columns])
        sub_statement = SqlStatement().SELECT(*table_cols).FROM(source_token).WHERE(alignment_str).compile(append_colon=False)

        self._commands.append(f"UPDATE {target_token} SET ({cols_str}) = ({sub_statement})")
        sub_statement2 = SqlStatement().SELECT_ALL().FROM(source_token).WHERE(alignment_str).compile(append_colon=False)
        self.WHERE(f"EXISTS({sub_statement2})")
        self.UPDATE_CONFLICTS(alignment_columns, update_columns)
        return self

    def DELETE(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("DELETE")
        return self

    def CREATE_TABLE(
        self: "SqlStatement",
        table_name: str,
        schema_def: str,
        unlogged: bool = False,
    ) -> "SqlStatement":
        str_def = f"CREATE TABLE {table_name} ({schema_def})"
        if unlogged:
            str_def = "UNLOGGED " + str_def
        self._commands.append(str_def)
        return self

    def CREATE_TEMP_TABLE_LIKE(
        self: "SqlStatement",
        temp_table_name: str,
        reference_token: DataToken,
    ) -> "SqlStatement":
        self._commands.append(
            f"CREATE TEMPORARY TABLE {temp_table_name} "
            + f"(like {reference_token} INCLUDING DEFAULTS)"
        )
        return self

    def CREATE_INDEX(self: "SqlStatement", index_name: str, data_token: DataToken, column_list: list[str]) -> "SqlStatement":
        col_str = ",".join(column_list)
        self._commands.append(f"CREATE UNIQUE INDEX {data_token.data_group}.{data_token.table_name}_{index_name} ON {data_token.table_name} ({col_str})")
        return self

    def ATTACH_DATABASE(self: "SqlStatement", file_path: str, data_group: str) -> "SqlStatement":
        self._commands.append(f"ATTACH DATABASE '{file_path}/{data_group}.db'")
        return self

    def DETACH_DATABASE(self: "SqlStatement", data_group: str) -> "SqlStatement":
        self._commands.append(f"DETACH DATABASE {data_group}")
        return self

    def CREATE_SCHEMA(self: "SqlStatement", data_group: str) -> "SqlStatement":
        self._commands.append(f"CREATE SCHEMA {data_group}")
        return self

    def DROP_TABLE(self: "SqlStatement", data_token: DataToken) -> "SqlStatement":
        self._commands.append(f"DROP TABLE {data_token}")
        return self

    def AND(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("AND")
        return self

    def OR(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("OR")
        return self

    def MIN(self: "SqlStatement", value: Any) -> "SqlStatement":
        self._commands.append(f"MIN({value})")
        return self

    def MAX(self: "SqlStatement", value: Any) -> "SqlStatement":
        self._commands.append(f"MAX({value})")
        return self

    def COUNT(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("COUNT(*)")
        return self

    def AS(self: "SqlStatement", alias: str) -> "SqlStatement":
        self._commands.append(f"AS {alias}")
        return self

    def LIKE(self: "SqlStatement", column: str, value: Any) -> "SqlStatement":
        self._commands.append(f"{column} LIKE '{value}'")
        return self

    def WHERE(
        self: "SqlStatement", other: Optional[str] = None
    ) -> "SqlStatement":
        if other is None:
            self._commands.append("WHERE")
        else:
            self._commands.append(f"WHERE {other}")
        return self

    def LIMIT(self: "SqlStatement", limit: int) -> "SqlStatement":
        self._commands.append(f"LIMIT {limit}")
        return self

    def ORDER_BY(
        self: "SqlStatement",
        *columns: str,
        ascending: Union[bool, List[bool]],
    ) -> "SqlStatement":
        col_list: List[str] = (
            [str(col) for col in columns] if type(columns) is list else [str(columns)]
        )
        asc_list: List[bool] = (
            cast(List[bool], ascending)
            if type(ascending) is list
            else [cast(bool, ascending) for _ in range(len(col_list))]
        )
        str_def = []
        for col, asc in zip(col_list, asc_list):
            asc_str = "ASC" if asc else "DESC"
            str_def.append(f"{col} {asc_str}")
        self._commands.append("ORDER BY " + ", ".join(str_def))
        return self

    def GROUP_BY(self: "SqlStatement", *columns: str) -> "SqlStatement":
        columns_str = ", ".join([str(col) for col in columns])
        self._commands.append(f"GROUP BY {columns_str}")
        return self

    def EQUAL(self: "SqlStatement", column: str, value: str) -> "SqlStatement":
        self._commands.append(f"{column}='{value}'")
        return self

    def NOT_EQUAL(self: "SqlStatement", column: str, value: str) -> "SqlStatement":
        self._commands.append(f"{column}!='{value}'")
        return self

    def COLUMNS_EQUAL(
        self: "SqlStatement",
        source_token: Union[str, DataToken, "SqlStatement"],
        source_column: str,
        target_token: Union[str, DataToken, "SqlStatement"],
        target_column: str,
    ) -> "SqlStatement":
        self._commands.append(
            f"{source_token}.{source_column}=" + f"{target_token}.{target_column}"
        )
        return self

    def EQUAL_MANY(
        self: "SqlStatement", column: str, values: List[Any]
    ) -> "SqlStatement":
        str_def = ", ".join([str(val) for val in values])
        self._commands.append(f"{column} IN ({str_def})")
        return self

    def GREATER_THAN(self: "SqlStatement", column: str, value: str) -> "SqlStatement":
        self._commands.append(f"{column}>'{value}'")
        return self

    def LESS_THAN(self: "SqlStatement", column: str, value: str) -> "SqlStatement":
        self._commands.append(f"{column}>'{value}'")
        return self

    def ADD(self: "SqlStatement", value1: Any, value2: Any) -> "SqlStatement":
        self._commands.append(f"{value1}+{value2}")
        return self

    def SUB(self: "SqlStatement", value1: Any, value2: Any) -> "SqlStatement":
        self._commands.append(f"{value1}-{value2}")
        return self

    def DIV(self: "SqlStatement", value1: Any, value2: Any) -> "SqlStatement":
        self._commands.append(f"{value1}/{value2}")
        return self

    def MUL(self: "SqlStatement", value1: Any, value2: Any) -> "SqlStatement":
        self._commands.append(f"{value1}*{value2}")
        return self

    def IN_RANGE(
        self: "SqlStatement",
        value: str,
        low_value: str,
        high_value: str,
    ) -> "SqlStatement":
        self._commands.append(f"{value} BETWEEN {low_value} AND {high_value}")
        return self

    def SKIP_CONFLICTS(self: "SqlStatement") -> "SqlStatement":
        self._commands.append("ON CONFLICT DO NOTHING")
        return self

    def UPDATE_CONFLICTS(
        self: "SqlStatement",
        unique_columns: List[str],
        update_columns: List[str],
    ) -> "SqlStatement":
        unique_str = ", ".join([str(col) for col in unique_columns])
        col_str = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
        self._commands.append(f"ON CONFLICT ({unique_str}) DO UPDATE SET {col_str}")
        return self

    def compile(self: "SqlStatement", append_colon: bool = True) -> str:
        result = " ".join(self._commands)
        if append_colon:
            result += ";"
        return result

    def __str__(self: "SqlStatement") -> str:
        return self.compile()

    def __repr__(self: "SqlStatement") -> str:
        return self.compile()
