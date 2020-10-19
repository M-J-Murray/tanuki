from typing import cast, get_args, get_type_hints

from pandas import DataFrame

from src.data_store.column import Column, ColumnAlias


class DataStore:
    data_frame: DataFrame

    def __init_subclass__(cls) -> None:
        super(DataStore, cls).__init_subclass__()
        for name, col in cls.columns().items():
            col._name = name
            setattr(cls, name, col)

    def __init__(self, column_data: dict[str, Column]) -> None:
        column_data = {str(name): col for name, col in column_data.items()}
        self.data_frame = DataFrame(column_data)
        self._validate_data_frame()
        self._attach_columns()

    @classmethod
    def sql_def(cls) -> str:
        entries = ["id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"]
        for name, col in cls.columns().items():
            entries.append(f"{name} {col.sql_def()}")
        return "\n".join(entries)

    @classmethod
    def columns(cls) -> dict[str, Column]:
        variables = get_type_hints(cls)
        columns: dict[str, Column] = {}
        missing_types = []
        for name, col in variables.items():
            if type(col) is Column:
                missing_types.append(name)
            elif type(col) is ColumnAlias:
                columns[name] = col
        if len(missing_types) > 0:
            raise TypeError(f"Failed to find column types for the following columns: {missing_types}")
        return columns

    def _validate_data_frame(self) -> None:
        columns = self.columns()

        missing_cols = []
        invalid_types = []
        for col in columns:
            if col not in self.data_frame:
                missing_cols.append(col)
            data = columns[col]
            if col._data_type != data._data_type:
                invalid_types.append(col._name)

        if len(missing_cols) != 0:
            raise ValueError(
                f"The following columns were missing from the data frame: {missing_cols}"
            )

    def _attach_columns(self) -> None:
        columns = self.columns()
        for col in columns:
            if col in self.data_frame:
                setattr(self, col, self.data_frame[col])
            else:
                setattr(self, col, None)

    def __contains__(self, key):
        if type(key) is str:
            return key in self.columns()
        else:
            return len(self.data_frame.merge(key)) == len(self.data_frame)

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        oc = cast(DataStore, other)
        return self.data_frame.equals(oc.data_frame)

    def __len__(self):
        return len(self.data_frame)

    def __iter__(self):
        for i in range(len(self)):
            yield tuple(self.data_frame.iloc[i])

    def __getitem__(self, item):
        if type(item) is str or type(item) is list and get_args(item)[0] is str:
            return type(self)(self.data_frame[item])
        else:
            return type(self)(self.data_frame.iloc[item])

    def __str__(self) -> str:
        return str(self.data_frame)

    def __repr__(self) -> str:
        return repr(self.data_frame)
