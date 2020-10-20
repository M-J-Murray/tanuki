from src.data_store.data_type import DataType
from typing import cast, get_args, get_type_hints, Optional, TypeVar

from pandas import DataFrame
from pandas.core.arrays.sparse import dtype

from src.data_store.column import Column, ColumnAlias

T = TypeVar("T", bound="DataStore")


class DataStore:
    data_frame: DataFrame

    def __init_subclass__(cls) -> None:
        super(DataStore, cls).__init_subclass__()
        for name, col in cls.columns().items():
            col._name = name
            setattr(cls, name, col)

    def __init__(self, column_data: Optional[dict[str, Column]] = None) -> None:
        if column_data:
            column_data = {str(name): col for name, col in column_data.items()}
            self.data_frame = DataFrame(column_data)
            self._validate_data_frame()
            self._attach_columns()
        else:
            self.data_frame = DataFrame()

    @classmethod
    def columns(cls) -> dict[str, ColumnAlias]:
        variables = get_type_hints(cls)
        columns: dict[str, Column] = {}
        missing_types = []
        for name, col in variables.items():
            if type(col) is Column:
                missing_types.append(name)
            elif type(col) is ColumnAlias:
                columns[name] = col
        if len(missing_types) > 0:
            raise TypeError(
                f"Failed to find column types for the following columns: {missing_types}"
            )
        return columns

    def _validate_data_frame(self) -> None:
        columns = self.columns()

        missing_cols = []
        invalid_types = []
        for name, col in columns.items():
            if name not in self.data_frame:
                missing_cols.append(name)
            data = Column(self.data_frame[name])
            if data.dtype != col.dtype:
                invalid_types.append(name)

        errors = []
        if len(missing_cols) != 0:
            errors.append(f"Column data was missing for: {missing_cols}")
        if len(invalid_types) != 0:
            errors.append(f"Invalid types provided for: {invalid_types}")
        if len(errors) != 0:
            raise ValueError("\n".join(errors))

    def _attach_columns(self) -> None:
        columns = self.columns()
        for col in columns:
            if col in self.data_frame:
                setattr(self, col, self.data_frame[col])
            else:
                setattr(self, col, None)

    @classmethod
    def _new_data_copy(cls: type[T], data_frame: DataFrame) -> T:
        instance: T = cls()
        instance.data_frame = data_frame
        instance._attach_columns()
        return instance

    def reset_index(self: T, drop: bool = True) -> T:
        return self._new_data_copy(self.data_frame.reset_index(drop=drop))

    def __contains__(self, key):
        if type(key) is str:
            return key in self.columns()
        else:
            return len(self.data_frame.merge(key)) == len(self.data_frame)

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        oc = cast(DataStore, other)
        return self.data_frame.equals(oc.data_frame)

    def __len__(self):
        return len(self.data_frame)

    def __iter__(self):
        return iter(self.data_frame)

    def __getitem__(self, item: str) -> Column:
        return self.columns()[item](self.data_frame[item])

    def __str__(self: "DataStore") -> str:
        """TODO!"""
        if len(self.data_frame) == 0:
            return (
                f"Empty {self.__class__.__name__}"
            )
        else:
            return (
                f"{self.__class__.__name__}n{self.data_frame}"
            )

    def __repr__(self: "DataStore") -> str:
        """TODO!"""
        return str(self)

