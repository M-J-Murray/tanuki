from __future__ import annotations

from typing import Any, Union, cast, Generic, get_type_hints, Type, TypeVar

from pandas import DataFrame

from src.data_store.column import Column, ColumnAlias

T = TypeVar("T", bound="DataStore")


class DataStore:
    data_frame: DataFrame
    loc: DataStore._LocIndexerFrame
    iloc: DataStore._ILocIndexerFrame

    def __init_subclass__(cls) -> None:
        super(DataStore, cls).__init_subclass__()
        for name, col in cls.columns().items():
            col._name = name
            setattr(cls, name, col)

    def __init__(self, **column_data: dict[str, Column]) -> None:
        if len(column_data) > 0:
            column_data = {str(name): col for name, col in column_data.items()}
            self.data_frame = DataFrame(column_data)
            self._validate_data_frame()
            self._attach_columns()
        else:
            self.data_frame = DataFrame()

        self.loc = DataStore._LocIndexerFrame(self)
        self.iloc = DataStore._ILocIndexerFrame(self)

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
            else:
                data = Column(self.data_frame[name])
                if data.dtype != col.dtype:
                    try:
                        data = col(data)
                    except:
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

    def __str__(self) -> str:
        """TODO!"""
        if len(self.data_frame) == 0:
            return f"Empty {self.__class__.__name__}"
        else:
            return f"{self.__class__.__name__}n{self.data_frame}"

    def __repr__(self) -> str:
        """TODO!"""
        return str(self)

    @classmethod
    def builder(
        cls: Type[T], **column_data: dict[str, Column]
    ) -> DataStore._Builder[T]:
        return DataStore._Builder[cls](cls, **column_data)

    class _Builder(Generic[T]):
        _store_class: Type[T]
        _column_data: dict[str, Column]

        def __init__(
            self, store_class: Type[T], **column_data: dict[str, Column]
        ) -> None:
            self._store_class = store_class
            self._column_data = column_data

        def append(self, column_name: str, column_data) -> DataStore._Builder[T]:
            self._column_data[str(column_name)] = column_data
            return self

        def __setitem__(self, column_name: str, column_data) -> None:
            self._column_data[str(column_name)] = column_data

        def build(self) -> T:
            return self._store_class(**self._column_data)

    class _ILocIndexerFrame(Generic[T]):
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[int, slice]) -> T:
            return self._data_store._new_data_copy(
                self._data_store.data_frame.iloc[item]
            )

    class _LocIndexerFrame:
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[Any, slice]) -> T:
            return self._data_store._new_data_copy(
                self._data_store.data_frame.loc[item]
            )