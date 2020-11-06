from __future__ import annotations

from typing import Any, cast, Generic, get_type_hints, Type, TypeVar, Union

from numpy import ndarray

from src.data_store.column import Column, ColumnAlias
from src.data_store.data_backend import DataBackend
from src.data_store.pandas_backend import PandasBackend

T = TypeVar("T", bound="DataStore")


class DataStore:
    _data_backend: DataBackend
    index: ndarray
    loc: DataStore._LocIndexer
    iloc: DataStore._ILocIndexer

    def __init_subclass__(cls) -> None:
        super(DataStore, cls).__init_subclass__()
        for name, col in cls.columns().items():
            col._name = name
            setattr(cls, name, col)

    def __init__(self, **column_data: dict[str, Column]) -> None:
        if len(column_data) > 0:
            column_data = {str(name): col for name, col in column_data.items()}
            self._data_backend = PandasBackend(column_data)
            self._validate_data_frame()
            self._attach_columns()
        else:
            self._data_backend = PandasBackend()

        self.index = self._data_backend.index()
        self.loc = DataStore._LocIndexer(self)
        self.iloc = DataStore._ILocIndexer(self)

    @classmethod
    def columns(cls) -> dict[str, ColumnAlias]:
        variables = get_type_hints(cls)
        columns: dict[str, Column] = {}
        missing_types = []
        for name, col in variables.items():
            if col is Column or type(col) is Column:
                missing_types.append(name)
            elif col is ColumnAlias or type(col) is ColumnAlias:
                columns[name] = col
        if len(missing_types) > 0:
            raise TypeError(
                f"Failed to find column types for the following columns: {missing_types}"
            )
        return columns

    def active_columns(self) -> dict[str, ColumnAlias]:
        columns = self.columns()
        backend_columns = self._data_backend.columns() & columns.keys()
        active_columns = {}
        for col in backend_columns:
            active_columns[col] = columns[col]
        return active_columns

    def _validate_data_frame(self) -> None:
        columns = self.active_columns()

        invalid_types = []
        for name, col in columns.items():
            data = Column(self._data_backend[name])
            if data.dtype != col.dtype:
                try:
                    self._data_backend[name] = col(data)
                except:
                    invalid_types.append(name)

        if len(invalid_types) != 0:
            raise TypeError(f"Invalid types provided for: {invalid_types}")

    def _attach_columns(self) -> None:
        columns = self.columns()
        active_columns = self.active_columns()
        for col in columns:
            if col in active_columns:
                setattr(self, col, self._data_backend[col])
            else:
                setattr(self, col, None)

    @classmethod
    def _new_data_copy(cls: type[T], data_backend: DataBackend) -> T:
        instance: T = cls()
        instance._data_backend = data_backend
        instance.index = data_backend.index()
        instance.loc = DataStore._LocIndexer(instance)
        instance.iloc = DataStore._ILocIndexer(instance)
        instance._attach_columns()
        return instance

    def __contains__(self, key):
        return key in self.columns()

    def __str__(self) -> str:
        if len(self._data_backend) == 0:
            return f"Empty {self.__class__.__name__}"
        else:
            return f"{self.__class__.__name__}{self._data_backend}"

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        oc = cast(DataStore, other)
        return self._data_backend == oc._data_backend

    def __len__(self):
        return len(self._data_backend)

    def __iter__(self):
        return iter(self._data_backend)

    def __getitem__(self, item: str) -> Column:
        if item not in self.columns():
            raise ValueError(
                f"Could not match '{item}' to {self.__class__.__name__} column"
            )
        elif item not in self.active_columns():
            return None
        else:
            return self.columns()[item](self._data_backend[item])

    def __getattr__(self, name: str) -> Any:
        raise ValueError(
            f"Could not match '{name}' to {self.__class__.__name__} column"
        )

    def reset_index(self: T, drop: bool = True) -> T:
        return self._new_data_copy(self._data_backend.reset_index(drop=drop))

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

    class _ILocIndexer(Generic[T]):
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[int, list, slice]) -> T:
            return self._data_store._new_data_copy(
                self._data_store._data_backend.iloc()[item]
            )

    class _LocIndexer:
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[Any, list, slice]) -> T:
            return self._data_store._new_data_copy(
                self._data_store._data_backend.loc()[item]
            )
