from __future__ import annotations

from io import UnsupportedOperation
from typing import (
    Any,
    cast,
    ClassVar,
    Generator,
    Generic,
    get_type_hints,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

from pandas import DataFrame, Index, Series

from src.data_backend.data_backend import DataBackend
from src.data_backend.pandas_backend import PandasBackend
from src.data_store.column import Column
from src.data_store.column_alias import ColumnAlias
from src.data_store.query_type import QueryType
from src.database.data_token import DataToken

T = TypeVar("T", bound="DataStore")


class DataStore:
    registed_stores: ClassVar[dict[str, Type[T]]] = {}

    version: ClassVar[int]
    columns: ClassVar[list[ColumnAlias]]

    _data_backend: DataBackend
    columns: list[ColumnAlias]
    index: Index
    _loc: DataStore._LocIndexer[T]
    _iloc: DataStore._ILocIndexer[T]

    def __init_subclass__(cls: Type[T], version: int = 1) -> None:
        super(DataStore, cls).__init_subclass__()
        cls.version = version
        cls.columns = []
        for name, col in cls._parse_columns().items():
            cls.columns.append(col)
            col._name = name
            setattr(cls, name, col)
        DataStore.registed_stores[cls.__name__] = cls

    def __init__(
        self: T, index: Optional[list] = None, **column_data: dict[str, Column]
    ) -> None:
        if len(column_data) > 0:
            column_data = {str(name): col for name, col in column_data.items()}
            self._data_backend = PandasBackend(column_data, index=index)
            self._validate_data_frame()
            self._attach_columns()
        else:
            self._data_backend = PandasBackend(index=index)

        self._compile()

    def _compile(self: T) -> None:
        self._all_columns = self._parse_columns()
        self._active_columns = self._parse_active_columns()
        self.columns = list(self._active_columns.values())
        self.index = self._data_backend.index
        self._loc = DataStore._LocIndexer[T](self)
        self._iloc = DataStore._ILocIndexer[T](self)

    @classmethod
    def link(
        cls: Type[T], database: D, data_token: DataToken, read_only: bool = True
    ) -> T:
        return cls.from_backend(database.backend(data_token, read_only))

    @classmethod
    def from_rows(
        cls: Type[T], data_rows: list[tuple], columns: Optional[list[str]] = None
    ) -> T:
        if columns is None:
            columns = cls._parse_columns().keys()
        else:
            columns = [str(col) for col in columns]
        data = DataFrame.from_records(data_rows, columns=columns)
        return cls.from_backend(PandasBackend(data))

    @classmethod
    def from_pandas(cls: Type[T], data: Union[Series, DataFrame]) -> T:
        return cls.from_backend(PandasBackend(data))

    def to_pandas(self: T) -> Union[Series, DataFrame]:
        return self._data_backend.to_pandas()

    @classmethod
    def from_backend(cls: Type[T], data_backend: DataBackend) -> T:
        instance = cls()
        instance._data_backend = data_backend
        instance._validate_data_frame()
        instance._attach_columns()
        instance._compile()
        return instance

    @property
    def loc(self: T) -> DataStore._LocIndexer[T]:
        return self._loc

    @property
    def iloc(self: T) -> DataStore._ILocIndexer[T]:
        return self._iloc

    def is_row(self: T) -> bool:
        return self._data_backend.is_row()

    def to_table(self: T) -> bool:
        return self.from_backend(self._data_backend.to_table())

    def to_row(self: T) -> bool:
        return self.from_backend(self._data_backend.to_row())

    @classmethod
    def _parse_columns(cls) -> dict[str, ColumnAlias]:
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

    def _parse_active_columns(self: T) -> dict[str, ColumnAlias]:
        columns = self._parse_columns()
        backend_columns = self._data_backend.columns
        unmatched_columns = set(backend_columns) - columns.keys()
        if len(unmatched_columns) > 0:
            raise KeyError(
                f"Data backend contains columns which are not supported by {self.__class__.__name__}"
            )

        active_columns = {}
        for col in backend_columns:
            active_columns[col] = columns[col]
        return active_columns

    def _validate_data_frame(self: T) -> None:
        columns = self._parse_active_columns()

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

    def _attach_columns(self: T) -> None:
        columns = self._parse_columns()
        active_columns = self._parse_active_columns()
        for col in columns:
            if col in active_columns:
                setattr(self, col, self._data_backend[col])
            else:
                setattr(self, col, None)

    def __contains__(self: T, key):
        return str(key) in self._all_columns

    def __str__(self: T) -> str:
        if len(self._data_backend) == 0:
            return f"Empty {self.__class__.__name__}"
        else:
            return f"{self.__class__.__name__}{self._data_backend}"

    def __repr__(self: T) -> str:
        return str(self)

    def __eq__(self: T, other):
        if issubclass(type(other), DataStore):
            return False
        oc = cast(DataStore, other)
        return (
            other.__class__.__name__ == self.__class__.__name__
            and self._data_backend == oc._data_backend
        )

    def equals(self: T, other):
        if not issubclass(type(other), DataStore):
            return False
        oc = cast(DataStore, other)
        return (
            other.__class__.__name__ == self.__class__.__name__
            and self._data_backend.equals(oc._data_backend)
        )

    def __len__(self: T):
        return len(self._data_backend)

    def __iter__(self: T):
        for column in self._data_backend:
            yield self._active_columns[column]

    def iterrows(self: T) -> Generator[tuple[int, T], None, None]:
        for i, row in self._data_backend.iterrows():
            yield (i, self.from_backend(row))

    def itertuples(self: T) -> Generator[tuple]:
        return self._data_backend.itertuples()

    def _get_column(self: T, item: str) -> T:
        if item not in self._all_columns:
            raise ValueError(
                f"Could not match '{item}' to {self.__class__.__name__} column"
            )
        elif item not in self._active_columns:
            return None
        else:
            return self._all_columns[item](self._data_backend[item])

    def _get_columns(self: T, columns: list[str]) -> T:
        missing_columns = self._active_columns.keys() - set(columns)
        unused_columns = set(columns) - self._active_columns.keys()
        errors = []
        if len(missing_columns) > 0:
            errors.append(f"Data Columns Missing: {missing_columns}")
        if len(unused_columns) > 0:
            errors.append(f"Unused Columns: {unused_columns}")
        if len(errors) > 0:
            raise ValueError(
                "Could not query column data because:\n" + "\n".join(errors)
            )

        return self.from_backend(self._data_backend[columns])

    def _compiled_query(self: T, query_type: QueryType) -> T:
        return self.from_backend(self._data_backend.query(query_type))

    def __getitem__(
        self: T, item: Union[str, list[str], Column[bool], QueryType]
    ) -> Union[Column, T]:
        if type(item) is str or type(item) is ColumnAlias:
            return self._get_column(str(item))
        elif isinstance(item, Iterable) and (
            type(next(iter(item))) is str or type(next(iter(item))) is ColumnAlias
        ):
            return self._get_columns([str(value for value in item)])
        elif type(item) is QueryType:
            return self._compiled_query(item)
        else:
            return self.from_backend(self._data_backend[item])

    def __getattr__(self: T, name: str) -> Any:
        raise ValueError(
            f"Could not match '{name}' to {self.__class__.__name__} column"
        )

    def set_index(self: T, column: Union[str, Iterable]) -> T:
        return self.from_backend(self._data_backend.set_index(column))

    def reset_index(self: T, drop: bool = False) -> T:
        return self.from_backend(self._data_backend.reset_index(drop=drop))

    @classmethod
    def concat(cls: T, all_data_stores: list[T], ignore_index: bool = False) -> T:
        all_backends = [store._data_backend for store in all_data_stores]
        return cls.from_backend(
            cls._data_backend.concat(all_backends, ignore_index=ignore_index)
        )

    @classmethod
    def builder(cls: Type[T]) -> DataStore._Builder[T]:
        return DataStore._Builder[cls](cls)

    class _Builder(Generic[T]):
        _store_class: Type[T]
        _column_data: dict[str, Column]
        _row_data: dict[str, list]

        def __init__(self, store_class: Type[T]) -> None:
            self._store_class = store_class
            self._column_data = {}
            self._row_data = {}

        def append_column(
            self, column_name: str, column_data: Column
        ) -> DataStore._Builder[T]:
            if len(self._row_data) > 0:
                raise UnsupportedOperation("Cannot insert column when row data present")
            self._column_data[str(column_name)] = column_data
            return self

        def __setitem__(self, column_name: str, column_data) -> None:
            self._column_data[str(column_name)] = column_data

        def append_row(self, **row_data: any) -> DataStore._Builder[T]:
            if len(self._column_data) > 0:
                raise UnsupportedOperation(
                    "Cannot insert row data when column data present"
                )
            for key, value in row_data.items():
                if key not in self._row_data:
                    self._row_data[key] = []
                self._row_data[key].append(value)
            return self

        def build(self) -> T:
            if len(self._column_data) > 0:
                return self._store_class(**self._column_data)
            else:
                return self._store_class(**self._row_data)

    class _ILocIndexer(Generic[T]):
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[int, list, slice]) -> T:
            return self._data_store.from_backend(
                self._data_store._data_backend.iloc[item]
            )

    class _LocIndexer(Generic[T]):
        _data_store: T

        def __init__(self, data_store: T) -> None:
            self._data_store = data_store

        def __getitem__(self, item: Union[Any, list, slice]) -> T:
            return self._data_store.from_backend(
                self._data_store._data_backend.loc[item]
            )


from src.database.database import Database

D = TypeVar("D", bound="Database")
