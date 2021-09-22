from __future__ import annotations

from io import UnsupportedOperation
from typing import (
    Any,
    cast,
    ClassVar,
    Generator,
    Generic,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

import numpy as np
from pandas import DataFrame, Series

from src.data_backend.data_backend import DataBackend
from src.data_backend.pandas_backend import PandasBackend
from src.data_store.column import Column
from src.data_store.column_alias import ColumnAlias
from src.data_store.data_type import Boolean, DataType, String, TypeAlias
from src.data_store.query import Query
from src.database.data_token import DataToken

from .index import Index
from .index_alias import IndexAlias
from .storable_type_factory import StorableTypeFactory

B = TypeVar("B", bound=DataBackend)
T = TypeVar("T", bound="DataStore")

class DataStore:
    # Class vars
    registered_stores: ClassVar[dict[str, Type[T]]] = {}

    type_factory: ClassVar[StorableTypeFactory]
    version: ClassVar[int]
    columns: ClassVar[list[ColumnAlias]]
    indices: ClassVar[list[IndexAlias]]

    # Instance vars
    columns: list[ColumnAlias]
    _data_backend: B
    loc: DataStore._LocIndexer[T]
    iloc: DataStore._ILocIndexer[T]
    index: Index

    def __init_subclass__(
        cls: Type[T], version: int = 1, register: bool = True
    ) -> None:
        super(DataStore, cls).__init_subclass__()
        if register and cls.__name__ in DataStore.registered_stores:
            raise TypeError(
                f"Duplicate DataStores found {cls} vs {DataStore.registered_stores[cls.__name__]}"
            )
        cls.type_factory = StorableTypeFactory(list(cls.__mro__), cls.__annotations__)
        cls.version = version
        cls.columns = []
        cls.indices = []
        for name, col in cls._parse_columns().items():
            cls.columns.append(col)
            setattr(cls, name, col)
        for name, index in cls._parse_indices().items():
            cls.indices.append(index)
            setattr(cls, name, index)
        if register:
            DataStore.registered_stores[cls.__name__] = cls

    def __init__(
        self: T, index: Optional[Iterable] = None, **column_data: dict[str, list]
    ) -> None:
        if len(column_data) > 0:
            column_data = {str(name): col for name, col in column_data.items()}
            self._data_backend = PandasBackend(column_data, index=index)
            self._validate_data_frame()
        else:
            self._data_backend = PandasBackend(index=index)
        self._compile()

    def _compile(self: T) -> None:
        self._attach_columns()
        self._attach_indices()
        self._all_columns = self._parse_columns()
        self._active_columns = self._parse_active_columns()
        self.columns = list(self._active_columns.values())
        self.index = self._data_backend.index
        self.loc = DataStore._LocIndexer[T](self)
        self.iloc = DataStore._ILocIndexer[T](self)

    @classmethod
    def link(
        cls: Type[T], database: D, data_token: DataToken, read_only: bool = True
    ) -> T:
        from src.data_backend.database_backend import DatabaseBackend

        return cls.from_backend(
            DatabaseBackend[T](cls, database, data_token, read_only=read_only), validate=False
        )

    @classmethod
    def from_rows(
        cls: Type[T], data_rows: list[tuple], columns: Optional[list[str]] = None
    ) -> T:
        if columns is None:
            columns = list(cls._parse_columns().keys())
        else:
            columns = [str(col) for col in columns]
        data = DataFrame.from_records(data_rows, columns=columns)
        return cls.from_backend(PandasBackend(data))

    @classmethod
    def from_pandas(cls: Type[T], data: Union[Series, DataFrame]) -> T:
        return cls.from_backend(PandasBackend(data))

    def to_pandas(self: T) -> DataFrame:
        return self._data_backend.to_pandas()

    def to_dict(self: T) -> DataFrame:
        return self._data_backend.to_dict()

    @property
    def values(self: T) -> np.array:
        return self._data_backend.values

    @property
    def dtypes(self: T) -> dict[str, DataType]:
        return self._data_backend.dtypes

    def is_link(self: T) -> bool:
        return self._data_backend.is_link()

    def link_token(self: T) -> Optional[DataToken]:
        return self._data_backend.link_token()

    def load(self: T) -> T:
        return self.from_backend(self._data_backend.load())

    @classmethod
    def from_backend(cls: Type[T], data_backend: B, validate: bool = True) -> T:
        instance = cls()
        instance._data_backend = data_backend
        if validate:
            instance._validate_data_frame()
        instance._compile()
        return instance

    @classmethod
    def _parse_columns(cls) -> dict[str, ColumnAlias]:
        return cls.type_factory.columns

    @classmethod
    def _parse_indices(cls) -> dict[str, IndexAlias]:
        return cls.type_factory.indices

    def _parse_active_columns(self: T) -> dict[str, ColumnAlias]:
        columns = self._parse_columns()
        backend_columns = [str(col) for col in self._data_backend.columns]
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
            if isinstance(col.dtype, TypeAlias):
                continue
            col_data = self._data_backend[name]
            data_dtype = Column.infer_dtype(name, col_data)
            # TODO: Run in batch
            if data_dtype != col.dtype:
                try:
                    cast_column = col(name, col_data)
                    self._data_backend[name] = cast_column._data_backend
                except:
                    invalid_types.append(name)

        if len(invalid_types) != 0:
            raise TypeError(f"Invalid types provided for: {invalid_types}")

    def _attach_columns(self: T) -> None:
        columns = self._parse_columns()
        active_columns = self._parse_active_columns()
        for name, col in columns.items():
            if name in active_columns:
                data = self._data_backend[name]
                setattr(self, name, col(name, data))
            else:
                setattr(self, name, None)

    def _attach_indices(self: T) -> None:
        indices = self._parse_indices()
        active_columns = self._parse_active_columns()
        for name, alias in indices.items():
            col_names = [col.name for col in alias.columns]
            has_columns = True
            for col in col_names:
                if col not in active_columns:
                    has_columns = False
                    break
            if not has_columns:
                setattr(self, name, None)
            else:
                data = self._data_backend.getitems(col_names)
                setattr(self, name, alias(name, data))

    def __contains__(self: T, key):
        return str(key) in self._all_columns

    def __str__(self: T) -> str:
        if len(self._data_backend) == 0:
            return f"Empty {self.__class__.__name__}"
        else:
            return f"{self.__class__.__name__}\n{self._data_backend}"

    def __repr__(self: T) -> str:
        return str(self)

    def equals(self: T, other) -> bool:
        if not issubclass(type(other), DataStore):
            return False
        oc = cast(DataStore, other)
        return (
            other.__class__.__name__ == self.__class__.__name__
            and self._data_backend.equals(oc._data_backend)
        )

    def _get_external_backend(self: T, other: Any) -> None:
        if issubclass(type(other), DataStore):
            if not self.__class__.__name__ == other.__class__.__name__:
                raise UnsupportedOperation(
                    "Cannot compare different DataStore types: "
                    + f"{self.__class__.__name__} vs {other.__class__.__name__}"
                )
            return cast(DataStore, other)._data_backend
        else:
            return other

    def __eq__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend == other

    def __ne__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend != other

    def __gt__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend > other

    def __ge__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend >= other

    def __lt__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend < other

    def __le__(self: T, other: Any) -> Any:
        other = self._get_external_backend(other)
        return self._data_backend <= other

    def __len__(self: T) -> int:
        return len(self._data_backend)

    def __iter__(self: T) -> Generator[ColumnAlias, None, None]:
        for column in self._data_backend:
            yield self._active_columns[column]

    def iterrows(self: T) -> Generator[tuple[int, T], None, None]:
        for i, row in self._data_backend.iterrows():
            row.index.name = self._data_backend.index.name
            yield (i, self.from_backend(row))

    def itertuples(self: T, ignore_index: bool = False) -> Generator[tuple]:
        return self._data_backend.itertuples(ignore_index=ignore_index)

    def _get_column(self: T, item: str) -> T:
        if item not in self._all_columns:
            raise ValueError(
                f"Could not match '{item}' to {self.__class__.__name__} column"
            )
        elif item not in self._active_columns:
            return None
        else:
            return self._all_columns[item](item, self._data_backend[item])

    def _get_columns(self: T, columns: list[str]) -> T:
        unused_columns = set(columns) - self._all_columns.keys()
        if len(unused_columns) > 0:
            raise ValueError(
                f"The following columns do not exist in {self.__class__.__name__}: {unused_columns}"
            )
        return self._data_backend.getitems(columns)

    def _get_mask(self: T, mask: list[bool]) -> T:
        return self._data_backend.getmask(mask)

    def query(self: T, query: Optional[Query] = None) -> T:
        return self.from_backend(self._data_backend.query(query))

    def __getitem__(
        self: T, item: Union[ColumnAlias, list[ColumnAlias], list[bool], Query]
    ) -> Union[Column, T]:
        if issubclass(type(item), Query):
            result = self._data_backend.query(item)
        elif item == "index":
            return self._data_backend.index
        elif type(item) is str or type(item) is ColumnAlias:
            result = self._get_column(str(item))
        elif isinstance(item, Iterable):
            sample = item
            while type(sample) is not str and isinstance(sample, Iterable):
                sample = next(iter(sample))
            value_type = DataType(type(sample))
            if value_type is String or value_type is ColumnAlias:
                result = self._get_columns([str(value) for value in item])
            elif value_type is Boolean:
                result = self._get_mask(item)
            else:
                raise RuntimeError(f"Unknown get item request: {item}")
        else:
            raise RuntimeError(f"Unknown get item request: {item}")

        if issubclass(type(result), DataBackend):
            result = self.from_backend(result)
        return result

    def __getattr__(self: T, name: str) -> Any:
        if name[0] != "_":
            raise AttributeError(
                f"Could not match '{name}' to {self.__class__.__name__} column"
            )

    def set_index(self: T, index: Union[Index, IndexAlias]) -> T:
        col_names = [str(col) for col in index.columns]
        return self.from_backend(self._data_backend.set_index(col_names))

    def reset_index(self: T, drop: bool = False) -> T:
        return self.from_backend(self._data_backend.reset_index(drop=drop))

    def append(self: T, new_store: T, ignore_index: bool = False) -> T:
        return self.from_backend(
            self._data_backend.append(
                new_store._data_backend, ignore_index=ignore_index
            )
        )

    def drop(self: T, indices: list[int]) -> T:
        return self.from_backend(
            self._data_backend.drop_indices(indices)
        )

    @classmethod
    def concat(cls: T, all_data_stores: list[T], ignore_index: bool = False) -> T:
        all_match = all([isinstance(item, cls) for item in all_data_stores])
        if not all_match:
            raise ValueError("All data stores must be same type for concat")

        backend_sample: B = all_data_stores[0]._data_backend
        all_backends = [store._data_backend for store in all_data_stores]
        return cls.from_backend(
            backend_sample.concat(all_backends, ignore_index=ignore_index)
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
