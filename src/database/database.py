from __future__ import annotations

from io import UnsupportedOperation
from src.data_store.column import Column
from types import TracebackType
from typing import Any, Generic, Optional, Type, TypeVar

from pandas import Index

from src.data_backend.database_backend import DatabaseBackend
from src.data_store.data_store import DataStore
from src.data_store.query_type import QueryType
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.data_token import DataToken

from .reference_tables import (
    DataStoreDefinition,
    DataStoreReference,
    PROTECTED_GROUP,
    TableReference,
)

T = TypeVar("T", bound=DataStore)


class DatabaseCorruptionError(IOError):
    def __init__(self, reason: str, *exception: Exception) -> None:
        super(DatabaseCorruptionError, self).__init__(
            f"Database in corrupted state: {reason}", *exception
        )


class MissingTableError(IOError):
    def __init__(self, data_token: DataToken) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find table for {data_token}"
        )


class MissingDataStoreTypeError(IOError):
    def __init__(self, store_type: str, store_version: int) -> None:
        super(MissingTableError, self).__init__(
            f"Failed to find DataStore type for {store_type}, version {store_version}"
        )


class Database:
    _db_adapter: DatabaseAdapter
    query: QueryAlias[T]

    _protected: set[DataToken]

    def __init__(self, database_adapter: DatabaseAdapter) -> None:
        self.query = Database.QueryAlias[T](self)
        self._db_adapter = database_adapter
        if not self._has_reference_tables():
            self._setup_reference_tables()
        self._validate_reference_tables()

    @staticmethod
    def backend(data_token: DataToken, read_only: bool = True) -> DatabaseBackend:
        return DatabaseBackend(data_token, read_only=read_only)

    def _has_reference_tables(self):
        return self._db_adapter.has_table(
            TableReference.data_token
        ) and self._db_adapter.has_table(DataStoreReference.data_token)

    def _setup_reference_tables(self):
        if not self._db_adapter.has_group(PROTECTED_GROUP):
            self._db_adapter.create_group(PROTECTED_GROUP)
        self._db_adapter.create_table(TableReference.data_token, TableReference)
        self._db_adapter.create_table(DataStoreReference.data_token, DataStoreReference)
        self._register_table(TableReference.data_token, TableReference, protected=True)
        self._register_table(
            DataStoreReference.data_token, DataStoreReference, protected=True
        )
        self._register_data_store_type(DataStoreDefinition)
        self._register_data_store_type(TableReference)
        self._register_data_store_type(DataStoreReference)

    def _validate_reference_tables(self):
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        if not self._db_adapter.has_table(DataStoreReference.data_token):
            raise DatabaseCorruptionError("DataStoreReference table is missing")

        exceptions = []
        error_messages = []
        for data_token in self.list_tables():
            try:
                store_type, store_version = self._table_store_type_version(data_token)
                token_version = self._definition_reference_version(
                    store_type, store_version
                )
                store_definition = self._data_store_definition(*token_version)
                if len(store_definition) == 0:
                    raise DatabaseCorruptionError(
                        f"{data_token} data store type reference empty"
                    )

                type_class = store_definition.store_class(store_type, store_version)
                if not issubclass(type_class, DataStore):
                    raise DatabaseCorruptionError(
                        f"Recieved invalid data store class for {data_token}"
                    )

            except Exception as e:
                exceptions.append(e)
                error_messages.append(f"{data_token}: {e}")

        if len(exceptions) > 0:
            error_message = (
                f"The following exception occured when validating the database state:\n"
                + "\n".join(error_messages)
            )
            raise DatabaseCorruptionError(error_message, *exceptions)

    def _is_table_registered(self, data_token: DataToken) -> bool:
        if not self._db_adapter.has_table(data_token):
            return False
        else:
            table_rows = self._db_adapter.query(TableReference.data_token)
            table_ref = TableReference.from_rows(table_rows)
            return data_token in table_ref.data_tokens()

    def _register_table(
        self, data_token: DataToken, data_store_type: Type[T], protected: bool = False
    ) -> None:
        if not self._is_table_registered(data_token):
            reference = TableReference.create_row(
                data_token, data_store_type, protected
            )
            self._db_adapter.insert(
                TableReference.data_token, reference, ignore_index=True
            )

    def _register_data_store_type(
        self, data_store_type: Type[T]
    ) -> None:
        if not self._has_data_store_type(
            data_store_type.__name__, data_store_type.version
        ):
            reference = DataStoreReference.create_row(data_store_type)
            self._db_adapter.insert(
                DataStoreReference.data_token, reference, ignore_index=True
            )
            reference_token = DataStoreDefinition.data_token(data_store_type)
            self._create_table(reference_token, data_store_type)
            self._db_adapter.insert(
                reference_token, DataStoreDefinition.from_type(data_store_type)
            )

    def _create_table(
        self, data_token: DataToken, data_store_type: Type[T], protected: bool = False
    ) -> None:
        if not self.has_group(data_token.data_group):
            self._db_adapter.create_group(data_token.data_group)
        self._db_adapter.create_table(data_token, data_store_type)
        self._register_table(data_token, data_store_type, protected)
        self._register_data_store_type(data_store_type)

    def _table_store_type_version(self, data_token: DataToken) -> tuple[str, int]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            columns = [
                str(TableReference.data_store_type),
                str(TableReference.data_store_version),
            ]
            table_rows = self._db_adapter.query(
                TableReference.data_token,
                (TableReference.table_name == data_token.table_name)
                & (TableReference.table_name == data_token.table_name),
                columns,
            )
            if len(table_rows) == 0:
                raise MissingTableError(data_token)
            if len(table_rows) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate table references found for {data_token}"
                )
            return table_rows[0]

    def _is_table_protected(self, data_token: DataToken) -> bool:
        if not self.has_table(data_token):
            raise MissingTableError(data_token)
        else:
            criteria = (TableReference.table_name == data_token.table_name) & (
                TableReference.data_group == data_token.data_group
            )
            table_rows = self._db_adapter.query(
                TableReference.data_token, criteria, [str(TableReference.protected)]
            )
            if len(table_rows) == 0:
                raise MissingTableError(data_token)
            if len(table_rows) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate table references found for {data_token}"
                )
            return table_rows[0][0]

    def _is_data_store_protected(self, store_name: str, store_version: int) -> bool:
        if not self._has_data_store_type(store_name, store_version):
            raise MissingDataStoreTypeError(store_name, store_version)
        else:
            query = (DataStoreReference.data_store_type == store_name) & (
                DataStoreReference.data_store_version == store_version
            )
            table_rows = self._db_adapter.query(
                DataStoreReference.data_token,
                query,
                [str(DataStoreReference.protected)],
            )
            if len(table_rows) == 0:
                raise DatabaseCorruptionError(
                    f"Data store definition {store_name} version {store_version} missing from DataStoreReference"
                )
            if len(table_rows) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate data store references found for {store_name} version {store_version}"
                )
            return table_rows[0][0]

    def has_table(self, data_token: DataToken) -> bool:
        return self._db_adapter.has_table(data_token)

    def list_tables(self) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            table_data = self.query[TableReference](TableReference.data_token)
            return TableReference.data_tokens(table_data)

    def has_group(self, data_group: str) -> bool:
        return self._db_adapter.has_group(data_group)

    def list_groups(self) -> set[str]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            data = self.query[TableReference](TableReference.data_token)
            return list(dict.fromkeys(data.data_group.tolist()))

    def list_group_tables(self, data_group: str) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            criteria = TableReference.data_group == data_group
            data = self.query[TableReference](TableReference.data_token, criteria)
            return data.data_tokens()

    def _has_data_store_type(self, store_name: str, store_version: int):
        type_versions = self._data_store_type_versions()
        return (
            store_name in type_versions and store_version in type_versions[store_name]
        )

    def _data_store_type_versions(self) -> dict[str, set[int]]:
        if not self._db_adapter.has_table(DataStoreReference.data_token):
            raise DatabaseCorruptionError("DataStoreReference table is missing")
        else:
            data = self._db_adapter.query(DataStoreReference.data_token)
            return DataStoreReference.data_store_versions(data)

    def _definition_reference_version(
        self, store_type: str, store_version: int
    ) -> tuple[DataToken, int]:
        if not self._db_adapter.has_table(DataStoreReference.data_token):
            raise DatabaseCorruptionError("DataStoreReference table is missing")
        else:
            columns = [
                str(DataStoreReference.definition_reference),
                str(DataStoreReference.definition_version),
            ]
            table_rows = self._db_adapter.query(
                DataStoreReference.data_token,
                (DataStoreReference.data_store_type == store_type)
                & (DataStoreReference.data_store_version == store_version),
                columns,
            )
            if len(table_rows) == 0:
                raise DatabaseCorruptionError(
                    f"Data store definition {store_type} V{store_version} missing from DataStoreReference"
                )
            if len(table_rows) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate data store references found for {store_type} V{store_version}"
                )
            return (
                DataToken(table_rows[0][0], PROTECTED_GROUP),
                table_rows[0][1],
            )

    def _data_store_definition(
        self, reference_token: DataToken, definition_version: int
    ) -> DataStoreDefinition:
        if not self._db_adapter.has_table(reference_token):
            raise DatabaseCorruptionError(f"{reference_token} table is missing")
        else:
            if definition_version > 1:
                raise UnsupportedOperation(
                    f"Cannot deserialise data store type for version {definition_version}"
                )
            def_rows = self._db_adapter.query(reference_token)
            return DataStoreDefinition.from_rows(def_rows)

    def _query(
        self: Database,
        data_token: DataToken,
        query_type: Optional[QueryType] = None,
        columns: Optional[list[str]] = None,
    ) -> T:
        if not self.has_table(data_token):
            raise MissingTableError(data_token)
        table_data = self._db_adapter.query(data_token, query_type, columns)
        store_type, store_version = self._table_store_type_version(data_token)
        reference_token, definition_version = self._definition_reference_version(
            store_type, store_version
        )
        store_definition = self._data_store_definition(
            reference_token, definition_version
        )
        store_class: Type[T] = store_definition.store_class(store_type, store_version)
        return store_class.from_rows(table_data)

    def insert(
        self: Database, data_token: DataToken, data_store: T, ignore_index: bool = False
    ) -> None:
        if not self.has_table(data_token):
            self._create_table(data_token, data_store.__class__)
        self._db_adapter.insert(data_token, data_store, ignore_index)

    def update(
        self: Database, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def upsert(
        self: Database, data_token: DataToken, data_store: T, *columns: str
    ) -> None:
        ...

    def drop_indices(self: Database, data_token: DataToken, indices: Index) -> None:
        ...

    def drop_table(self: Database, data_token: DataToken) -> None:
        ...

    def drop_group(self: Database, data_group: str) -> None:
        ...

    def copy_table(
        self: Database, source_data_token: DataToken, target_data_token: DataToken
    ) -> None:
        ...

    def __enter__(self: Database) -> Database:
        return self

    def __exit__(
        self: Database,
        type: Optional[Type[BaseException]] = None,
        value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        self._db_adapter.stop()

    class QueryAlias(Generic[T]):
        """Query Database

        Parameters
        ----------
        data_token : DataToken
            database table and group
        query_type : Optional[QueryType], optional
            to query the data, by default None
        columns : Optional[list[str]], optional
            specific columns expected as output, by default None

        Returns
        -------
        T
            The queried data in the expected DataStore class
        """

        _database: Database
        _data_store_type: Type[T]

        def __init__(
            self,
            data_base: Database,
            data_store_type: Type[T] = Any,
        ) -> None:
            self._database = data_base
            self._data_store_type = data_store_type

        def __getitem__(self, data_store_type: Type[T]) -> Database.QueryAlias[T]:
            return Database.QueryAlias[T](self._database, data_store_type)

        def __call__(
            self,
            data_token: DataToken,
            query_type: Optional[QueryType] = None,
            columns: Optional[list[str]] = None,
        ) -> T:
            """Query Database

            Parameters
            ----------
            data_token : DataToken
                database table and group
            query_type : Optional[QueryType], optional
                to query the data, by default None
            columns : Optional[list[str]], optional
                specific columns expected as output, by default None

            Returns
            -------
            T
                The queried data in the expected DataStore class
            """
            return self._database._query(data_token, query_type, columns)
