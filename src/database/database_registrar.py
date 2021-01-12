from __future__ import annotations

from io import UnsupportedOperation
from typing import Optional, Type, TypeVar

from src.data_store.query_type import Query
from src.database.adapter.database_adapter import DatabaseAdapter
from src.database.data_token import DataToken

from .db_exceptions import DatabaseCorruptionError, MissingGroupError, MissingTableError
from .reference_tables import (
    PROTECTED_GROUP,
    StoreDefinition,
    StoreReference,
    TableReference,
)


class DatabaseRegistrar:
    _db_adapter: DatabaseAdapter

    def __init__(self, db_adapter: DatabaseAdapter) -> None:
        self._db_adapter = db_adapter
        # TODO: Added migration step between reference table versions
        if not self._has_reference_tables():
            print("No protected reference tables found, rebuilding registrar")
            self._setup_reference_tables()
        self._validate_reference_tables()

    def _has_reference_tables(self):
        return self._db_adapter.has_table(
            TableReference.data_token
        ) and self._db_adapter.has_table(StoreReference.data_token)

    def _setup_reference_tables(self):
        if not self._db_adapter.has_group(PROTECTED_GROUP):
            self._db_adapter.create_group(PROTECTED_GROUP)
        self._db_adapter.create_table(TableReference.data_token, TableReference)
        self._db_adapter.create_table(StoreReference.data_token, StoreReference)
        self._register_table(TableReference.data_token, TableReference, protected=True)
        self._register_table(StoreReference.data_token, StoreReference, protected=True)
        self._register_store_class(StoreDefinition)
        self._register_store_class(TableReference)
        self._register_store_class(StoreReference)

    def _validate_reference_tables(self):
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        if not self._db_adapter.has_table(StoreReference.data_token):
            raise DatabaseCorruptionError("StoreReference table is missing")

        exceptions = []
        error_messages = []
        for data_token in self.list_tables():
            try:
                if not self._db_adapter.has_group(data_token.data_group):
                    raise MissingGroupError(data_token.data_group)
                if not self._db_adapter.has_table(data_token):
                    raise MissingTableError(data_token)
                store_type, store_version = self._table_store_type_version(data_token)
                token_version = self._definition_reference_version(
                    store_type, store_version
                )
                store_definition = self._store_definition(*token_version)
                if len(store_definition) == 0:
                    raise DatabaseCorruptionError(
                        f"{data_token} data store type reference empty"
                    )

                type_class = store_definition._store_class(store_type, store_version)
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

    def create_table(
        self: DatabaseRegistrar,
        data_token: DataToken,
        store_class: Type[T],
        protected: bool = False,
    ) -> None:
        if not self.has_group(data_token.data_group):
            self._db_adapter.create_group(data_token.data_group)
        self._db_adapter.create_table(data_token, store_class)
        self._register_table(data_token, store_class, protected)
        self._register_store_class(store_class)

    def drop_table(self: DatabaseRegistrar, data_token: DataToken) -> None:
        if not self.has_table(data_token):
            raise MissingTableError(data_token)
        if self._is_table_protected(data_token):
            raise UnsupportedOperation("Cannot delete from protected data group")
        self._db_adapter.drop_table(data_token)
        store_type, store_version = self._table_store_type_version(data_token)

        store_type_version_count = sum(
            (TableReference.store_type == store_type)
            & (TableReference.store_version == store_version)
        )
        version_count = self._db_adapter.query(
            TableReference.data_token, store_type_version_count
        )
        if version_count == 1:
            self._drop_store_type(store_type, store_version)

        criteria = (TableReference.table_name == data_token.table_name) & (
            TableReference.data_group == data_token.data_group
        )
        self._db_adapter.delete(TableReference.data_token, criteria)

        if len(self.list_group_tables(data_token.data_group)) == 0:
            self._db_adapter.drop_group(data_token.data_group)

    def drop_group(self: DatabaseRegistrar, data_group: str) -> None:
        if not self.has_group(data_group):
            raise MissingGroupError(data_group)
        if data_group == PROTECTED_GROUP:
            raise UnsupportedOperation("Cannot delete protected data group")
        if self._group_contains_protected_tables(data_group):
            raise UnsupportedOperation(
                "Cannot delete group that contains protected table"
            )
        for token in self.list_group_tables(data_group):
            self.drop_table(token)

    def copy_table(
        self: DatabaseRegistrar,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        raise NotImplementedError()

    def move_table(
        self: DatabaseRegistrar,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        raise NotImplementedError()

    def copy_group(
        self: DatabaseRegistrar,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        raise NotImplementedError()

    def move_group(
        self: DatabaseRegistrar,
        source_data_token: DataToken,
        target_data_token: DataToken,
    ) -> None:
        raise NotImplementedError()

    def _is_table_registered(self, data_token: DataToken) -> bool:
        if not self._db_adapter.has_table(data_token):
            return False
        else:
            return data_token in self._table_references().data_tokens()

    def has_table(self, data_token: DataToken) -> bool:
        token_set = set(self._table_references().data_tokens())
        registrar_result = data_token in token_set
        db_result = self._db_adapter.has_table(data_token)
        if registrar_result != db_result:
            raise DatabaseCorruptionError(
                "Table existence disagreement between"
                + f"registrar[{registrar_result}] vs database[{db_result}] for data token '{data_token}'"
            )
        else:
            return registrar_result

    def _table_references(self, criteria: Optional[Query] = None) -> TableReference:
        table_rows = self._db_adapter.query(
            TableReference.data_token, query_type=criteria
        )
        return TableReference.from_rows(table_rows)

    def _store_references(self, criteria: Optional[Query] = None) -> StoreReference:
        table_rows = self._db_adapter.query(
            StoreReference.data_token, query_type=criteria
        )
        return StoreReference.from_rows(table_rows)

    def list_tables(self) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            criteria = TableReference.protected == False
            return self._table_references(criteria).data_tokens()

    def has_group(self, data_group: str) -> bool:
        group_set = set(self._table_references().data_group.values)
        registrar_result = data_group in group_set
        db_result = self._db_adapter.has_group(data_group)
        if registrar_result != db_result:
            raise DatabaseCorruptionError(
                "Group existence disagreement between"
                + f" registrar[{registrar_result}] vs database[{db_result}] for group '{data_group}'"
            )
        else:
            return registrar_result

    def list_groups(self) -> set[str]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            criteria = TableReference.protected == False
            table_references = self._table_references(criteria)
            return table_references.data_group.tolist()

    def list_group_tables(self, data_group: str) -> list[DataToken]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            criteria = TableReference.data_group == data_group
            table_references = self._table_references(criteria)
            return table_references.data_tokens()

    def _register_table(
        self, data_token: DataToken, store_class: Type[T], protected: bool = False
    ) -> None:
        if not self._is_table_registered(data_token):
            reference = TableReference.create_row(data_token, store_class, protected)
            self._db_adapter.insert(
                TableReference.data_token, reference, ignore_index=True
            )

    def _register_store_class(self, store_class: Type[T]) -> None:
        if not self._has_store_type(store_class.__name__, store_class.version):
            reference = StoreReference.create_row(store_class)
            self._db_adapter.insert(
                StoreReference.data_token, reference, ignore_index=True
            )
            reference_token = StoreDefinition.data_token(store_class)
            self.create_table(reference_token, StoreDefinition, protected=True)
            self._db_adapter.insert(
                reference_token, StoreDefinition.from_type(store_class)
            )

    def _table_store_type_version(self, data_token: DataToken) -> tuple[str, int]:
        if not self._db_adapter.has_table(TableReference.data_token):
            raise DatabaseCorruptionError("TableReference table is missing")
        else:
            columns = [
                str(TableReference.store_type),
                str(TableReference.store_version),
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

    def _group_contains_protected_tables(self, data_group: str) -> bool:
        if not self.has_group(data_group):
            raise MissingGroupError(data_group)
        else:
            criteria = TableReference.data_group == data_group
            table_rows = self._db_adapter.query(
                TableReference.data_token, criteria, [str(TableReference.protected)]
            )
            return any([item[0] for item in table_rows])

    def _has_store_type(self, store_name: str, store_version: int):
        type_versions = self._store_references().store_versions()
        return (
            store_name in type_versions and store_version in type_versions[store_name]
        )

    def _definition_reference_version(
        self, store_type: str, store_version: int
    ) -> tuple[DataToken, int]:
        if not self._db_adapter.has_table(StoreReference.data_token):
            raise DatabaseCorruptionError("StoreReference table is missing")
        else:
            columns = [
                str(StoreReference.definition_reference),
                str(StoreReference.definition_version),
            ]
            table_rows = self._db_adapter.query(
                StoreReference.data_token,
                (StoreReference.store_type == store_type)
                & (StoreReference.store_version == store_version),
                columns,
            )
            if len(table_rows) == 0:
                raise DatabaseCorruptionError(
                    f"Data store definition {store_type} V{store_version} missing from StoreReference"
                )
            if len(table_rows) > 1:
                raise DatabaseCorruptionError(
                    f"Duplicate data store references found for {store_type} V{store_version}"
                )
            return (
                DataToken(table_rows[0][0], PROTECTED_GROUP),
                table_rows[0][1],
            )

    def _store_definition(
        self, reference_token: DataToken, definition_version: int
    ) -> StoreDefinition:
        if not self._db_adapter.has_table(reference_token):
            raise DatabaseCorruptionError(f"{reference_token} table is missing")
        else:
            if definition_version > 1:
                raise UnsupportedOperation(
                    f"Cannot deserialise data store type for version {definition_version}"
                )
            def_rows = self._db_adapter.query(reference_token)
            return StoreDefinition.from_rows(def_rows)

    def store_class(self, data_token: DataToken) -> Type[T]:
        store_type, store_version = self._table_store_type_version(data_token)
        reference_token, definition_version = self._definition_reference_version(
            store_type, store_version
        )
        store_definition = self._store_definition(reference_token, definition_version)
        return store_definition._store_class(store_type, store_version)

    def _drop_store_type(self, store_type: str, store_version: int) -> None:
        reference_token, _ = self._definition_reference_version(
            store_type, store_version
        )
        self._db_adapter.drop_table(reference_token)
        criteria = (TableReference.table_name == reference_token.table_name) & (
            TableReference.data_group == reference_token.data_group
        )
        self._db_adapter.delete(TableReference.data_token, criteria)
        criteria = (StoreReference.store_type == store_type) & (
            StoreReference.store_version == store_version
        )
        self._db_adapter.delete(StoreReference.data_token, criteria)


from src.data_store.data_store import DataStore

T = TypeVar("T", bound=DataStore)
