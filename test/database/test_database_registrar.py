from test.helpers.mock_adapter import MockAdapter

from hamcrest import assert_that, equal_to, is_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.data_store.data_type import String, Int64, Boolean
from src.database.data_token import DataToken
from src.database.database_registrar import DatabaseRegistrar
from src.database.reference_tables import PROTECTED_GROUP, StoreDefinition
import pickle

from test.helpers.example_store import ExampleStore, RAW_GROUP


class TestDatabaseRegistrar:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()
        self.registrar = DatabaseRegistrar(self.adapter)

    def test_reference_table_setup(self) -> None:
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
        ]
        assert_that(self.registrar.list_groups(), equal_to([]))
        assert_that(
            self.registrar.list_group_tables(PROTECTED_GROUP),
            equal_to(protected_tokens),
        )
        assert_that(
            self.registrar.list_tables(),
            equal_to([]),
        )
        assert_that(self.registrar.has_group(PROTECTED_GROUP), is_(True))
        for token in protected_tokens:
            assert_that(self.registrar.has_table(token), is_(True))
            assert_that(self.registrar.is_table_protected(token), is_(True))

    def test_has_group(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(self.registrar.has_group(PROTECTED_GROUP), is_(True))
        assert_that(self.registrar.has_group(RAW_GROUP), is_(True))
        assert_that(self.registrar.has_group("NOT_GROUP"), is_(False))

    def test_list_group(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(self.registrar.list_groups(), equal_to([RAW_GROUP]))

    def test_table_protected(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(
            self.registrar.is_table_protected(ExampleStore.data_token), is_(False)
        )
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
        ]
        for token in protected_tokens:
            assert_that(self.registrar.is_table_protected(token), is_(True))

    def test_group_contains_protected(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(
            self.registrar.group_contains_protected_tables(
                ExampleStore.data_token.data_group
            ),
            is_(False),
        )
        assert_that(
            self.registrar.group_contains_protected_tables(PROTECTED_GROUP),
            is_(True),
        )

    def test_has_table(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        all_tokens = [
            ExampleStore.data_token,
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
        ]
        for token in all_tokens:
            assert_that(self.registrar.has_table(token), is_(True))
        assert_that(
            self.registrar.has_table(DataToken("table_reference", "NOT_GROUP")),
            is_(False),
        )
        assert_that(
            self.registrar.has_table(DataToken("not_table", "public")), is_(False)
        )

    def test_list_table(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        all_tokens = [ExampleStore.data_token]
        assert_that(self.registrar.list_tables(), equal_to(all_tokens))

    def test_list_group_tables(self) -> None:
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
            DataToken("ExampleStore_v1_definition", PROTECTED_GROUP),
        ]

        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(
            self.registrar.list_group_tables(RAW_GROUP),
            equal_to([ExampleStore.data_token]),
        )
        assert_that(
            self.registrar.list_group_tables(PROTECTED_GROUP),
            equal_to(protected_tokens),
        )

    def test_has_store_type(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        assert_that(self.registrar.has_store_type("ExampleStore", 1), is_(True))
        assert_that(self.registrar.has_store_type("TableReference", 1), is_(True))
        assert_that(self.registrar.has_store_type("StoreReference", 1), is_(True))
        assert_that(self.registrar.has_store_type("StoreDefinition", 1), is_(True))
        assert_that(
            self.registrar.has_store_type("StoreDefinition_v1_definition", 1),
            is_(False),
        )

    def test_store_type_versions(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        actual = self.registrar.store_type_versions()
        expected = {
            "ExampleStore": {1},
            "TableReference": {1},
            "StoreReference": {1},
            "StoreDefinition": {1},
        }
        assert_that(actual, equal_to(expected))

    def test_definition_reference_version(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        store_name_def_tokens = {
            "ExampleStore": DataToken("ExampleStore_v1_definition", PROTECTED_GROUP),
            "TableReference": DataToken(
                "TableReference_v1_definition", PROTECTED_GROUP
            ),
            "StoreReference": DataToken(
                "StoreReference_v1_definition", PROTECTED_GROUP
            ),
            "StoreDefinition": DataToken(
                "StoreDefinition_v1_definition", PROTECTED_GROUP
            ),
        }
        for store_name, def_token in store_name_def_tokens.items():
            store_token, def_version = self.registrar.definition_reference_version(
                store_name, 1
            )
            assert_that(store_token, equal_to(def_token))
            assert_that(def_version, equal_to(1))

    def test_store_definition(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        actual = self.registrar.store_definition(
            DataToken("ExampleStore_v1_definition", PROTECTED_GROUP), 1
        )
        expected = StoreDefinition(
            column_name=["a", "b", "c"],
            column_type=[
                pickle.dumps(String),
                pickle.dumps(Int64),
                pickle.dumps(Boolean),
            ],
        )
        assert_that(actual.equals(expected), is_(True))

    def test_store_class(self) -> None:
        self.registrar.create_table(ExampleStore.data_token, ExampleStore)
        store_class = self.registrar.store_class(ExampleStore.data_token)
        actual = store_class(a=1, b=2, c=3)
        expected = ExampleStore(a=1, b=2, c=3)
        assert_that(actual.equals(expected), is_(True))
        assert_that(store_class.__name__, equal_to("ExampleStore"))
        assert_that(actual.test_method(), equal_to("test_result"))
        
        for actual, expected in zip(store_class.columns, ExampleStore.columns):
            assert_that(actual._name, equal_to(expected._name))
            assert_that(actual.dtype, equal_to(expected.dtype))

    def test_drop_store_type(self) -> None:
        fail("Not Implemented")
