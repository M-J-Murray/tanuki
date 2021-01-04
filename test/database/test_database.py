from test.helpers.mock_adapter import MockAdapter

from hamcrest import assert_that, equal_to, is_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.data_token import DataToken
from src.database.database import Database
from src.database.reference_tables import (
    PROTECTED_GROUP,
)

RAW_GROUP = "raw"


class ExampleStore(DataStore):
    data_token = DataToken("test", RAW_GROUP)

    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestDatabase:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()
        self.database = Database(self.adapter)

    def test_reference_table_setup(self) -> None:
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinitionV1_definition", PROTECTED_GROUP),
            DataToken("TableReferenceV1_definition", PROTECTED_GROUP),
            DataToken("StoreReferenceV1_definition", PROTECTED_GROUP),
        ]
        assert_that(self.database.list_groups(), equal_to([]))
        assert_that(
            self.database.list_group_tables(PROTECTED_GROUP),
            equal_to(protected_tokens),
        )
        assert_that(
            self.database.list_tables(),
            equal_to([]),
        )
        assert_that(self.database.has_group(PROTECTED_GROUP), is_(True))
        for token in protected_tokens:
            assert_that(self.database.has_table(token), is_(True))
            assert_that(self.database._is_table_protected(token), is_(True))

    def test_has_group(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(self.database.has_group(PROTECTED_GROUP), is_(True))
        assert_that(self.database.has_group(RAW_GROUP), is_(True))
        assert_that(self.database.has_group("NOT_GROUP"), is_(False))

    def test_list_group(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(self.database.list_groups(), equal_to([RAW_GROUP]))

    def test_table_protected(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(
            self.database._is_table_protected(ExampleStore.data_token), is_(False)
        )
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinitionV1_definition", PROTECTED_GROUP),
            DataToken("TableReferenceV1_definition", PROTECTED_GROUP),
            DataToken("StoreReferenceV1_definition", PROTECTED_GROUP),
        ]
        for token in protected_tokens:
            assert_that(self.database._is_table_protected(token), is_(True))

    def test_has_table(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        all_tokens = [
            ExampleStore.data_token,
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("TableReferenceV1_definition", PROTECTED_GROUP),
            DataToken("StoreDefinitionV1_definition", PROTECTED_GROUP),
            DataToken("StoreReferenceV1_definition", PROTECTED_GROUP),
        ]
        for token in all_tokens:
            assert_that(self.database.has_table(token), is_(True))
        assert_that(
            self.database.has_table(DataToken("table_reference", "NOT_GROUP")),
            is_(False),
        )
        assert_that(
            self.database.has_table(DataToken("not_table", "public")), is_(False)
        )

    def test_list_table(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        all_tokens = [ExampleStore.data_token]
        assert_that(self.database.list_tables(), equal_to(all_tokens))

    def test_list_group_tables(self) -> None:
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("StoreDefinitionV1_definition", PROTECTED_GROUP),
            DataToken("TableReferenceV1_definition", PROTECTED_GROUP),
            DataToken("StoreReferenceV1_definition", PROTECTED_GROUP),
            DataToken("ExampleStoreV1_definition", PROTECTED_GROUP),
        ]

        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(
            self.database.list_group_tables(RAW_GROUP),
            equal_to([ExampleStore.data_token]),
        )
        assert_that(
            self.database.list_group_tables(PROTECTED_GROUP), equal_to(protected_tokens)
        )

    def test_insert(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(self.database.has_table(ExampleStore.data_token), is_(True))
        queried = self.database.query[ExampleStore](ExampleStore.data_token)
        assert_that(queried.equals(data), is_(True))

    def test_update(self) -> None:
        fail("Not Implemented")

    def test_upsert(self) -> None:
        fail("Not Implemented")

    def test_delete(self) -> None:
        fail("Not Implemented")

    def test_copy_table(self) -> None:
        fail("Not Implemented")

    def test_drop_group(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)

        assert_that(self.database.list_groups(), equal_to([RAW_GROUP]))
        self.database.drop_group(RAW_GROUP)
        assert_that(self.database.list_groups(), equal_to([]))

    def test_drop_table(self) -> None:
        # self.test_create_table()

        data_token = DataToken("test_table", "test_group")
        self.db_adapter.drop_table(data_token)
        assert_that(self.db_adapter.list_groups(), equal_to([]))
        assert_that(self.db_adapter.list_tables(), equal_to([]))
        assert_that(self.db_adapter.list_group_tables("test_group"), equal_to([]))
