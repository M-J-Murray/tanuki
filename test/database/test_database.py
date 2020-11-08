from hamcrest import assert_that, equal_to
from pytest import fail

from src.database.data_token import DataToken

from src.data_store.data_store import DataStore
from src.data_store.column import Column
from src.database.database import Database

from test.helpers.mock_adapter import MockAdapter


class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestDatabase:
    def setup_method(self) -> None:
        self.database = Database(MockAdapter())

    def test_drop_group(self) -> None:
        # self.test_create_group()

        self.database.drop_group("test_group")
        assert_that(self.db_adapter.list_groups(), equal_to([]))

    def test_drop_table(self) -> None:
        # self.test_create_table()

        data_token = DataToken("test_table", "test_group")
        self.db_adapter.drop_table(data_token)
        assert_that(self.db_adapter.list_groups(), equal_to([]))
        assert_that(self.db_adapter.list_tables(), equal_to([]))
        assert_that(self.db_adapter.list_group_tables("test_group"), equal_to([]))

    def test_list_group(self) -> None:
        # self.test_create_group()
        data_token = DataToken("test_table", "test_group2")
        self.db_adapter.create_table(data_token, ExampleStore)

        assert_that(
            self.db_adapter.list_groups(), equal_to(["test_group1", "test_group2"])
        )

    def test_list_table(self) -> None:
        fail("Not Implemented")

    def test_list_group_tables(self) -> None:
        fail("Not Implemented")
