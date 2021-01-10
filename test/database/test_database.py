from test.helpers.mock_adapter import MockAdapter

from hamcrest import assert_that, equal_to, is_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.data_token import DataToken
from src.database.database import Database

from test.helpers.example_store import ExampleStore



class TestDatabase:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()
        self.database = Database(self.adapter)

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
