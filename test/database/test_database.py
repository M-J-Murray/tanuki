from test.helpers.example_store import ExampleStore
from test.helpers.mock_adapter import MockAdapter

from hamcrest import assert_that, equal_to, is_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.data_token import DataToken
from src.database.database import Database


class TestDatabase:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()
        self.database = Database(self.adapter)

    def test_insert(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)
        assert_that(self.database.has_table(ExampleStore.data_token), is_(True))
        queried = self.database.query(ExampleStore, ExampleStore.data_token)
        assert_that(queried.equals(data), is_(True))

    def test_update(self) -> None:
        data = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, data)

        row_replacement = ExampleStore(a="b", b=2, c=True)
        self.database.update(
            ExampleStore.data_token, row_replacement, [ExampleStore.a, ExampleStore.b]
        )

        queried = self.database.query(ExampleStore, ExampleStore.data_token)
        expected = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, True, True])
        assert_that(queried.equals(expected), is_(True))

    def test_upsert(self) -> None:
        data = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, data)

        row_replacement = ExampleStore(
            a=["b", "d", "b"], b=[2, 2, 4], c=[True, False, False]
        )
        self.database.upsert(
            ExampleStore.data_token, row_replacement, [ExampleStore.a, ExampleStore.b]
        )

        queried = self.database.query(ExampleStore, ExampleStore.data_token)
        expected = ExampleStore(
            a=["a", "b", "c", "d", "b"],
            b=[1, 2, 3, 2, 4],
            c=[True, True, True, False, False],
        )
        assert_that(queried.equals(expected), is_(True))

    def test_delete(self) -> None:
        data = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, data)
        self.database.delete(ExampleStore.data_token, ExampleStore.b < 3)

        queried = self.database.query(ExampleStore, ExampleStore.data_token)
        expected = ExampleStore(
            a=["c"],
            b=[3],
            c=[True],
        )
        assert_that(queried.equals(expected), is_(True))

    def test_drop_indices(self) -> None:
        fail("Not Implemented")

    def test_drop_table(self) -> None:
        # self.test_create_table()

        data_token = DataToken("test_table", "test_group")
        self.db_adapter.drop_table(data_token)
        assert_that(self.db_adapter.list_groups(), equal_to([]))
        assert_that(self.db_adapter.list_tables(), equal_to([]))
        assert_that(self.db_adapter.list_group_tables("test_group"), equal_to([]))

    def test_drop_group(self) -> None:
        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)

        assert_that(self.database.list_groups(), equal_to([RAW_GROUP]))
        self.database.drop_group(RAW_GROUP)
        assert_that(self.database.list_groups(), equal_to([]))
