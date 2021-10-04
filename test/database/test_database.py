from test.helpers.example_store import ExampleStore, RAW_GROUP
from test.helpers.mock_adapter import MockAdapter

from hamcrest import assert_that, equal_to, is_

from src.tanuki.database.data_token import DataToken
from src.tanuki.database.database import Database
from src.tanuki.database.reference_tables import PROTECTED_GROUP


class TestDatabase:
    def setup_method(self) -> None:
        self.adapter = MockAdapter()
        self.database = Database(self.adapter)

    def test_has_group(self) -> None:
        test = ExampleStore(a=["1", "2", "3"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, test)
        assert_that(self.database.has_group(PROTECTED_GROUP), is_(True))
        assert_that(self.database.has_group(RAW_GROUP), is_(True))
        assert_that(self.database.has_group("NOT_GROUP"), is_(False))

    def test_list_group(self) -> None:
        test = ExampleStore(a=["1", "2", "3"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, test)
        assert_that(self.database.list_groups(), equal_to([RAW_GROUP]))

    def test_has_table(self) -> None:
        test = ExampleStore(a=["1", "2", "3"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, test)
        all_tokens = [
            ExampleStore.data_token,
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
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
        test = ExampleStore(a=["1", "2", "3"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, test)
        all_tokens = [ExampleStore.data_token]
        assert_that(self.database.list_tables(), equal_to(all_tokens))

    def test_list_group_tables(self) -> None:
        protected_tokens = [
            DataToken("table_reference", PROTECTED_GROUP),
            DataToken("store_reference", PROTECTED_GROUP),
            DataToken("index_reference", PROTECTED_GROUP),
            DataToken("StoreDefinition_v1_definition", PROTECTED_GROUP),
            DataToken("TableReference_v1_definition", PROTECTED_GROUP),
            DataToken("StoreReference_v1_definition", PROTECTED_GROUP),
            DataToken("IndexReference_v1_definition", PROTECTED_GROUP),
            DataToken("ExampleStore_v1_definition", PROTECTED_GROUP),
        ]

        test = ExampleStore(a=["1", "2", "3"], b=[1, 2, 3], c=[True, False, True])
        self.database.insert(ExampleStore.data_token, test)
        assert_that(
            self.database.list_group_tables(RAW_GROUP),
            equal_to([ExampleStore.data_token]),
        )
        assert_that(
            self.database.list_group_tables(PROTECTED_GROUP),
            equal_to(protected_tokens),
        )

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

    def test_drop_table(self) -> None:
        assert_that(self.database.has_group(RAW_GROUP), equal_to(False))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(False))

        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)

        assert_that(self.database.has_group(RAW_GROUP), equal_to(True))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(True))

        self.database.drop_table(ExampleStore.data_token)

        assert_that(self.database.has_group(RAW_GROUP), equal_to(False))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(False))

    def test_drop_group(self) -> None:
        assert_that(self.database.has_group(RAW_GROUP), equal_to(False))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(False))

        data = ExampleStore(a="a", b=1, c=True)
        self.database.insert(ExampleStore.data_token, data)

        assert_that(self.database.has_group(RAW_GROUP), equal_to(True))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(True))

        self.database.drop_group(RAW_GROUP)

        assert_that(self.database.has_group(RAW_GROUP), equal_to(False))
        assert_that(self.database.has_table(ExampleStore.data_token), equal_to(False))
