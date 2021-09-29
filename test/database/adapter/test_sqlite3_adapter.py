from pathlib import Path
import shutil
import tempfile
from test.helpers.example_store import ExampleStore, RAW_GROUP
from test.helpers.mock_backend import MockBackend
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to, is_
from pytest import fail

from src.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter
from src.database.data_token import DataToken
from src.database.db_exceptions import DatabaseAdapterUsageError


class TestSqlite3Adapter:
    sql_db: Sqlite3Container
    db_adapter: Sqlite3Adapter

    def setup_method(self) -> None:
        self.tmp_db_dir = Path(tempfile.gettempdir()) / "tanuki_test"
        if self.tmp_db_dir.exists():
            shutil.rmtree(self.tmp_db_dir, ignore_errors=True)
        self.sql_db = Sqlite3Container(self.tmp_db_dir)
        self.sql_db.start()
        self.db_adapter = Sqlite3Adapter(self.sql_db.connection_config())

    def teardown_method(self) -> None:
        self.db_adapter.stop()
        self.sql_db.stop()

    def test_invalid_usage(self) -> None:
        try:
            self.db_adapter.has_group(ExampleStore.data_token)
            fail("Expected exception")
        except DatabaseAdapterUsageError as e:
            assert_that(
                str(e),
                equal_to(
                    "Data adapter wasn't opened before method call: 'has_group'\nUse 'with adapter:'"
                ),
            )

    def test_exception_rollback(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

            self.db_adapter.create_group(token.data_group)
            self.db_adapter.create_group_table(token, ExampleStore)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

            try:
                self.db_adapter.query(token, "INVALID")
            except Exception:
                pass

            # assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

    def test_create_group_table(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

            self.db_adapter.create_group(token.data_group)
            self.db_adapter.create_group_table(token, ExampleStore)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

    def test_table_persistance(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

            self.db_adapter.create_group(token.data_group)
            self.db_adapter.create_group_table(token, ExampleStore)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

        self.db_adapter.stop()
        self.db_adapter = Sqlite3Adapter(self.sql_db.connection_config())

        with self.db_adapter:
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

    def test_drop_group_table(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

            self.db_adapter.create_group(token.data_group)
            self.db_adapter.create_group_table(token, ExampleStore)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

            self.db_adapter.drop_group_table(token)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

    def test_create_group(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))

            self.db_adapter.create_group("raw")
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))

    def test_drop_group(self) -> None:
        with self.db_adapter:
            token = ExampleStore.data_token
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

            self.db_adapter.create_group(token.data_group)
            self.db_adapter.create_group_table(token, ExampleStore)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))

            self.db_adapter.drop_group(token.data_group)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))

    def test_create_index(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.create_index(ExampleStore.data_token, ExampleStore.a_index)

            token2 = DataToken("test2", RAW_GROUP)
            self.db_adapter.create_group_table(token2, ExampleStore)
            self.db_adapter.create_index(token2, ExampleStore.a_index)

            assert_that(
                self.db_adapter.has_index(ExampleStore.data_token, ExampleStore.a_index)
            )
            assert_that(self.db_adapter.has_index(token2, ExampleStore.a_index))
            self.db_adapter.insert(ExampleStore.data_token, test1)

    def test_insert_from_values(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), is_(True))

            test2 = ExampleStore(a=["d"], b=[4], c=[False])
            self.db_adapter.insert(ExampleStore.data_token, test2)

            test3 = ExampleStore.concat([test1, test2], ignore_index=True)
            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)

            assert_that(queried2.equals(test3), is_(True))

    def test_insert_from_link(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            token2 = DataToken("test2", RAW_GROUP)
            self.db_adapter.create_group_table(token2, ExampleStore)

            linked_store = ExampleStore.from_backend(
                MockBackend(ExampleStore.data_token, test1.columns), validate=False
            )
            self.db_adapter.insert(token2, linked_store)

            raw1 = self.db_adapter.query(token2)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(test1.equals(queried1))

    def test_update_from_values(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), is_(True))

            test2 = ExampleStore(a=["b"], b=[4], c=[True])
            self.db_adapter.update(ExampleStore.data_token, test2, [ExampleStore.a])

            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)
            test2 = ExampleStore(a=["a", "b", "c"], b=[1, 4, 3], c=[True, True, True])
            assert_that(queried2.equals(test2), is_(True))

    def test_update_from_link(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            token2 = DataToken("test2", RAW_GROUP)
            test2 = ExampleStore(a=["b"], b=[4], c=[True])
            self.db_adapter.create_group_table(token2, ExampleStore)
            self.db_adapter.insert(token2, test2)

            linked_store = ExampleStore.from_backend(
                MockBackend(token2, test2.columns), validate=False
            )
            self.db_adapter.update(
                ExampleStore.data_token, linked_store, ExampleStore.a_index.columns
            )

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw1)
            test2 = ExampleStore(a=["a", "b", "c"], b=[1, 4, 3], c=[True, True, True])
            assert_that(queried2.equals(test2), is_(True))

    def test_upsert_from_values(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.create_index(ExampleStore.data_token, ExampleStore.a_index)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), is_(True))

            test2 = ExampleStore(a=["b", "e"], b=[4, 5], c=[True, False])
            self.db_adapter.upsert(ExampleStore.data_token, test2, [ExampleStore.a])

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw1)
            test2 = ExampleStore(
                a=["a", "b", "c", "e"], b=[1, 4, 3, 5], c=[True, True, True, False]
            )
            assert_that(queried2.equals(test2), is_(True))

    def test_upsert_from_link(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.create_index(ExampleStore.data_token, ExampleStore.a_index)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            token2 = DataToken("test2", RAW_GROUP)
            test2 = ExampleStore(a=["b", "e"], b=[4, 5], c=[True, False])
            self.db_adapter.create_group_table(token2, ExampleStore)
            self.db_adapter.create_index(token2, ExampleStore.a_index)
            self.db_adapter.insert(token2, test2)

            linked_store = ExampleStore.from_backend(
                MockBackend(token2, test2.columns), validate=False
            )
            self.db_adapter.upsert(
                ExampleStore.data_token, linked_store, [ExampleStore.a]
            )

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw1)
            test2 = ExampleStore(
                a=["a", "b", "c", "e"], b=[1, 4, 3, 5], c=[True, True, True, False]
            )
            assert_that(queried2.equals(test2), is_(True))

    def test_delete(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), is_(True))

            self.db_adapter.delete(ExampleStore.data_token, ExampleStore.b == 2)
            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)
            test2 = ExampleStore(a=["a", "c"], b=[1, 3], c=[True, True])
            assert_that(queried2.equals(test2), is_(True))
