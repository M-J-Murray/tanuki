from datetime import datetime
from pathlib import Path
import shutil
import tempfile

from precisely import assert_that, equal_to, not_
from pytest import fail

from helpers.example_metadata import ExampleMetadata
from helpers.example_store import ExampleStore, RAW_GROUP
from helpers.mock_backend import MockBackend
from helpers.sqlite3_container import Sqlite3Container
from tanuki.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter
from tanuki.database.data_token import DataToken
from tanuki.database.db_exceptions import DatabaseAdapterUsageError


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
        try:
            with self.db_adapter:
                token = ExampleStore.data_token
                assert_that(
                    self.db_adapter.has_group(token.data_group), equal_to(False)
                )
                assert_that(self.db_adapter.has_group_table(token), equal_to(False))

                self.db_adapter.create_group(token.data_group)
                self.db_adapter.create_group_table(token, ExampleStore)

                assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
                assert_that(self.db_adapter.has_group_table(token), equal_to(True))

                self.db_adapter.query(token, "INVALID")
                fail("Expected exception")
            fail("Expected exception")
        except Exception as e:
            assert_that(
                str(e),
                equal_to(
                    "Database command failed, rolling back: query failed\nno such column: INVALID"
                ),
            )

        with self.db_adapter:
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
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
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            self.db_adapter._update_group_table_metadata(token, test_metadata)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))
            assert_that(
                self.db_adapter.get_group_table_metadata(token, ExampleMetadata),
                not_(equal_to(None)),
            )

            self.db_adapter.drop_group_table(token)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))
            assert_that(
                self.db_adapter.get_group_table_metadata(token, ExampleMetadata),
                equal_to(None),
            )

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
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            self.db_adapter._update_group_table_metadata(token, test_metadata)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))
            assert_that(
                self.db_adapter.get_group_table_metadata(token, ExampleMetadata),
                not_(equal_to(None)),
            )

        with self.db_adapter:
            assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
            assert_that(self.db_adapter.has_group_table(token), equal_to(True))
            assert_that(
                self.db_adapter.get_group_table_metadata(token, ExampleMetadata),
                not_(equal_to(None)),
            )

        with self.db_adapter:
            self.db_adapter.drop_group(token.data_group)

            assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
            assert_that(self.db_adapter.has_group_table(token), equal_to(False))
            assert_that(
                self.db_adapter.get_group_table_metadata(token, ExampleMetadata),
                equal_to(None),
            )

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
                self.db_adapter.has_index(
                    ExampleStore.data_token, ExampleStore.a_index
                ),
                equal_to(True),
            )
            assert_that(
                self.db_adapter.has_index(token2, ExampleStore.a_index), equal_to(True)
            )
            self.db_adapter.insert(ExampleStore.data_token, test1)

    def test_group_table_metadata_path(self) -> None:
        expected = self.tmp_db_dir / "metadata" / "raw" / "test.json"
        actual = self.db_adapter._group_table_metadata_path(ExampleStore.data_token)
        assert_that(actual, equal_to(expected))

    def test_update_get_group_table_metadata(self) -> None:
        test_metadata = ExampleMetadata(
            test_str="test",
            test_int=123,
            test_float=0.123,
            test_bool=True,
            test_timestamp=datetime.now(),
        )
        with self.db_adapter:
            self.db_adapter._update_group_table_metadata(
                ExampleStore.data_token, test_metadata
            )

        actual = self.db_adapter.get_group_table_metadata(
            ExampleStore.data_token, ExampleMetadata
        )
        assert_that(actual, equal_to(test_metadata))

    def test_delete_group_table_metadata(self) -> None:
        with self.db_adapter:
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            self.db_adapter._update_group_table_metadata(
                ExampleStore.data_token, test_metadata
            )
            actual = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual, equal_to(test_metadata))

            self.db_adapter._delete_group_table_metadata(ExampleStore.data_token)

            actual = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual, equal_to(None))

    def test_delete_group_metadata(self) -> None:
        with self.db_adapter:
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            token1 = DataToken("test1", RAW_GROUP)
            token2 = DataToken("test2", RAW_GROUP)
            self.db_adapter._update_group_table_metadata(token1, test_metadata)
            self.db_adapter._update_group_table_metadata(token2, test_metadata)
            assert_that(
                self.db_adapter.get_group_table_metadata(token1, ExampleMetadata),
                not_(equal_to(None)),
            )
            assert_that(
                self.db_adapter.get_group_table_metadata(token2, ExampleMetadata),
                not_(equal_to(None)),
            )

            self.db_adapter._delete_group_metadata(RAW_GROUP)

            assert_that(
                self.db_adapter.get_group_table_metadata(token1, ExampleMetadata),
                equal_to(None),
            )
            assert_that(
                self.db_adapter.get_group_table_metadata(token2, ExampleMetadata),
                equal_to(None),
            )

    def test_insert_from_values(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), equal_to(True))

            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            test2 = ExampleStore(a=["d"], b=[4], c=[False], metadata=test_metadata)
            self.db_adapter.insert(ExampleStore.data_token, test2)

            actual_metadata = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual_metadata, equal_to(test_metadata))

            test3 = ExampleStore.concat([test1, test2], ignore_index=True)
            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)

            assert_that(queried2.equals(test3), equal_to(True))

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
            assert_that(test1.equals(queried1), equal_to(True))

    def test_update_from_values(self) -> None:
        with self.db_adapter:
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            test1 = ExampleStore(
                a=["a", "b", "c"],
                b=[1, 2, 3],
                c=[True, False, True],
                metadata=test_metadata,
            )
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            actual_metadata = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual_metadata, equal_to(test_metadata))

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), equal_to(True))

            test2_metadata = ExampleMetadata("b", 2, 1.234, True, datetime.now())
            test2 = ExampleStore(a=["b"], b=[4], c=[True], metadata=test2_metadata)
            self.db_adapter.update(ExampleStore.data_token, test2, [ExampleStore.a])

            actual_metadata = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual_metadata, equal_to(test2_metadata))

            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)
            test2 = ExampleStore(a=["a", "b", "c"], b=[1, 4, 3], c=[True, True, True])
            assert_that(queried2.equals(test2), equal_to(True))

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
            assert_that(queried2.equals(test2), equal_to(True))

    def test_upsert_from_values(self) -> None:
        with self.db_adapter:
            test_metadata = ExampleMetadata(
                test_str="test",
                test_int=123,
                test_float=0.123,
                test_bool=True,
                test_timestamp=datetime.now(),
            )
            test1 = ExampleStore(
                a=["a", "b", "c"],
                b=[1, 2, 3],
                c=[True, False, True],
                metadata=test_metadata,
            )
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.create_index(ExampleStore.data_token, ExampleStore.a_index)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            actual_metadata = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual_metadata, equal_to(test_metadata))

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), equal_to(True))

            test2_metadata = ExampleMetadata(
                test_str="test2",
                test_int=234,
                test_float=1.234,
                test_bool=False,
                test_timestamp=datetime.now(),
            )
            test2 = ExampleStore(
                a=["b", "e"], b=[4, 5], c=[True, False], metadata=test2_metadata
            )
            self.db_adapter.upsert(ExampleStore.data_token, test2, [ExampleStore.a])

            actual_metadata = self.db_adapter.get_group_table_metadata(
                ExampleStore.data_token, ExampleMetadata
            )
            assert_that(actual_metadata, equal_to(test2_metadata))

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw1)
            test2 = ExampleStore(
                a=["a", "b", "c", "e"], b=[1, 4, 3, 5], c=[True, True, True, False]
            )
            assert_that(queried2.equals(test2), equal_to(True))

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
            assert_that(queried2.equals(test2), equal_to(True))

    def test_delete(self) -> None:
        with self.db_adapter:
            test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
            self.db_adapter.create_group(ExampleStore.data_token.data_group)
            self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
            self.db_adapter.insert(ExampleStore.data_token, test1)

            raw1 = self.db_adapter.query(ExampleStore.data_token)
            queried1 = ExampleStore.from_rows(raw1)
            assert_that(queried1.equals(test1), equal_to(True))

            self.db_adapter.delete(ExampleStore.data_token, ExampleStore.b == 2)
            raw2 = self.db_adapter.query(ExampleStore.data_token)
            queried2 = ExampleStore.from_rows(raw2)
            test2 = ExampleStore(a=["a", "c"], b=[1, 3], c=[True, True])
            assert_that(queried2.equals(test2), equal_to(True))
