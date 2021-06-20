import os
from pathlib import Path
import shutil
import tempfile
from test.helpers.example_store import ExampleStore
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to, is_, is_in, not_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter
from src.database.data_token import DataToken


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

    def test_create_group_table(self) -> None:
        token = ExampleStore.data_token
        assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
        assert_that(self.db_adapter.has_group_table(token), equal_to(False))

        self.db_adapter.create_group(token.data_group)
        self.db_adapter.create_group_table(token, ExampleStore)

        assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
        assert_that(self.db_adapter.has_group_table(token), equal_to(True))

    def test_table_persistance(self) -> None:
        token = ExampleStore.data_token
        assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))
        assert_that(self.db_adapter.has_group_table(token), equal_to(False))

        self.db_adapter.create_group(token.data_group)
        self.db_adapter.create_group_table(token, ExampleStore)

        assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
        assert_that(self.db_adapter.has_group_table(token), equal_to(True))

        self.db_adapter.stop()
        self.db_adapter = Sqlite3Adapter(self.sql_db.connection_config())

        assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))
        assert_that(self.db_adapter.has_group_table(token), equal_to(True))

    def test_drop_group_table(self) -> None:
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
        token = ExampleStore.data_token
        assert_that(self.db_adapter.has_group(token.data_group), equal_to(False))

        self.db_adapter.create_group("raw")
        assert_that(self.db_adapter.has_group(token.data_group), equal_to(True))

    def test_drop_group(self) -> None:
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

    def test_insert_query(self) -> None:
        test1 = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        self.db_adapter.create_group(ExampleStore.data_token.data_group)
        self.db_adapter.create_group_table(ExampleStore.data_token, ExampleStore)
        self.db_adapter.insert(ExampleStore.data_token, test1)

        raw1 = self.db_adapter.query(ExampleStore.data_token)
        queried1 = ExampleStore.from_rows(raw1)
        assert_that(queried1.equals(test1), is_(True))

    def test_update(self) -> None:
        fail("Not Implemented")

    def test_upsert(self) -> None:
        fail("Not Implemented")

    def test_delete(self) -> None:
        fail("Not Implemented")

    def test_drop_indices(self) -> None:
        fail("Not Implemented")
