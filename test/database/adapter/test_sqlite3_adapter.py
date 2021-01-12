import os
import tempfile
from test.helpers.example_store import ExampleStore
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to, is_in, not_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter
from src.database.data_token import DataToken


class TestSqlite3Adapter:
    sql_db: Sqlite3Container
    db_adapter: Sqlite3Adapter

    def setup_method(self) -> None:
        self.tmp_db_path = tempfile.gettempdir() + "/tmp_db.sqlite3"
        self.sql_db = Sqlite3Container(self.tmp_db_path)
        self.db_adapter = Sqlite3Adapter(self.sql_db.connection_config())

    def teardown_method(self) -> None:
        self.db_adapter.stop()
        self.sql_db.stop()
        os.remove(self.tmp_db_path)

    def test_create_table(self) -> None:
        token = ExampleStore.data_token
        assert_that(self.db_adapter.has_table(token), equal_to(False))

        self.db_adapter.create_table(token, ExampleStore)

        assert_that(self.db_adapter.has_table(token), equal_to(True))

    def test_drop_table(self) -> None:
        token = ExampleStore.data_token
        assert_that(self.db_adapter.has_table(token), equal_to(False))

        self.db_adapter.create_table(token, ExampleStore)

        assert_that(self.db_adapter.has_table(token), equal_to(True))

        self.db_adapter.drop_table(token)

        assert_that(self.db_adapter.has_table(token), equal_to(False))

    def test_create_group(self) -> None:
        fail("Not Implemented")

    def test_drop_group(self) -> None:
        fail("Not Implemented")

    def test_insert_query(self) -> None:
        fail("Not Implemented")

    def test_update(self) -> None:
        fail("Not Implemented")

    def test_upsert(self) -> None:
        fail("Not Implemented")

    def test_delete(self) -> None:
        fail("Not Implemented")

    def test_drop_indices(self) -> None:
        fail("Not Implemented")
