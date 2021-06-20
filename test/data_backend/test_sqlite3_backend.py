from typing import Any
from src.database.data_token import DataToken
from src.database.sqlite3_database import Sqlite3Database
from test.helpers.sqlite3_container import Sqlite3Container

from pytest import fail
from hamcrest import assert_that, is_

from src.data_store.data_store import DataStore
from src.data_store.column import Column

from test.helpers.example_store import ExampleStore
from pathlib import Path
import tempfile
import shutil


class TestSqlite3Backend:
    sql_db: Sqlite3Container

    @classmethod
    def setup_class(cls) -> None:
        tmp_db_dir = Path(tempfile.gettempdir()) / "tanuki_test"
        if tmp_db_dir.exists():
            shutil.rmtree(tmp_db_dir, ignore_errors=True)
        tmp_db_dir.mkdir()
        cls.sql_db = Sqlite3Container(tmp_db_dir)

    def teardown_method(self) -> None:
        self.sql_db.reset()

    @classmethod
    def teardown_class(cls) -> None:
        cls.sql_db.stop()

    def test_insert(self) -> None:
        data_token = DataToken("test_table", "raw")
        test_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        conn_config = self.sql_db.connection_config()
        with Sqlite3Database(conn_config) as db:
            db_store = ExampleStore.link(db, data_token)
            db_store = db_store.append(test_store)

        with Sqlite3Database(conn_config) as db:
            db_store = ExampleStore.link(db, data_token)
            assert_that(db_store.equals(test_store), is_(True))
