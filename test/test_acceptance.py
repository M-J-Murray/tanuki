from test.helpers.example_store import ExampleStore
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to

from src.database.data_token import DataToken
from src.database.sqlite3_database import Sqlite3Database
import tempfile
from pathlib import Path
import shutil


class TestAcceptance:
    sql_db: Sqlite3Container

    @classmethod
    def setup_class(cls) -> None:
        tmp_db_dir = Path(tempfile.gettempdir()) / "tanuki_test"
        if tmp_db_dir.exists():
            shutil.rmtree(tmp_db_dir, ignore_errors=True)
        tmp_db_dir.mkdir()
        cls.sql_db = Sqlite3Container(tmp_db_dir)
        cls.sql_db.start()

    def teardown_method(self) -> None:
        self.sql_db.reset()

    @classmethod
    def teardown_class(cls) -> None:
        cls.sql_db.stop()

    def test_create_insert_query_data(self) -> None:
        insert_store = ExampleStore(
            a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True]
        )
        conn_conf = self.sql_db.connection_config()

        data_token = DataToken("test_table", "raw")
        with Sqlite3Database(conn_conf) as db:
            db.insert(data_token, insert_store)

            query_store = ExampleStore.link(db, data_token)
            len(query_store)
            query_mask = query_store.b >= 2
            actual = query_store[query_mask]

        expected = ExampleStore(index=[1, 2], a=["b", "c"], b=[2, 3], c=[False, True])
        assert_that(actual, expected)
