from datetime import datetime
from helpers.example_metadata import ExampleMetadata
from helpers.example_store import ExampleStore
from helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to

from tanuki.database.data_token import DataToken
from tanuki.database.sqlite3_database import Sqlite3Database
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
        metadata = ExampleMetadata(
            test_str="test",
            test_int=123,
            test_float=0.123,
            test_bool=True,
            test_timestamp=datetime.now(),
        )
        insert_store = ExampleStore(
            metadata=metadata,
            a=["a", "b", "c"],
            b=[1, 2, 3],
            c=[True, False, True],
        )
        conn_conf = self.sql_db.connection_config()

        data_token = DataToken("test_table", "raw")
        with Sqlite3Database(conn_conf) as db:
            db.insert(data_token, insert_store)
            query_mask = ExampleStore.b >= 2
            actual = db.query(ExampleStore, data_token, query_mask)

        expected = ExampleStore(
            metadata=metadata,
            a=["b", "c"],
            b=[2, 3],
            c=[False, True],
        )
        assert_that(actual.equals(expected), equal_to(True))
