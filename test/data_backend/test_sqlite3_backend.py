from src.database.sqlite3_database import Sqlite3Database
from test.helpers.sqlite3_container import Sqlite3Container

from pytest import fail

from src.data_store.data_store import DataStore
from src.data_store.column import Column


class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestSqlite3Backend:
    sql_db: Sqlite3Container

    @classmethod
    def setup_class(cls) -> None:
        cls.sql_db = Sqlite3Container()

    def teardown_method(self) -> None:
        self.sql_db.reset()

    @classmethod
    def teardown_class(cls) -> None:
        cls.sql_db.stop()

    def test_insert(self) -> None:
        test_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        conn_config = self.sql_db.connection_config()
        with Sqlite3Database(conn_config) as db:
            db.insert(test_store)
            
