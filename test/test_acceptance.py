from src.database.data_token import DataToken
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.sqlite3_database import Sqlite3Database


class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestAcceptance:
    sql_db: Sqlite3Container

    @classmethod
    def setup_class(cls) -> None:
        cls.sql_db = Sqlite3Container()

    def teardown_method(self) -> None:
        self.sql_db.reset()

    @classmethod
    def teardown_class(cls) -> None:
        cls.sql_db.stop()

    def test_create_insert_query_data(self) -> None:
        insert_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        conn_conf = self.sql_db.connection_config()

        data_token = DataToken("test_table", "raw")
        with Sqlite3Database(conn_conf) as db:
            db.insert(data_token, insert_store)

            query_store = ExampleStore.link(db, data_token)
            query_mask = query_store.b > 2
            query_store = query_store[query_mask]
        fail("Write assertions")
