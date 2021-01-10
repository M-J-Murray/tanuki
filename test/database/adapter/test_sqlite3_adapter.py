from src.database.data_token import DataToken
from test.helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to, is_in, not_
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.database.adapter.sqlite3.sqlite3_adapter import Sqlite3Adapter

from test.helpers.example_store import ExampleStore



class TestSqlite3Adapter:
    sql_db: Sqlite3Container
    db_adapter: Sqlite3Adapter

    @classmethod
    def setup_class(cls) -> None:
        cls.sql_db = Sqlite3Container()
        cls.db_adapter = Sqlite3Adapter(cls.sql_db.connection_config())

    def teardown_method(self) -> None:
        self.db_adapter.stop()
        self.sql_db.reset()
        self.db_adapter = Sqlite3Adapter(self.sql_db.connection_config())

    @classmethod
    def teardown_class(cls) -> None:
        cls.db_adapter.stop()
        cls.sql_db.stop()

    def test_insert(self) -> None:
        data_token = DataToken("test_table", "raw")
        test_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True])
        self.db_adapter.insert(data_token, test_store)
