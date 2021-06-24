from pathlib import Path
import shutil
import tempfile
from test.helpers.example_store import ExampleStore
from test.helpers.sqlite3_container import Sqlite3Container
from typing import Any

from hamcrest import assert_that, equal_to, is_
import numpy as np
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore
from src.data_store.data_type import Boolean, Int64, String
from src.database.data_token import DataToken
from src.database.sqlite3_database import Sqlite3Database


class TestDatabaseBackend:
    sql_db: Sqlite3Container

    @classmethod
    def setup_class(cls) -> None:
        tmp_db_dir = Path(tempfile.gettempdir()) / "tanuki_test"
        if tmp_db_dir.exists():
            shutil.rmtree(tmp_db_dir, ignore_errors=True)
        tmp_db_dir.mkdir()
        cls.sql_db = Sqlite3Container(tmp_db_dir)

    @classmethod
    def teardown_class(cls) -> None:
        cls.sql_db.stop()

    def setup_method(self) -> None:
        self.test_store = ExampleStore(
            a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True]
        )
        conn_config = self.sql_db.connection_config()
        self.db = Sqlite3Database(conn_config)
        self.db.insert(ExampleStore.data_token, self.test_store)
        self.db_store = ExampleStore.link(self.db, ExampleStore.data_token)
        queried = self.db_store.query()
        assert_that(queried.equals(self.test_store), is_(True))

    def teardown_method(self) -> None:
        self.sql_db.reset()

    def test_is_link(self) -> None:
        assert_that(self.db_store.is_link(), is_(True))

    def test_link_token(self) -> None:
        assert_that(self.db_store.link_token(), equal_to(ExampleStore.data_token))

    def test_to_pandas(self) -> None:
        assert_that(
            self.db_store.to_pandas().equals(self.test_store.to_pandas()), is_(True)
        )

    def test_columns(self) -> None:
        assert_that(
            [str(col) for col in self.db_store.columns],
            equal_to(["index", "a", "b", "c"]),
        )

    def test_values(self) -> None:
        expected = np.array(
            [
                ["a", 1, True],
                ["b", 2, False],
                ["c", 3, True],
            ],
            dtype="object",
        )
        assert_that(np.array_equal(self.db_store.values, expected), is_(True))

    def test_dtypes(self) -> None:
        assert_that(
            self.db_store.dtypes,
            equal_to({"index": Int64, "a": String, "b": Int64, "c": Boolean}),
        )

    def test_cast_columns(self) -> None:
        try:
            self.db_store._data_backend.cast_columns({"a": Int64})
            fail("Expected exception")
        except Exception as e:
            assert_that(isinstance(e, NotImplementedError), is_(True))

    def test_to_dict(self) -> None:
        expected = {
            "a": ["a", "b", "c"],
            "b": [1, 2, 3],
            "c": [True, False, True],
        }
        assert_that(self.db_store.to_dict(), equal_to(expected))

    def test_index(self) -> None:
        expected = np.array([0, 1, 2])
        index = self.db_store.index
        print(index)
        assert_that(isinstance(index, Column), equal_to(True))
        assert_that(np.array_equal(index.values, expected), equal_to(True))

    def test_index_name(self) -> None:
        raise NotImplementedError()

    def test_loc(self) -> None:
        raise NotImplementedError()

    def test_iloc(self) -> None:
        raise NotImplementedError()

    def test_equals(self) -> None:
        raise NotImplementedError()

    def test_eq(self) -> None:
        raise NotImplementedError()

    def test_ne(self) -> None:
        raise NotImplementedError()

    def test_gt(self) -> None:
        raise NotImplementedError()

    def test_ge(self) -> None:
        raise NotImplementedError()

    def test_lt(self) -> None:
        raise NotImplementedError()

    def test_le(self) -> None:
        raise NotImplementedError()

    def test_len(self) -> int:
        raise NotImplementedError()

    def test_iter(self) -> None:
        raise NotImplementedError()

    def test_iterrows(self) -> None:
        raise NotImplementedError()

    def test_itertuples(self) -> None:
        raise NotImplementedError()

    def test_getitem(self) -> None:
        raise NotImplementedError()

    def test_getitems(self) -> None:
        raise NotImplementedError()

    def test_getmask(self) -> None:
        raise NotImplementedError()

    def test_query(self) -> None:
        raise NotImplementedError()

    def test_setitem(self) -> None:
        raise NotImplementedError()

    def test_set_index(self) -> None:
        raise NotImplementedError()

    def test_reset_index(self) -> None:
        raise NotImplementedError()

    def test_append(self) -> None:
        raise NotImplementedError()

    def test_drop_indices(self) -> None:
        raise NotImplementedError()

    def test_concat(self) -> None:
        raise NotImplementedError()

    def test_str(self) -> None:
        raise NotImplementedError()

    def test_repr(self) -> None:
        raise NotImplementedError()
