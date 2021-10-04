from pathlib import Path
import shutil
import tempfile
from helpers.example_store import ExampleStore
from helpers.sqlite3_container import Sqlite3Container

import numpy as np
from precisely import assert_that, equal_to
from pytest import fail

from tanuki.data_store.index.database_index import DatabaseIndex
from tanuki.data_store.index.pandas_index import PandasIndex, PIndex
from tanuki.database.sqlite3_database import Sqlite3Database


class TestDatabaseIndex:
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
        self.backend = self.db_store._data_backend
        self.index = self.db_store.index
        self.a_index = self.db_store.a_index
        self.ab_index = self.db_store.ab_index

    def teardown_method(self) -> None:
        self.sql_db.reset()

    def test_name(self):
        assert_that(self.index.name, equal_to("index"))
        assert_that(self.a_index.name, equal_to("a_index"))
        assert_that(self.ab_index.name, equal_to("ab_index"))

    def test_columns(self):
        assert_that(self.index.columns, equal_to([]))
        assert_that(self.a_index.columns, equal_to(["a"]))
        assert_that(self.ab_index.columns, equal_to(["a", "b"]))

    def test_to_pandas(self) -> None:
        assert_that(self.index.to_pandas().equals(self.index), equal_to(True))

        expected = PandasIndex(PIndex(["a", "b", "c"], name="a_index"), ["a"])
        assert_that(self.a_index.to_pandas().equals(expected), equal_to(True))

        multi_index = PIndex([("a", 1), ("b", 2), ("c", 3)])
        multi_index.name = "ab_index"
        expected = PandasIndex(multi_index, ["a", "b"])
        assert_that(self.ab_index.to_pandas().equals(expected), equal_to(True))

    def test_getitem(self):
        expected = PandasIndex(PIndex([1], name="index"), [])
        assert_that(self.index[1], equal_to(1))
        assert_that(self.index[[1]].equals(expected), equal_to(True))

        expected = PandasIndex(PIndex(["b"], name="a_index"), ["a"])
        assert_that(self.a_index[1], equal_to("b"))
        assert_that(self.a_index[[1]].equals(expected), equal_to(True))

        multi_index = PIndex([("b", 2)])
        multi_index.name = "ab_index"
        expected = PandasIndex(multi_index, ["a", "b"])
        assert_that(self.ab_index[1], equal_to(("b", 2)))
        assert_that(self.ab_index[[1]].equals(expected), equal_to(True))

    def test_values(self):
        expected = np.array([0, 1, 2])
        assert_that(np.array_equal(self.index.values, expected), equal_to(True))

        expected = np.array(["a", "b", "c"])
        assert_that(np.array_equal(self.a_index.values, expected), equal_to(True))

        expected = np.array([("a", 1), ("b", 2), ("c", 3)], dtype="object,int")
        assert_that(np.array_equal(self.ab_index.values, expected), equal_to(True))

    def test_tolist(self):
        expected = [0, 1, 2]
        assert_that(np.array_equal(self.index.tolist(), expected), equal_to(True))

        expected = np.array(["a", "b", "c"])
        assert_that(np.array_equal(self.a_index.tolist(), expected), equal_to(True))

        expected = [("a", 1), ("b", 2), ("c", 3)]
        assert_that(np.array_equal(self.ab_index.tolist(), expected), equal_to(True))

    def test_equals(self):
        test = PandasIndex(PIndex([0, 1, 2], name="index"), [])
        assert_that(self.index.equals(test), equal_to(True))

        test = DatabaseIndex("a_index", self.backend["a"])
        assert_that(self.a_index.equals(test), equal_to(True))

        test = DatabaseIndex("ab_index", self.backend.getitems(["a", "b"]))
        assert_that(self.ab_index.equals(test), equal_to(True))

    def test_eq(self):
        expected = np.array([False, True, False])
        actual = self.index == 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array(["b", 2, False], dtype=object)
        query = self.a_index == "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index == ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_ne(self):
        expected = np.array([True, False, True])
        actual = self.index != 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array([["a", 1, True], ["c", 3, True]], dtype=object)
        query = self.a_index != "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index != ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_gt(self):
        expected = np.array([False, False, True])
        actual = self.index > 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array(["c", 3, True], dtype=object)
        query = self.a_index > "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index > ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_ge(self):
        expected = np.array([False, True, True])
        actual = self.index >= 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array([["b", 2, False], ["c", 3, True]], dtype=object)
        query = self.a_index >= "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index >= ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_lt(self):
        expected = np.array([True, False, False])
        actual = self.index < 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array(["a", 1, True], dtype=object)
        query = self.a_index < "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index < ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_le(self):
        expected = np.array([True, True, False])
        actual = self.index <= 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

        expected = np.array([["a", 1, True], ["b", 2, False]], dtype=object)
        query = self.a_index <= "b"
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

        query = self.ab_index <= ("b", 2)
        actual = self.backend.query(query)
        assert_that(np.array_equal(actual.values, expected), equal_to(True))

    def test_len(self) -> int:
        assert_that(len(self.index), equal_to(3))
        assert_that(len(self.a_index), equal_to(3))
        assert_that(len(self.ab_index), equal_to(3))

    def test_str(self) -> str:
        assert_that(str(self.index), equal_to("Int64Index([0, 1, 2], dtype='int64', name='index')"))
        assert_that(str(self.a_index), equal_to("Database Link: raw.test\nActive Columns: ['a']"))
        assert_that(str(self.ab_index), equal_to("Database Link: raw.test\nActive Columns: ['a', 'b']"))

    def test_repr(self) -> str:
        assert_that(repr(self.index), equal_to("Int64Index([0, 1, 2], dtype='int64', name='index')"))
        assert_that(repr(self.a_index), equal_to("Database Link: raw.test\nActive Columns: ['a']"))
        assert_that(repr(self.ab_index), equal_to("Database Link: raw.test\nActive Columns: ['a', 'b']"))
