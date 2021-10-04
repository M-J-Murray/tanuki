from pathlib import Path
import shutil
import tempfile
from helpers.example_store import ExampleStore
from helpers.sqlite3_container import Sqlite3Container

from hamcrest import assert_that, equal_to, is_
import numpy as np
from pandas import Index as PIndex
from pandas import Series
from pytest import fail

from tanuki.data_backend.database_backend import DatabaseBackend
from tanuki.data_backend.pandas_backend import PandasBackend
from tanuki.data_store.data_type import Boolean, Int64, String
from tanuki.data_store.index.database_index import DatabaseIndex
from tanuki.data_store.index.pandas_index import PandasIndex
from tanuki.database.sqlite3_database import Sqlite3Database


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
        self.data_backend = self.db_store._data_backend

        self.test_series0 = PandasBackend(
            Series({"a": "a", "b": 1, "c": True}), index=[0]
        )
        self.test_series2 = PandasBackend(
            Series({"a": "c", "b": 3, "c": True}), index=[2]
        )

    def teardown_method(self) -> None:
        self.sql_db.reset()

    def test_is_link(self) -> None:
        assert_that(self.data_backend.is_link(), is_(True))

    def test_link_token(self) -> None:
        assert_that(self.data_backend.link_token(), equal_to(ExampleStore.data_token))

    def test_to_pandas(self) -> None:
        assert_that(
            self.data_backend.to_pandas().equals(self.test_store.to_pandas()), is_(True)
        )

    def test_columns(self) -> None:
        assert_that(
            [str(col) for col in self.data_backend.columns],
            equal_to(["a", "b", "c"]),
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
        assert_that(np.array_equal(self.data_backend.values, expected), is_(True))

    def test_dtypes(self) -> None:
        assert_that(
            self.data_backend.dtypes,
            equal_to({"a": String, "b": Int64, "c": Boolean}),
        )

    def test_cast_columns(self) -> None:
        try:
            self.data_backend.cast_columns({"a": Int64})
            fail("Expected exception")
        except Exception as e:
            assert_that(isinstance(e, NotImplementedError), is_(True))

    def test_to_dict(self) -> None:
        expected = {
            "a": ["a", "b", "c"],
            "b": [1, 2, 3],
            "c": [True, False, True],
        }
        assert_that(self.data_backend.to_dict(), equal_to(expected))

    def test_index(self) -> None:
        initial_expected = np.array([0, 1, 2])
        assert_that(self.data_backend.index.name, equal_to("index"))
        assert_that(isinstance(self.data_backend.index, PandasIndex), equal_to(True))
        assert_that(
            np.array_equal(self.data_backend.index.values, initial_expected),
            equal_to(True),
        )

        new_expected = np.array(["a", "b", "c"])
        new_store = self.data_backend.set_index(ExampleStore.a_index)
        assert_that(new_store.index.name, equal_to("a_index"))
        assert_that(isinstance(new_store.index, DatabaseIndex), equal_to(True))
        assert_that(
            np.array_equal(new_store.index.values, new_expected), equal_to(True)
        )

        reset_store = new_store.reset_index()
        assert_that(reset_store.index.name, equal_to("index"))
        assert_that(isinstance(reset_store.index, PandasIndex), equal_to(True))
        assert_that(
            np.array_equal(reset_store.index.values, initial_expected), equal_to(True)
        )

    def test_iloc(self) -> None:
        actual_series = self.data_backend.iloc[0]
        assert_that(actual_series.equals(self.test_series0), is_(True))

    def test_loc(self) -> None:
        test_slice = self.data_backend.iloc[[0, 2]]
        actual_series = test_slice.loc[2]
        assert_that(actual_series.equals(self.test_series2), is_(True))

    def test_equals(self) -> None:
        test = DatabaseBackend(ExampleStore, self.db, ExampleStore.data_token)
        assert_that(self.data_backend.equals(test), equal_to(True))

        newBackend = self.data_backend[ExampleStore.a]
        test = DatabaseBackend(ExampleStore, self.db, ExampleStore.data_token)
        assert_that(newBackend.equals(test), equal_to(False))

        test = DatabaseBackend(
            ExampleStore, self.db, ExampleStore.data_token, None, ["a"]
        )
        assert_that(self.data_backend.equals(test[ExampleStore.a]), equal_to(False))

        test = PandasBackend(
            {"a": ["a", "b", "d"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(self.data_backend.equals(test), equal_to(False))

    def test_eq(self) -> None:
        expected = PandasBackend({"a": ["a"], "b": [1], "c": [True]})
        query = self.data_backend["a"] == "a"
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"a": ["a", "c"], "b": [1, 3]})
        expected = PandasBackend({"a": ["a", "c"], "b": [1, 3], "c": [True, True]})
        query = self.data_backend == test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_ne(self) -> None:
        expected = PandasBackend({"a": ["b", "c"], "b": [2, 3], "c": [False, True]})
        query = self.data_backend["a"] != "a"
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"a": ["a", "b", "d"], "b": [2, 2, 3]})
        expected = PandasBackend({"a": ["a", "c"], "b": [1, 3], "c": [True, True]})
        query = self.data_backend != test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_gt(self) -> None:
        expected = PandasBackend({"a": ["c"], "b": [3], "c": [True]})
        query = self.data_backend["b"] > 2
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"b": [2]})
        query = self.data_backend > test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_ge(self) -> None:
        expected = PandasBackend({"a": ["b", "c"], "b": [2, 3], "c": [False, True]})
        query = self.data_backend["b"] >= 2
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"b": [2]})
        query = self.data_backend >= test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_lt(self) -> None:
        expected = PandasBackend({"a": ["a"], "b": [1], "c": [True]})
        query = self.data_backend["b"] < 2
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"b": [2]})
        query = self.data_backend < test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_le(self) -> None:
        expected = PandasBackend({"a": ["a", "b"], "b": [1, 2], "c": [True, False]})
        query = self.data_backend["b"] <= 2
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

        test = PandasBackend({"b": [2]})
        query = self.data_backend <= test
        queried = self.data_backend.query(query)
        assert_that(queried.equals(expected), equal_to(True))

    def test_len(self) -> None:
        assert_that(len(self.data_backend), equal_to(3))

    def test_iter(self) -> None:
        columns = ["a", "b", "c"]
        for actual_col, expected_col in zip(self.data_backend, columns):
            assert_that(actual_col, equal_to(expected_col))

    def test_iterows(self) -> None:
        for i, row in self.data_backend.iterrows():
            iloc_row = self.data_backend.iloc[i]
            assert_that(row.equals(iloc_row), is_(True))

    def test_itertuples(self) -> None:
        for i, a, b, c in self.data_backend.itertuples():
            iloc_row = self.data_backend.iloc[i]
            assert_that(a, equal_to(iloc_row["a"].values[0]))
            assert_that(b, equal_to(iloc_row["b"].values[0]))
            assert_that(c, equal_to(iloc_row["c"].values[0]))

    def test_getitem(self) -> None:
        expected = DatabaseBackend(
            ExampleStore, self.db, ExampleStore.data_token, selected_columns=["b"]
        )
        assert_that(self.data_backend["b"].equals(expected), equal_to(True))

    def test_getitems(self) -> None:
        expected = DatabaseBackend(
            ExampleStore, self.db, ExampleStore.data_token, selected_columns=["a", "b"]
        )
        assert_that(
            self.data_backend.getitems(["a", "b"]).equals(expected), equal_to(True)
        )

    def test_getmask(self) -> None:
        test = self.data_backend.getmask([True, False, True])
        expected = PandasBackend(
            {"a": ["a", "c"], "b": [1, 3], "c": [True, True]},
            index=PandasIndex(PIndex([0, 2], name="index"), []),
        )
        assert_that(test.equals(expected), equal_to(True))

    def test_query(self) -> None:
        query = (ExampleStore.a == "a") | (ExampleStore.b == 3)
        test = self.data_backend.query(query)
        expected = PandasBackend(
            {"a": ["a", "c"], "b": [1, 3], "c": [True, True]},
            index=PandasIndex(PIndex([0, 1], name="index"), []),
        )
        assert_that(test.equals(expected), equal_to(True))

    def test_setitem(self) -> None:
        try:
            self.data_backend["a"] = ["d", "e", "f"]
            fail("Expected exception")
        except NotImplementedError as e:
            assert_that(
                str(e),
                equal_to(
                    "The current version of Tanuki does not support Store to DB writing"
                ),
            )

    def test_append(self) -> None:
        try:
            postfix = PandasBackend({"a": ["d"], "b": [4], "c": [False]})
            self.data_backend.append(postfix, ignore_index=True)
            fail("Expected exception")
        except NotImplementedError as e:
            assert_that(
                str(e),
                equal_to(
                    "The current version of Tanuki does not support Store to DB writing"
                ),
            )

    def test_drop_indices(self) -> None:
        try:
            self.data_backend.drop_indices([1])
            fail("Expected exception")
        except NotImplementedError as e:
            assert_that(
                str(e),
                equal_to(
                    "The current version of Tanuki does not support Store to DB writing"
                ),
            )

    def test_concat(self) -> None:
        try:
            postfix = PandasBackend({"a": ["d"], "b": [4], "c": [False]})
            DatabaseBackend.concat([self.data_backend, postfix], ignore_index=True)
            fail("Expected exception")
        except NotImplementedError as e:
            assert_that(
                str(e),
                equal_to(
                    "The current version of Tanuki does not support Store to DB writing"
                ),
            )

    def test_str(self) -> None:
        expected = "Database Link: raw.test\nActive Columns: ['a', 'b', 'c']"
        assert_that(str(self.data_backend), equal_to(expected))

    def test_repr(self) -> None:
        expected = "Database Link: raw.test\nActive Columns: ['a', 'b', 'c']"
        assert_that(repr(self.data_backend), equal_to(expected))
