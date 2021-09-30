from test.helpers.example_store import ExampleStore

from hamcrest import assert_that, equal_to, is_, is_in
import numpy as np
from pandas import Index as PIndex
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_backend.pandas_backend import PandasBackend
from src.data_store.data_type import Boolean, Int64, Object
from src.data_store.index.pandas_index import PandasIndex


class TestPandasBackend:
    @classmethod
    def setup_class(cls) -> None:
        cls.test_frame = PandasBackend(
            DataFrame({"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [True, False, True]})
        )
        cls.test_series0 = PandasBackend(Series({"a": "a", "b": 1, "c": True}), index=[0])
        cls.test_series2 = PandasBackend(Series({"a": "c", "b": 3, "c": True}), index=[2])

    def test_iloc(self) -> None:
        actual_series = self.test_frame.iloc[0]
        assert_that(actual_series.equals(self.test_series0), is_(True))

    def test_loc(self) -> None:
        test_slice = self.test_frame.iloc[[0, 2]]
        actual_series = test_slice.loc[2]
        assert_that(actual_series.equals(self.test_series2), is_(True))

    def test_to_dict(self) -> None:
        frame_expected_dict = {
            "a": ["a", "b", "c"],
            "b": [1, 2, 3],
            "c": [True, False, True],
        }
        assert_that(self.test_frame.to_dict(), equal_to(frame_expected_dict))
        series_expected_dict = {"a": ["a"], "b": [1], "c": [True]}
        assert_that(self.test_series0.to_dict(), equal_to(series_expected_dict))

    def test_single_row(self) -> None:
        assert_that(self.test_series0["a"].values[0], equal_to("a"))
        assert_that(self.test_series0["b"].values[0], equal_to(1))
        assert_that(self.test_series0["c"].values[0], equal_to(True))

        example_row = self.test_frame.iloc[0]
        assert_that(example_row["a"].values[0], equal_to("a"))
        assert_that(example_row["b"].values[0], equal_to(1))
        assert_that(example_row["c"].values[0], equal_to(True))

    def test_set_index(self) -> None:
        test_slice = self.test_frame.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.set_index(ExampleStore.a_index)
        assert_that(test_slice.index.tolist(), equal_to(["a", "c"]))

    def test_get_index(self) -> None:
        assert_that(self.test_frame.index.tolist(), equal_to([0, 1, 2]))
        test_slice = self.test_frame.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))

    def test_reset_index(self) -> None:
        test_slice = self.test_frame.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.reset_index(drop=True)
        assert_that(test_slice.index.tolist(), equal_to([0, 1]))

    def test_contains(self) -> None:
        assert_that("a", is_in(self.test_frame))

    def test_len(self) -> None:
        assert_that(len(self.test_frame), equal_to(3))

    def test_columns(self) -> None:
        assert_that(self.test_frame.columns, equal_to(["a", "b", "c"]))
        assert_that(self.test_series0.columns, equal_to(["a", "b", "c"]))

    def test_iter(self) -> None:
        columns = ["a", "b", "c"]
        for actual_col, expected_col in zip(self.test_frame, columns):
            assert_that(actual_col, equal_to(expected_col))

    def test_iterows(self) -> None:
        for i, row in self.test_frame.iterrows():
            iloc_row = self.test_frame.iloc[i]
            assert_that(row.equals(iloc_row), is_(True))

    def test_itertuples(self) -> None:
        for i, a, b, c in self.test_frame.itertuples():
            iloc_row = self.test_frame.iloc[i]
            assert_that(a, equal_to(iloc_row["a"].values[0]))
            assert_that(b, equal_to(iloc_row["b"].values[0]))
            assert_that(c, equal_to(iloc_row["c"].values[0]))

    def test_str(self) -> None:
        expected = "       a  b      c\nindex             \n0      a  1   True\n1      b  2  False\n2      c  3   True"
        assert_that(str(self.test_frame), equal_to(expected))

    def test_repr(self) -> None:
        expected = "       a  b      c\nindex             \n0      a  1   True\n1      b  2  False\n2      c  3   True"
        assert_that(repr(self.test_frame), equal_to(expected))

    def test_is_link(self) -> None:
        assert_that(self.test_frame.is_link(), equal_to(False))

    def test_link_token(self) -> None:
        assert_that(self.test_frame.link_token(), equal_to(None))

    def test_to_pandas(self) -> None:
        expected = DataFrame(
            {"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(self.test_frame.to_pandas().equals(expected), equal_to(True))

    def test_values(self) -> None:
        expected = DataFrame(
            {"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(
            np.array_equal(self.test_frame.values, expected.values), equal_to(True)
        )

    def test_dtypes(self) -> None:
        expected = {"a": Object, "b": Int64, "c": Boolean}
        assert_that(self.test_frame.dtypes, equal_to(expected))

    def test_cast_columns(self) -> None:
        expected = DataFrame({"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [1, 0, 1]})
        actual = self.test_frame.cast_columns({"c": int})
        assert_that(np.array_equal(actual.values, expected.values), equal_to(True))

    def test_index(self) -> None:
        expected = PandasIndex(PIndex([0, 1, 2]), [])
        assert_that(self.test_frame.index.equals(expected), equal_to(True))

        new_frame = self.test_frame.set_index(ExampleStore.ab_index)
        expected = PandasIndex(PIndex([("a", 1), ("b", 2), ("c", 3)]), ["a", "b"])
        assert_that(new_frame.index.equals(expected), equal_to(True))

    def test_index_name(self) -> None:
        assert_that(self.test_frame.index_name, equal_to("index"))
        new_frame = self.test_frame.set_index(ExampleStore.ab_index)
        assert_that(new_frame.index_name, equal_to("ab_index"))

    def test_equals(self) -> None:
        test = PandasBackend(
            {"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(self.test_frame.equals(test), equal_to(True))

        test = PandasBackend(
            {"a": ["a", "b", "d"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(self.test_frame.equals(test), equal_to(False))

    def test_eq(self) -> None:
        test = PandasBackend(
            {"a": ["d", "b", "c"], "b": [1, 4, 3], "c": [True, False, False]}
        )
        expected = DataFrame(
            {
                "a": [False, True, True],
                "b": [True, False, True],
                "c": [True, True, False],
            }
        )
        assert_that((self.test_frame == test).equals(expected), equal_to(True))

    def test_ne(self) -> None:
        test = PandasBackend(
            {"a": ["d", "b", "c"], "b": [1, 4, 3], "c": [True, False, False]}
        )
        expected = DataFrame(
            {
                "a": [True, False, False],
                "b": [False, True, False],
                "c": [False, False, True],
            }
        )
        assert_that((self.test_frame != test).equals(expected), equal_to(True))

    def test_gt(self) -> None:
        sample = ExampleStore(b=[1, 2, 3])
        test = ExampleStore(b=[1, 1, 3])
        expected = DataFrame({"b": [False, True, False]})
        assert_that((sample > test).equals(expected), equal_to(True))

    def test_ge(self) -> None:
        sample = ExampleStore(b=[0, 2, 3])
        test = ExampleStore(b=[1, 1, 3])
        expected = DataFrame({"b": [False, True, True]})
        assert_that((sample >= test).equals(expected), equal_to(True))

    def test_lt(self) -> None:
        sample = ExampleStore(b=[0, 2, 3])
        test = ExampleStore(b=[1, 1, 3])
        expected = DataFrame({"b": [True, False, False]})
        assert_that((sample < test).equals(expected), equal_to(True))

    def test_le(self) -> None:
        sample = ExampleStore(b=[0, 2, 3])
        test = ExampleStore(b=[1, 1, 3])
        expected = DataFrame({"b": [True, False, True]})
        assert_that((sample <= test).equals(expected), equal_to(True))

    def test_getitem(self) -> None:
        expected = PandasBackend({"b": [1, 2, 3]})
        assert_that(self.test_frame["b"].equals(expected), equal_to(True))

    def test_getitems(self) -> None:
        expected = PandasBackend({"a": ["a", "b", "c"], "b": [1, 2, 3]})
        assert_that(
            self.test_frame.getitems(["a", "b"]).equals(expected), equal_to(True)
        )

    def test_getmask(self) -> None:
        test = self.test_frame.getmask([True, False, True])
        expected = PandasBackend(
            {"a": ["a", "c"], "b": [1, 3], "c": [True, True]},
            index=PandasIndex(PIndex([0, 2]), []),
        )
        repr(expected)
        assert_that(test.equals(expected), equal_to(True))

    def test_query(self) -> None:
        query = (ExampleStore.a == "a") | (ExampleStore.b == 3)
        test = self.test_frame.query(query)
        expected = PandasBackend(
            {"a": ["a", "c"], "b": [1, 3], "c": [True, True]},
            index=PandasIndex(PIndex([0, 2]), []),
        )
        assert_that(test.equals(expected), equal_to(True))

    def test_setitem(self) -> None:
        self.test_frame["a"] = ["d", "e", "f"]
        expected = PandasBackend(
            {"a": ["d", "e", "f"], "b": [1, 2, 3], "c": [True, False, True]}
        )
        assert_that(self.test_frame.equals(expected), equal_to(True))

    def test_append(self) -> None:
        postfix = PandasBackend({"a": ["d"], "b": [4], "c": [False]})
        new_frame = self.test_frame.append(postfix, ignore_index=True)
        expected = PandasBackend(
            {
                "a": ["a", "b", "c", "d"],
                "b": [1, 2, 3, 4],
                "c": [True, False, True, False],
            }
        )
        assert_that(new_frame.equals(expected), equal_to(True))

    def test_drop_indices(self) -> None:
        new_frame = self.test_frame.drop_indices([1])
        expected = PandasBackend(
            {"a": ["a", "c"], "b": [1, 3], "c": [True, True]},
            index=PandasIndex(PIndex([0, 2]), []),
        )
        assert_that(new_frame.equals(expected), equal_to(True))

    def test_concat(self) -> None:
        postfix = PandasBackend({"a": ["d"], "b": [4], "c": [False]})
        new_frame = PandasBackend.concat([self.test_frame, postfix], ignore_index=True)
        expected = PandasBackend(
            {
                "a": ["a", "b", "c", "d"],
                "b": [1, 2, 3, 4],
                "c": [True, False, True, False],
            }
        )
        assert_that(new_frame.equals(expected), equal_to(True))
