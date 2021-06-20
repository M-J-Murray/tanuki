from hamcrest import assert_that, equal_to, is_, is_in
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from src.data_backend.pandas_backend import PandasBackend


class TestPandasBackend:
    @classmethod
    def setup_class(cls) -> None:
        cls.test_frame = PandasBackend(
            DataFrame({"a": ["a", "b", "c"], "b": [1, 2, 3], "c": [True, False, True]})
        )
        cls.test_series0 = PandasBackend(Series({"a": "a", "b": 1, "c": True}))
        cls.test_series2 = PandasBackend(Series({"a": "c", "b": 3, "c": True}))

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
        assert_that(self.test_frame.to_dict("list"), equal_to(frame_expected_dict))
        series_expected_dict = {"a": "a", "b": 1, "c": True}
        assert_that(self.test_series0.to_dict("list"), equal_to(series_expected_dict))

    def test_single_row(self) -> None:
        assert_that(self.test_series0["a"], equal_to("a"))
        assert_that(self.test_series0["b"], equal_to(1))
        assert_that(self.test_series0["c"], equal_to(True))

        example_row = self.test_frame.iloc[0]
        assert_that(example_row["a"], equal_to("a"))
        assert_that(example_row["b"], equal_to(1))
        assert_that(example_row["c"], equal_to(True))

    def test_set_index(self) -> None:
        test_slice = self.test_frame.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.set_index("b")
        assert_that(test_slice.index.tolist(), equal_to([1, 3]))

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

    def test_eq(self) -> None:
        stores_operator_equals = self.test_frame == self.test_frame
        stores_functional_equals = self.test_frame.equals(self.test_frame)
        assert_that(stores_operator_equals.all().all(), is_(True))
        assert_that(stores_functional_equals, is_(True))

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
            assert_that(a, equal_to(iloc_row["a"]))
            assert_that(b, equal_to(iloc_row["b"]))
            assert_that(c, equal_to(iloc_row["c"]))

    def test_str(self) -> None:
        expected = "\n   a  b      c\n0  a  1   True\n1  b  2  False\n2  c  3   True"
        assert_that(str(self.test_frame), equal_to(expected))

    def test_repr(self) -> None:
        expected = "\n   a  b      c\n0  a  1   True\n1  b  2  False\n2  c  3   True"
        assert_that(repr(self.test_frame), equal_to(expected))
