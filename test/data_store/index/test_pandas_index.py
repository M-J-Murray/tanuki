import numpy as np
from pandas import Index as PIndex
from precisely import assert_that, equal_to
from pytest import fail

from src.data_store.index.pandas_index import PandasIndex


class TestPandasIndex:

    def setup_method(self):
        self.index = PandasIndex(PIndex(np.arange(0, 3), name="index"), ["a", "b"])
    
    def test_name(self):
        assert_that(self.index.name, equal_to("index"))

    def test_columns(self):
        assert_that(self.index.columns, equal_to(["a", "b"]))

    def test_to_pandas(self) -> None:
        assert_that(self.index.to_pandas().equals(self.index), equal_to(True))

    def test_getitem(self):
        expected = PandasIndex(PIndex([1], name="index"), ["a", "b"])
        assert_that(self.index[1], equal_to(1))
        assert_that(self.index[[1]].equals(expected), equal_to(True))

    def test_values(self):
        assert_that(np.array_equal(self.index.values, np.array([0, 1, 2])), equal_to(True))

    def test_tolist(self):
        assert_that(self.index.tolist(), equal_to([0, 1, 2]))

    def test_equals(self):
        test = PandasIndex(PIndex([0, 1, 2], name="index"), ["a", "b"])
        assert_that(self.index.equals(test), equal_to(True))

        test = PandasIndex(PIndex([0, 1], name="index"), ["a", "b"])
        assert_that(self.index.equals(test), equal_to(False))

        test = PandasIndex(PIndex([0, 1, 2], name="index2"), ["a", "b"])
        assert_that(self.index.equals(test), equal_to(False))

        test = PandasIndex(PIndex([0, 1, 2], name="index"), ["a"])
        assert_that(self.index.equals(test), equal_to(False))

    def test_eq(self):
        expected = np.array([False, True, False])
        actual = self.index == 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_ne(self):
        expected = np.array([True, False, True])
        actual = self.index != 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_gt(self):
        expected = np.array([False, False, True])
        actual = self.index > 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_ge(self):
        expected = np.array([False, True, True])
        actual = self.index >= 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_lt(self):
        expected = np.array([True, False, False])
        actual = self.index < 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_le(self):
        expected = np.array([True, True, False])
        actual = self.index <= 1
        assert_that(np.array_equal(actual, expected), equal_to(True))

    def test_len(self) -> int:
        assert_that(len(self.index), equal_to(3))

    def test_str(self) -> str:
        assert_that(str(self.index), equal_to("Int64Index([0, 1, 2], dtype='int64', name='index')"))

    def test_repr(self) -> str:
        assert_that(repr(self.index), equal_to("Int64Index([0, 1, 2], dtype='int64', name='index')"))
    