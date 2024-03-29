from __future__ import annotations
from datetime import datetime
from helpers.example_metadata import ExampleMetadata

from helpers.example_store import ExampleStore
from typing import cast

from hamcrest import assert_that, equal_to, is_, is_in
from pandas import DataFrame
from pytest import fail

from tanuki.data_store.column import Column
from tanuki.data_store.column_alias import ColumnAlias
from tanuki.data_store.data_store import DataStore
from tanuki.data_store.data_type import Boolean, Int64, String
from tanuki.data_store.index.index import Index


class TestDataStore:
    test_store: ExampleStore

    @classmethod
    def setup_class(cls) -> None:
        cls.metadata = ExampleMetadata(
            test_str="test",
            test_int=123,
            test_float=0.123,
            test_bool=True,
            test_timestamp=datetime.now(),
        )
        cls.test_store = ExampleStore(
            metadata=cls.metadata,
            a=["a", "b", "c"], 
            b=[1, 2, 3], 
            c=[True, False, True]
        )
        cls.test_row0 = ExampleStore(a="a", b=1, c=True, index=0)
        cls.test_row2 = ExampleStore(a="c", b=3, c=True, index=2)

    def test_metadata(self) -> None:
        assert_that(self.test_store.metadata, equal_to(self.metadata))

    def test_get_column_by_name(self) -> None:
        assert_that(self.test_store.a.tolist(), equal_to(["a", "b", "c"]))
        assert_that(self.test_store.b.tolist(), equal_to([1, 2, 3]))
        assert_that(self.test_store.c.tolist(), equal_to([True, False, True]))

    def test_get_column_like_dict(self) -> None:
        assert_that(self.test_store["a"].tolist(), equal_to(["a", "b", "c"]))
        assert_that(self.test_store["b"].tolist(), equal_to([1, 2, 3]))
        assert_that(self.test_store["c"].tolist(), equal_to([True, False, True]))

    def test_missing_column(self) -> None:
        test_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3])
        assert_that(test_store["a"].tolist(), equal_to(["a", "b", "c"]))
        assert_that(test_store["b"].tolist(), equal_to([1, 2, 3]))
        assert_that(test_store.c, equal_to(None))
        assert_that(test_store["c"], equal_to(None))

        try:
            test_store.d
            fail("Expected exception")
        except Exception as e:
            assert_that("Could not match 'd' to ExampleStore column", is_in(str(e)))

        try:
            test_store["d"]
            fail("Expected exception")
        except Exception as e:
            assert_that("Could not match 'd' to ExampleStore column", is_in(str(e)))

    def test_castable_types(self) -> None:
        test_store = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3], c=[1, 0, 1])
        assert_that(test_store["a"].tolist(), equal_to(["a", "b", "c"]))
        assert_that(test_store["b"].tolist(), equal_to([1, 2, 3]))
        assert_that(test_store["c"].tolist(), equal_to([True, False, True]))

    def test_missing_types(self) -> None:
        try:

            class TempStore(DataStore):
                a: Column

            fail("Expected exception")
        except Exception as e:
            assert_that(
                "Failed to find column types for the following columns: ['a']",
                is_in(str(e)),
            )

    def test_invalid_types(self) -> None:
        try:
            ExampleStore(a=["a", "b", "c"], b=["a", "b", "c"], c=[True, False, True])
            fail("Expected exception")

        except Exception as e:
            assert_that("Invalid types provided for: ['b']", is_in(str(e)))

    def test_columns_builder(self) -> None:
        builder = ExampleStore.builder()
        builder["a"] = ["a", "b", "c"]
        builder.append_column("b", [1, 2, 3]).append_column(
            ExampleStore.c, [True, False, True]
        )
        test_store = builder.build()
        assert_that(test_store.a.tolist(), equal_to(["a", "b", "c"]))
        assert_that(test_store.b.tolist(), equal_to([1, 2, 3]))
        assert_that(test_store.c.tolist(), equal_to([True, False, True]))

    def test_rows_builder(self) -> None:
        builder = ExampleStore.builder()
        builder.append_row(a="a", b=1, c=True)
        builder.append_row(a="b", b=2, c=False)
        builder.append_row(a="c", b=3, c=True)
        test_store = builder.build()
        assert_that(test_store.a.tolist(), equal_to(["a", "b", "c"]))
        assert_that(test_store.b.tolist(), equal_to([1, 2, 3]))
        assert_that(test_store.c.tolist(), equal_to([True, False, True]))

    def test_single_row(self) -> None:
        example_row = ExampleStore(a=["a"], b=[1])
        assert_that(example_row.a.values, equal_to(["a"]))
        assert_that(example_row.b.values, equal_to([1]))

        example_row = ExampleStore(a="a", b=1)
        assert_that(example_row.a.values, equal_to(["a"]))
        assert_that(example_row.b.values, equal_to([1]))

        test_slice = self.test_store.iloc[0]
        assert_that(test_slice.a.values, equal_to(["a"]))
        assert_that(test_slice.b.values, equal_to([1]))
        assert_that(test_slice.b.values, equal_to([True]))

    def test_empty_store(self) -> None:
        example_row = ExampleStore(a=[], b=[], c=[])
        assert_that(len(example_row.a), equal_to(0))
        assert_that(len(example_row.b), equal_to(0))
        assert_that(len(example_row.c), equal_to(0))

        example_row = ExampleStore(b=[], c=[])
        assert_that(example_row.a, equal_to(None))
        assert_that(len(example_row.b), equal_to(0))
        assert_that(len(example_row.c), equal_to(0))

        example_row = ExampleStore(c=[])
        assert_that(example_row.a, equal_to(None))
        assert_that(example_row.b, equal_to(None))
        assert_that(len(example_row.c), equal_to(0))

        example_row = ExampleStore()
        assert_that(example_row.a, equal_to(None))
        assert_that(example_row.b, equal_to(None))
        assert_that(example_row.c, equal_to(None))

    def test_get_columns(self) -> None:
        test1 = self.test_store["a", "b"]
        test2 = self.test_store[ExampleStore.a, ExampleStore.b]
        test3 = self.test_store[[ExampleStore.a, ExampleStore.b]]
        expected = ExampleStore(a=["a", "b", "c"], b=[1, 2, 3])
        assert_that(test1.equals(test2), is_(True))
        assert_that(test2.equals(test3), is_(True))
        assert_that(test3.equals(expected), is_(True))

    def test_get_query(self) -> None:
        query = ExampleStore.b > 1
        actual = self.test_store[query]
        expected = ExampleStore(a=["b", "c"], b=[2, 3], c=[False, True], index=[1, 2])
        assert_that(actual.equals(expected), is_(True))

    def test_get_mask(self) -> None:
        mask = cast(Column, self.test_store.b > 1)
        actual = self.test_store[mask.values]
        expected = ExampleStore(a=["b", "c"], b=[2, 3], c=[False, True], index=[1, 2])
        assert_that(actual.equals(expected), is_(True))

    def test_iloc(self) -> None:
        actual_series = self.test_store.iloc[0]
        assert_that(actual_series.equals(self.test_row0), is_(True))

    def test_loc(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        actual_series = test_slice.loc[2]
        assert_that(actual_series.equals(self.test_row2), is_(True))

    def test_get_index(self) -> None:
        assert_that(self.test_store.index.tolist(), equal_to([0, 1, 2]))
        assert_that(self.test_store["index"].tolist(), equal_to([0, 1, 2]))
        assert_that(self.test_store.a_index.tolist(), equal_to(["a", "b", "c"]))
        assert_that(
            self.test_store.ab_index.tolist(), equal_to([("a", 1), ("b", 2), ("c", 3)])
        )
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        assert_that(test_slice["index"].tolist(), equal_to([0, 2]))
        assert_that(test_slice.a_index.tolist(), equal_to(["a", "c"]))
        assert_that(test_slice.ab_index.tolist(), equal_to([("a", 1), ("c", 3)]))

    def test_invalid_index_reference(self) -> None:
        try:
            class TempStore(DataStore):
                a: Column[str]
                index: Index[a]

            fail("Expected exception")
        except Exception as e:
            assert_that(
                "Cannot name index 'index', 'index' is reserved word in datastore",
                is_in(str(e)),
            )

    def test_invalid_index_duplicate_reference(self) -> None:
        try:
            class TempStore(DataStore):
                a: Column[str]
                a_index: Index[a, a]

            fail("Expected exception")
        except Exception as e:
            assert_that(
                "Duplicated columns were attached to index 'a_index': ['a']",
                is_in(str(e)),
            )

    def test_invalid_index_missing_reference(self) -> None:
        try:
            class TempStore(DataStore):
                a: Column[str]

                a_index: Index

            fail("Expected exception")
        except Exception as e:
            assert_that(
                "No columns were attached to index 'a_index'",
                is_in(str(e)),
            )
            
    
    def test_invalid_index_invalid_reference(self) -> None:
        try:
            class TempStore(DataStore):
                a: Column[str]

                b_index: Index[b]

            fail("Expected exception")
        except Exception as e:
            assert_that(
                "Failed to find the following columns from 'b_index' index: {'b'}",
                is_in(str(e)),
            )

    def test_set_index(self) -> None:
        assert_that(self.test_store.index.tolist(), equal_to([0, 1, 2]))

        new_store = self.test_store.set_index(self.test_store.a_index)
        assert_that(new_store.index.tolist(), equal_to(["a", "b", "c"]))

        new_store = self.test_store.set_index(ExampleStore.ab_index)
        assert_that(new_store.index.tolist(), equal_to([("a", 1), ("b", 2), ("c", 3)]))

    def test_reset_index(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))

        test_slice = test_slice.reset_index()
        assert_that(test_slice.index.tolist(), equal_to([0, 1]))

        test_slice = self.test_store.set_index(ExampleStore.ab_index)
        assert_that(test_slice.index.tolist(), equal_to([("a", 1), ("b", 2), ("c", 3)]))

        test_slice = test_slice.reset_index()
        assert_that(test_slice.index.tolist(), equal_to([0, 1, 2]))

    def test_append(self) -> None:
        actual = self.test_store.append(
            ExampleStore(a="d", b=4, c=False), ignore_index=True
        )
        assert_that(actual.a.tolist(), equal_to(["a", "b", "c", "d"]))
        assert_that(actual.b.tolist(), equal_to([1, 2, 3, 4]))
        assert_that(actual.c.tolist(), equal_to([True, False, True, False]))

        expected = ExampleStore(
            a=["a", "b", "c", "d"], b=[1, 2, 3, 4], c=[True, False, True, False]
        )
        assert_that(actual.equals(expected), is_(True))

    def test_concat(self) -> None:
        new_store = ExampleStore(a="d", b=4, c=False)
        actual = ExampleStore.concat([self.test_store, new_store], ignore_index=True)
        assert_that(actual.a.tolist(), equal_to(["a", "b", "c", "d"]))
        assert_that(actual.b.tolist(), equal_to([1, 2, 3, 4]))
        assert_that(actual.c.tolist(), equal_to([True, False, True, False]))

        expected = ExampleStore(
            a=["a", "b", "c", "d"], b=[1, 2, 3, 4], c=[True, False, True, False]
        )
        assert_that(actual.equals(expected), is_(True))

    def test_contains(self) -> None:
        assert_that("a", is_in(self.test_store))
        assert_that(ExampleStore.a, is_in(self.test_store))

    def test_equals(self) -> None:
        assert_that(self.test_store.equals(self.test_store), is_(True))
        assert_that(self.test_store.equals(1), is_(False))

        test = ExampleStore(
            a=["a", "b", "c"], 
            b=[1, 2, 3], 
            c=[True, False, True]
        )
        assert_that(self.test_store.equals(test), is_(False))

    def test_eq(self) -> None:
        result = self.test_store == self.test_store
        assert_that(result.values.all().all(), is_(True))
        result = self.test_store == "a"
        expected = DataFrame(
            {
                "a": [True, False, False],
                "b": [False, False, False],
                "c": [False, False, False],
            }
        )
        assert_that(expected.equals(result), is_(True))

    def test_ne(self) -> None:
        result = self.test_store != self.test_store
        assert_that(result.values.any().any(), is_(False))
        result = self.test_store != "a"
        expected = DataFrame(
            {
                "a": [False, True, True],
                "b": [True, True, True],
                "c": [True, True, True],
            }
        )
        assert_that(expected.equals(result), is_(True))

    def test_gt(self) -> None:
        result = self.test_store > self.test_store
        assert_that(result.values.any().any(), is_(False))
        try:
            self.test_store > 1
            fail("Expected exception")
        except TypeError as e:
            assert_that(
                "'>' not supported between instances of 'str' and 'int'", is_in(str(e))
            )

    def test_ge(self) -> None:
        result = self.test_store >= self.test_store
        assert_that(result.values.all().all(), is_(True))
        try:
            self.test_store >= 1
            fail("Expected exception")
        except TypeError as e:
            assert_that(
                "'>=' not supported between instances of 'str' and 'int'", is_in(str(e))
            )

    def test_lt(self) -> None:
        result = self.test_store < self.test_store
        assert_that(result.values.any().any(), is_(False))
        try:
            self.test_store < 1
            fail("Expected exception")
        except TypeError as e:
            assert_that(
                "'<' not supported between instances of 'str' and 'int'", is_in(str(e))
            )

    def test_le(self) -> None:
        result = self.test_store <= self.test_store
        assert_that(result.values.all().all(), is_(True))
        try:
            self.test_store <= 1
            fail("Expected exception")
        except TypeError as e:
            assert_that(
                "'<=' not supported between instances of 'str' and 'int'", is_in(str(e))
            )

    def test_len(self) -> None:
        assert_that(len(self.test_store), equal_to(3))

    def test_columns(self) -> None:
        columns = ["a", "b", "c"]
        types = [String, Int64, Boolean]
        for actual_column, expected_name, expected_type in zip(
            self.test_store.columns, columns, types
        ):
            assert_that(type(actual_column), equal_to(ColumnAlias))
            assert_that(str(actual_column), equal_to(expected_name))
            assert_that(actual_column.dtype, equal_to(expected_type))

        test_store2 = ExampleStore(b=[1, 2, 3], c=[True, False, True])
        columns = ["b", "c"]
        types = [Int64, Boolean]
        for actual_column, expected_name, expected_type in zip(
            test_store2.columns, columns, types
        ):
            assert_that(type(actual_column), equal_to(ColumnAlias))
            assert_that(str(actual_column), equal_to(expected_name))
            assert_that(actual_column.dtype, equal_to(expected_type))

    def test_indices(self) -> None:
        expected_inds = {"a_index": ["a"], "ab_index": ["a", "b"]}
        assert_that(len(self.test_store.indices), equal_to(len(expected_inds)))
        for actual, expected in zip(self.test_store.indices, expected_inds):
            assert_that(actual.name, equal_to(expected))
            expected_cols = expected_inds[expected]
            assert_that(len(actual.columns), equal_to(len(expected_cols)))
            actual_cols = [str(col) for col in actual.columns]
            assert_that(actual_cols, equal_to(expected_cols))

    def test_iter(self) -> None:
        # Test dataframe
        columns = ["a", "b", "c"]
        types = [String, Int64, Boolean]
        for actual_column, expected_name, expected_type in zip(
            self.test_store, columns, types
        ):
            assert_that(type(actual_column), equal_to(ColumnAlias))
            assert_that(str(actual_column), equal_to(expected_name))
            assert_that(actual_column.dtype, equal_to(expected_type))

        # # Test series
        # test_row = ExampleStore(a="a", b=1, c=True)
        # columns = ["a", "b", "c"]
        # types = [String, Int64, Boolean]
        # for actual_column, expected_name, expected_type in zip(
        #     test_row, columns, types
        # ):
        #     assert_that(type(actual_column), equal_to(ColumnAlias))
        #     assert_that(str(actual_column), equal_to(expected_name))
        #     assert_that(actual_column.dtype, equal_to(expected_type))

    def test_iterows(self) -> None:
        for i, row in self.test_store.iterrows():
            iloc_row = self.test_store.iloc[i]
            assert_that(row.equals(iloc_row), is_(True))

    def test_itertuples(self) -> None:
        for i, a, b, c in self.test_store.itertuples():
            iloc_row = self.test_store.iloc[i]
            assert_that(a, equal_to(iloc_row.a.item()))
            assert_that(b, equal_to(iloc_row.b.item()))
            assert_that(c, equal_to(iloc_row.c.item()))

    def test_str(self) -> None:
        expected = "ExampleStore\n       a  b      c\nindex             \n0      a  1   True\n1      b  2  False\n2      c  3   True"
        assert_that(str(self.test_store), equal_to(expected))

    def test_repr(self) -> None:
        expected = "ExampleStore\n       a  b      c\nindex             \n0      a  1   True\n1      b  2  False\n2      c  3   True"
        assert_that(repr(self.test_store), equal_to(expected))
