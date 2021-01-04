from hamcrest import assert_that, equal_to, is_, is_in
from pytest import fail

from src.data_store.column import Column
from src.data_store.column_alias import ColumnAlias
from src.data_store.data_store import DataStore
from src.data_store.data_type import Boolean, Int64, String


class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestDataStore:
    @classmethod
    def setup_class(cls) -> None:
        cls.test_store = ExampleStore(
            a=["a", "b", "c"], b=[1, 2, 3], c=[True, False, True]
        )
        cls.test_row0 = ExampleStore(a="a", b=1, c=True)
        cls.test_row2 = ExampleStore(a="c", b=3, c=True)

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
        assert_that(example_row.a, equal_to("a"))
        assert_that(example_row.b, equal_to(1))

        example_row = ExampleStore(a="a", b=1)
        assert_that(example_row.a, equal_to("a"))
        assert_that(example_row.b, equal_to(1))

        test_slice = self.test_store.iloc[0]
        assert_that(test_slice.a, equal_to("a"))
        assert_that(test_slice.b, equal_to(1))
        assert_that(test_slice.b, equal_to(True))

    def test_iloc(self) -> None:
        actual_series = self.test_store.iloc[0]
        assert_that(actual_series.equals(self.test_row0), is_(True))

    def test_loc(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        actual_series = test_slice.loc[2]
        assert_that(actual_series.equals(self.test_row2), is_(True))

    def test_set_index(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.set_index("b")
        assert_that(test_slice.index.tolist(), equal_to([1, 3]))

    def test_get_index(self) -> None:
        assert_that(self.test_store.index.tolist(), equal_to([0, 1, 2]))
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))

    def test_reset_index(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.reset_index(drop=True)
        assert_that(test_slice.index.tolist(), equal_to([0, 1]))

    def test_contains(self) -> None:
        assert_that("a", is_in(self.test_store))
        assert_that(ExampleStore.a, is_in(self.test_store))

    def test_eq(self) -> None:
        stores_operator_equals = self.test_store == self.test_store
        stores_functional_equals = self.test_store.equals(self.test_store)
        assert_that(stores_operator_equals.all().all(), is_(True))
        assert_that(stores_functional_equals, is_(True))

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

    def test_iter(self) -> None:
        columns = ["a", "b", "c"]
        types = [String, Int64, Boolean]
        for actual_column, expected_name, expected_type in zip(
            self.test_store, columns, types
        ):
            assert_that(type(actual_column), equal_to(ColumnAlias))
            assert_that(str(actual_column), equal_to(expected_name))
            assert_that(actual_column.dtype, equal_to(expected_type))

    def test_iterows(self) -> None:
        for i, row in self.test_store.iterrows():
            iloc_row = self.test_store.iloc[i]
            assert_that(row.equals(iloc_row), is_(True))

    def test_itertuples(self) -> None:
        for i, a, b, c in self.test_store.itertuples():
            iloc_row = self.test_store.iloc[i]
            assert_that(a, equal_to(iloc_row.a))
            assert_that(b, equal_to(iloc_row.b))
            assert_that(c, equal_to(iloc_row.c))

    def test_str(self) -> None:
        expected = "ExampleStore\n   a  b      c\n0  a  1   True\n1  b  2  False\n2  c  3   True"
        assert_that(str(self.test_store), equal_to(expected))

    def test_repr(self) -> None:
        expected = "ExampleStore\n   a  b      c\n0  a  1   True\n1  b  2  False\n2  c  3   True"
        assert_that(repr(self.test_store), equal_to(expected))
