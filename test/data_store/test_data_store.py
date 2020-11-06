from hamcrest import assert_that, equal_to, is_in
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore


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

    def test_builder(self) -> None:
        builder = ExampleStore.builder()
        builder["a"] = ["a", "b", "c"]
        builder.append("b", [1, 2, 3]).append(ExampleStore.c, [True, False, True])
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

    def test_reset_index(self) -> None:
        test_slice = self.test_store.iloc[[0, 2]]
        assert_that(test_slice.index.tolist(), equal_to([0, 2]))
        test_slice = test_slice.reset_index(drop=True)
        assert_that(test_slice.index.tolist(), equal_to([0, 1]))

    def test_contains(self, key) -> None:
        fail("Not Implemented")

    def test_eq(self, other) -> None:
        fail("Not Implemented")

    def test_len(self) -> None:
        fail("Not Implemented")

    def test_iter(self) -> None:
        fail("Not Implemented")

    def test_str(self) -> None:
        fail("Not Implemented")

    def test_repr(self) -> None:
        fail("Not Implemented")
