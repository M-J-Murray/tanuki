from hamcrest import assert_that, equal_to
from pytest import fail

from src.data_store.column import Column
from src.data_store.data_store import DataStore

# TODO: look at removing subclass generic reference
class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestDataStore:
    @classmethod
    def setup_class(cls) -> None:
        cls.test_store = ExampleStore(
            {
                ExampleStore.a: ["a", "b", "c"],
                ExampleStore.b: [1, 2, 3],
                ExampleStore.c: [True, False, True],
            }
        )

    def test_get_column_by_name(self) -> None:
        assert_that(self.test_store.a.tolist(), equal_to(["a", "b", "c"]))
        assert_that(self.test_store.b.tolist(), equal_to([1, 2, 3]))
        assert_that(self.test_store.c.tolist(), equal_to([True, False, True]))

    def test_get_column_like_dict(self) -> None:
        assert_that(self.test_store["a"].tolist(), equal_to(["a", "b", "c"]))
        assert_that(self.test_store["b"].tolist(), equal_to([1, 2, 3]))
        assert_that(self.test_store["c"].tolist(), equal_to([True, False, True]))

    def test_missing_columns(self) -> None:
        fail("Not Implemented")

    def test_castable_types(self) -> None:
        fail("Not Implemented")

    def test_missing_types(self) -> None:
        fail("Not Implemented")

    def test_invalid_types(self) -> None:
        fail("Not Implemented")

    def test_builder(self) -> None:
        fail("Not Implemented")

    def test_reset_index(self) -> None:
        fail("Not Implemented")

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
