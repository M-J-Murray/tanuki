from hamcrest import assert_that, equal_to
from pytest import fail

from src.data.data_store.column import Column
from src.data.data_store.data_store import DataStore


class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]


class TestDataStore:
    def test_valid_init(self) -> None:
        test_store = ExampleStore({
            ExampleStore.a: ["a", "b", "c"],
            ExampleStore.b: [1, 2, 3],
            ExampleStore.c: [True, False, True],
        })
        assert_that(test_store.a, equal_to(["a", "b", "c"]))
        assert_that(test_store.b, equal_to([1, 2, 3]))
        assert_that(test_store.c, equal_to([1, 2, 3]))

    def test_get_column_by_name(self) -> None:
        pass

    def test_get_column_like_dict(self) -> None:
        pass

    def test_missing_columns(self) -> None:
        fail("Not Implemented")

    def test_castable_types(self) -> None:
        fail("Not Implemented")

    def test_invalid_types(self) -> None:
        fail("Not Implemented")

    def test_builder(self) -> None:
        fail("Not Implemented")
