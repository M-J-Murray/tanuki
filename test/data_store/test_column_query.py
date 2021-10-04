from test.helpers.example_store import ExampleStore

from hamcrest import assert_that, equal_to
from pytest import fail

from src.tanuki.data_store.column import Column
from src.tanuki.data_store.data_store import DataStore
from src.tanuki.data_store.query import EqualsQuery


class TestColumnQuery:
    def test_equality_order(self) -> None:
        criteria = ExampleStore.a == 1
        assert_that(type(criteria), equal_to(EqualsQuery))

        criteria = 1 == ExampleStore.a
        assert_that(type(criteria), equal_to(EqualsQuery))
