
from pytest import fail
from hamcrest import assert_that, equal_to

from src.data_store.data_store import DataStore
from src.data_store.column import Column, ColumnQuery

class ExampleStore(DataStore):
    a: Column[str]
    b: Column[int]
    c: Column[bool]

class TestColumnQuery:
    
    def test_equality_order(self) -> None:
        criteria = ExampleStore.a == 1
        assert_that(type(criteria), equal_to(ColumnQuery))

        criteria = 1 == ExampleStore.a
        assert_that(type(criteria), equal_to(ColumnQuery))
    