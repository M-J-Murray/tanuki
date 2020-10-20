from hamcrest import assert_that, equal_to, is_in
from pytest import fail

from src.data_store.column import Column

from _thread import start_new_thread

class TestColumn:
    def test_type_casting(self) -> None:
        data = [1, 2, 3]
        column = Column(data)
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[int]([1, 2, 3])
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[int]([1.23, 2.23, 3.23])
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[float]([1.23, 2.23, 3.23])
        assert_that(column.tolist(), equal_to([1.23, 2.23, 3.23]))

        column = Column[str]([1, 2, 3])
        assert_that(column.tolist(), equal_to(["1", "2", "3"]))

        column = Column[bool]([0, 1, 2])
        assert_that(column.tolist(), equal_to([False, True, True]))

        column: Column[bool] = Column([0, 1, 2])
        assert_that(column.tolist(), equal_to([0, 1, 2]))

        try:
            Column[float](["a", "b", "c"])
            fail("Expected cast exception")
        except Exception as e:
            assert_that("Failed to cast 'String' to 'Float64'", is_in(str(e)))
        

    def test_multi_threaded(self) -> None:
        data = [1, 2, 3]
        thread_running = True

        def assign_types():
            while thread_running:
                Column[bool](data)

        start_new_thread(assign_types, ((),))
        for _ in range(1000):
            column = Column(data)
            assert_that(column.tolist(), equal_to(data))
        thread_running = False