from _thread import start_new_thread

from hamcrest import assert_that, equal_to, is_in
from hamcrest.core.core.is_ import is_
from pandas.core.frame import DataFrame
from pytest import fail

from src.data_store.column import Column


class TestColumn:
    def test_type_casting(self) -> None:
        data = [1, 2, 3]
        column = Column("test", data)
        print(column)
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[int]("test", [1, 2, 3])
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[int]("test", [1.23, 2.23, 3.23])
        assert_that(column.tolist(), equal_to([1, 2, 3]))

        column = Column[float]("test", [1.23, 2.23, 3.23])
        assert_that(column.tolist(), equal_to([1.23, 2.23, 3.23]))

        column = Column[str]("test", [1, 2, 3])
        assert_that(column.tolist(), equal_to(["1", "2", "3"]))

        column = Column[bool]("test", [0, 1, 2])
        assert_that(column.tolist(), equal_to([False, True, True]))

        column: Column[bool] = Column("test", [0, 1, 2])
        assert_that(column.tolist(), equal_to([0, 1, 2]))

        try:
            Column[float]("test", ["a", "b", "c"])
            fail("Expected cast exception")
        except Exception as e:
            assert_that("Failed to cast 'String' to 'Float64'", is_in(str(e)))

    def test_multi_threaded(self) -> None:
        data = [1, 2, 3]
        thread_running = True

        def assign_types():
            while thread_running:
                Column[bool]("test", data)

        start_new_thread(assign_types, ((),))
        for _ in range(1000):
            column = Column("test", data)
            assert_that(column.tolist(), equal_to(data))
        thread_running = False

    def test_first(self) -> None:
        column = Column[int]("test", [1, 2, 3])
        assert_that(column.first().tolist(), equal_to([1]))

    def test_equals(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(column1.equals(column1), equal_to(True))
        assert_that(column1.equals(column2), equal_to(False))
        assert_that(column1.equals(1), equal_to(False))

    def test_eq(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [True, True, True]}).equals(column1 == column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [False, True, False]}).equals(column1 == column2), is_(True)
        )
        assert_that(DataFrame({"test": [False, True, False]}).equals(column1 == 2), is_(True))

    def test_ne(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [False, False, False]}).equals(column1 != column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [True, False, True]}).equals(column1 != column2), is_(True)
        )
        assert_that(DataFrame({"test": [True, True, False]}).equals(column1 != 3), is_(True))

    def test_gt(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [False, False, False]}).equals(column1 > column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [True, False, True]}).equals(column2 > column1), is_(True)
        )
        assert_that(DataFrame({"test": [True, False, True]}).equals(column2 > 3), is_(True))

    def test_ge(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [True, True, True]}).equals(column1 >= column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [False, True, False]}).equals(column1 >= column2), is_(True)
        )
        assert_that(DataFrame({"test": [False, False, True]}).equals(column1 >= 3), is_(True))

    def test_lt(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [False, False, False]}).equals(column1 < column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [True, False, True]}).equals(column1 < column2), is_(True)
        )
        assert_that(DataFrame({"test": [False, True, False]}).equals(column2 < 3), is_(True))

    def test_le(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        column2 = Column[int]("test", [4, 2, 5])
        assert_that(
            DataFrame({"test": [True, True, True]}).equals(column1 <= column1), is_(True)
        )
        assert_that(
            DataFrame({"test": [True, True, True]}).equals(column1 <= column2), is_(True)
        )
        assert_that(DataFrame({"test": [False, True, False]}).equals(column2 <= 3), is_(True))

    def test_getitem(self) -> None:
        column1 = Column[int]("test", [1, 2, 3])
        assert_that(column1[1].item(), equal_to(2))
        column2 = Column[int]("test", [1, 2, 3], index=[1, 2, 3])
        assert_that(column2[1].item(), equal_to(1))

    def test_iter(self) -> None:
        column = Column[int]("test", [1, 2, 3])
        expected = [1, 2, 3]
        for (_, a), e in zip(column,expected):
            assert_that(a, equal_to(e))

    def test_str(self) -> None:
        column = Column[int]("test", [1, 2, 3])
        assert_that(str(column), equal_to("       test\nindex      \n0         1\n1         2\n2         3"))

    def test_repr(self) -> None:
        column = Column[int]("test", [1, 2, 3])
        assert_that(repr(column), equal_to("       test\nindex      \n0         1\n1         2\n2         3"))
