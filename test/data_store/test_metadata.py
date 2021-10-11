from datetime import datetime

from precisely import assert_that, equal_to

from helpers.example_metadata import ExampleMetadata


class TestMetadata:
    def test_to_from_dict(self) -> None:
        timestamp = datetime.now()
        metadata = ExampleMetadata(
            test_str="test",
            test_int=123,
            test_float=0.123,
            test_bool=True,
            test_timestamp=datetime.now(),
        )
        expected_dict = {
            "test_str": "a",
            "test_int": 1,
            "test_float": 0.123,
            "test_bool": False,
            "test_timestamp": str(timestamp),
        }
        actual_dict = metadata.to_dict()
        assert_that(actual_dict, equal_to(expected_dict))

        actual_data = ExampleMetadata.from_dict(actual_dict)
        assert_that(metadata, equal_to(actual_data))
