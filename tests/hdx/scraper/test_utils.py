from datetime import datetime

from hdx.data.dataset import Dataset

from hdx.scraper.utilities import (
    get_isodate_from_dataset_date,
    string_params_to_dict,
)


class TestUtils:
    def test_string_params_to_dict(self):
        result = string_params_to_dict("a: 123, b: 345")
        assert result == {"a": "123", "b": "345"}
        result = string_params_to_dict("a:123,b:345")
        assert result == {"a": "123", "b": "345"}

    def test_get_isodate_from_dataset_date(self, configuration):
        dataset = Dataset(
            {
                "dataset_date": "[2022-01-11T02:24:08.241 TO 2022-01-11T02:24:08.241]"
            }
        )
        result = get_isodate_from_dataset_date(dataset, datetime.now())
        assert result == "2022-01-11"
