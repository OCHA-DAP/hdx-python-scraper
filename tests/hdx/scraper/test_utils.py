from datetime import datetime

from hdx.data.dataset import Dataset

from hdx.scraper.utilities import get_isodate_from_dataset_date


class TestUtils:
    def test_get_isodate_from_dataset_date(self, configuration):
        dataset = Dataset(
            {
                "dataset_date": "[2022-01-11T02:24:08.241 TO 2022-01-11T02:24:08.241]"
            }
        )
        result = get_isodate_from_dataset_date(dataset, datetime.now())
        assert result == "2022-01-11"
