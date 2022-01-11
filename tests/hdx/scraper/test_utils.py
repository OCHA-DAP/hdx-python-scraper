from datetime import datetime

from hdx.data.dataset import Dataset

from hdx.scraper.utils import (
    get_isodate_from_dataset_date,
    get_sources_from_datasetinfo,
)


class TestUtils:
    def test_get_isodate_from_dataset_date(self, configuration):
        dataset = Dataset(
            {
                "dataset_date": "[2022-01-11T02:24:08.241 TO 2022-01-11T02:24:08.241]"
            }
        )
        result = get_isodate_from_dataset_date(dataset, datetime.now())
        assert result == "2022-01-11"

    def test_get_sources_from_datasetinfo(self):
        datasetinfo = {
            "format": "csv",
            "dataset": "global-school-closures-covid19",
            "resource": "School closure evolution",
            "headers": 1,
            "url": "https://data.humdata.org/dataset/6a41be98-75b9-4365-9ea3-e33d0dd2668b/resource/3cc9837d-d610-4d3c-91ce-bd24b77b62ba/download/full-dataset-30-nov.csv",
            "date": datetime(2021, 1, 31, 23, 59, 59),
            "source": "UNESCO",
            "source_url": "https://data.humdata.org/dataset/global-school-closures-covid19",
        }
        results = get_sources_from_datasetinfo(
            datasetinfo, ["#status+country+closed"]
        )
        assert results == [
            (
                "#status+country+closed",
                "2021-01-31",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            )
        ]
