from datetime import datetime

import pytest

from hdx.scraper.utilities.reader import Read


class TestReaders:
    def test_read(self, configuration):
        url = Read.get_url("http://{{var}}", var="hello")
        assert url == "http://hello"
        datasetinfo = {
            "name": "test",
            "dataset": "sahel-humanitarian-needs-overview",
            "format": "csv",
        }
        reader = Read.get_reader()
        headers, iterator = reader.read(datasetinfo, file_type="csv")
        assert headers == [
            "Country",
            "nutrition",
            "Affected 2017",
            "In Need 2017",
            "Targeted 2017",
            "% targeted",
        ]
        assert next(iterator) == {
            "Country": "#country",
            "nutrition": "#sector?",
            "Affected 2017": "#affected",
            "In Need 2017": "#inneed",
            "Targeted 2017": "#targeted",
            "% targeted": "#targeted+percentage",
        }
        assert next(iterator) == {
            "Country": "Burkina Faso",
            "nutrition": "MAM",
            "Affected 2017": "433,412",
            "In Need 2017": "433,412",
            "Targeted 2017": "             _",
            "% targeted": "0",
        }
        date = datetime(2016, 9, 1, 23, 59, 59)
        assert datasetinfo == {
            "name": "test",
            "dataset": "sahel-humanitarian-needs-overview",
            "format": "csv",
            "headers": 1,
            "source_date": date,
            "source": "Multiple organisations",
            "source_url": "https://data.humdata.org/dataset/sahel-humanitarian-needs-overview",
            "url": "https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/2527ac5b-66fe-46f0-8b9b-7086d2c4ddd3/download/hno-2017-sahel-nutrition.csv",
        }
        datasetinfo = {
            "name": "test",
            "dataset": "sahel-humanitarian-needs-overview",
            "resource": "HNO-2017-Sahel- People in need.xlsx",
            "format": "xlsx",
            "sheet": 1,
        }
        headers, iterator = reader.read(datasetinfo)
        assert headers == [
            "Country",
            "Sector",
            "People in need",
            "Total population",
        ]
        assert next(iterator) == {
            "Country": "#country",
            "Sector": "#sector",
            "People in need": "#inneed",
            "Total population": "#total",
        }
        assert next(iterator) == {
            "Country": "Mali",
            "Sector": "Shelter/NFI",
            "People in need": 317000,
            "Total population": 100000,
        }
        assert datasetinfo == {
            "name": "test",
            "dataset": "sahel-humanitarian-needs-overview",
            "resource": "HNO-2017-Sahel- People in need.xlsx",
            "format": "xlsx",
            "sheet": 1,
            "headers": 1,
            "source_date": date,
            "source": "Multiple organisations",
            "source_url": "https://data.humdata.org/dataset/sahel-humanitarian-needs-overview",
            "url": "https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/d9248be4-7bfb-4a81-a7aa-c035dcb737a2/download/hno-2017-sahel-people-in-need.xlsx",
        }
        with pytest.raises(ValueError):
            datasetinfo = {"name": "test", "format": "unknown"}
            reader.read(datasetinfo)
        with pytest.raises(ValueError):
            datasetinfo = {
                "name": "test",
                "dataset": "sahel-humanitarian-needs-overview",
                "format": "json",
            }
            reader.read(datasetinfo)
