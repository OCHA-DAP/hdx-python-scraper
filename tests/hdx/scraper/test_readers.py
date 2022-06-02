from datetime import datetime

import pytest
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from hdx.scraper.utilities.reader import Read


class TestReaders:
    def test_clone(self):
        with Download(user_agent="test") as downloader:
            with Read(
                downloader,
                "fallback_dir",
                "saved_dir",
                "temp_dir",
                save=False,
                use_saved=False,
                prefix="population",
                today=parse_date("2021-02-01"),
            ) as reader:
                clone_reader = reader.clone(downloader)
                for property, value in vars(reader).items():
                    if property == "downloader":
                        continue
                    assert getattr(clone_reader, property) == value

    def test_read_dataset(self, configuration, monkeypatch):
        def test_read_from_hdx(dataset_name):
            if dataset_name == "None":
                return None
            dataset = Dataset({"name": dataset_name})
            resource = Resource(
                {
                    "name": "test",
                    "url": "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/edit#gid=1088874596",
                }
            )
            resource.set_file_type("csv")
            dataset.add_update_resource(resource)
            return dataset

        with temp_dir("TestReader") as temp_folder:
            with Download(user_agent="test") as downloader:
                munged_url = "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/export?format=csv&gid=1088874596"
                with Read(
                    downloader,
                    temp_folder,
                    temp_folder,
                    temp_folder,
                    save=True,
                    use_saved=False,
                    prefix="test",
                    today=parse_date("2021-02-01"),
                ) as reader:
                    monkeypatch.setattr(
                        Dataset, "read_from_hdx", test_read_from_hdx
                    )
                    dataset_name = "None"
                    dataset = reader.read_dataset(dataset_name)
                    assert dataset is None
                    dataset_name = "Test Dataset"
                    dataset = reader.read_dataset(dataset_name)
                    assert dataset["name"] == dataset_name
                    assert dataset.get_resource()["url"] == munged_url
                    monkeypatch.delattr(Dataset, "read_from_hdx")
                with Read(
                    downloader,
                    temp_folder,
                    temp_folder,
                    temp_folder,
                    save=False,
                    use_saved=True,
                    prefix="test",
                    today=parse_date("2021-02-01"),
                ) as reader:
                    dataset_name = "None"
                    dataset = reader.read_dataset(dataset_name)
                    assert dataset is None
                    dataset_name = "Test Dataset"
                    dataset = reader.read_dataset(dataset_name)
                    assert dataset["name"] == dataset_name
                    assert dataset.get_resource()["url"] == munged_url

    def test_read_hxl_resource(self, fixtures):
        with temp_dir("TestReader") as temp_folder:
            with Download(user_agent="test") as downloader:
                with Read(
                    downloader,
                    temp_folder,
                    fixtures,
                    temp_folder,
                    save=False,
                    use_saved=True,
                    prefix="whowhatwhere",
                    today=parse_date("2021-02-01"),
                ) as reader:
                    resource = Resource(
                        {
                            "name": "3w data",
                            "url": "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/edit#gid=1088874596",
                        }
                    )
                    resource.set_file_type("csv")
                    data = reader.read_hxl_resource("AFG", resource, "3w data")
                    assert len(data.headers) == 15
                    data = reader.read_hxl_resource(
                        "notags", resource, "3w data"
                    )
                    assert data is None
                    with pytest.raises(FileNotFoundError):
                        reader.read_hxl_resource(
                            "not exist", resource, "3w data"
                        )

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
