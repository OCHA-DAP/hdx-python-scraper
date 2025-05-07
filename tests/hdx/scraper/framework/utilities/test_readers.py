from datetime import datetime, timezone

import pytest

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir


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
        def read_from_hdx(dataset_name, _):
            if dataset_name == "None":
                return None
            dataset = Dataset({"name": dataset_name})
            resource = Resource(
                {
                    "name": "test",
                    "url": "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/edit#gid=1088874596",
                }
            )
            resource.set_format("csv")
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
                    monkeypatch.setattr(Dataset, "read_from_hdx", read_from_hdx)
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

    def test_search_datasets(self, configuration, monkeypatch):
        filename = "TestDataset"

        def search_in_hdx(*args, **kwargs):
            datasets = []
            for i in range(2):
                dataset = Dataset({"name": f"{filename}_{i}"})
                resource = Resource(
                    {
                        "name": "test",
                        "url": f"{filename}_{i}.json",
                    }
                )
                resource.set_format("csv")
                dataset.add_update_resource(resource)
                datasets.append(dataset)
            return datasets

        with temp_dir("TestReader") as temp_folder:
            with Download(user_agent="test") as downloader:
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
                    monkeypatch.setattr(Dataset, "search_in_hdx", search_in_hdx)
                    datasets = reader.search_datasets(filename)
                    assert len(datasets) == 2
                    dataset = datasets[0]
                    assert dataset["name"] == f"{filename}_0"
                    assert dataset.get_resource()["url"] == f"{filename}_0.json"
                    dataset = datasets[1]
                    assert dataset["name"] == f"{filename}_1"
                    assert dataset.get_resource()["url"] == f"{filename}_1.json"
                    monkeypatch.delattr(Dataset, "search_in_hdx")
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
                    datasets = reader.search_datasets(filename)
                    assert len(datasets) == 2
                    dataset = datasets[0]
                    assert dataset["name"] == f"{filename}_0"
                    assert dataset.get_resource()["url"] == f"{filename}_0.json"
                    dataset = datasets[1]
                    assert dataset["name"] == f"{filename}_1"
                    assert dataset.get_resource()["url"] == f"{filename}_1.json"

    def test_read_hxl_resource(self, input_folder):
        with temp_dir("TestReader") as temp_folder:
            with Download(user_agent="test") as downloader:
                with Read(
                    downloader,
                    temp_folder,
                    input_folder,
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
                    resource.set_format("csv")
                    data = reader.read_hxl_resource(
                        resource, file_prefix="whowhatwhere_afg"
                    )
                    assert len(data.headers) == 15
                    data = reader.read_hxl_resource(
                        resource, file_prefix="whowhatwhere_notags"
                    )
                    assert data is None
                    with pytest.raises(FileNotFoundError):
                        reader.read_hxl_resource(
                            resource, file_prefix="whowhatwhere_not_exist"
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
        headers, iterator = reader.read(datasetinfo, format="csv")
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
        assert datasetinfo == {
            "dataset": "sahel-humanitarian-needs-overview",
            "filename": "hno_2017_sahel_nutrition.csv",
            "format": "csv",
            "hapi_dataset_metadata": {
                "hdx_id": "47f6ef46-500f-421a-9fa2-fefd93facf95",
                "hdx_stub": "sahel-humanitarian-needs-overview",
                "hdx_provider_stub": "ocha-rowca",
                "hdx_provider_name": "OCHA West and Central Africa (ROWCA)",
                "license": "[Creative Commons Attribution International](http://www.opendefinition.org/licenses/cc-by)",
                "time_period": {
                    "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                    "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
                },
                "title": "Sahel : Humanitarian Needs Overview",
            },
            "hapi_resource_metadata": {
                "download_url": "https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/2527ac5b-66fe-46f0-8b9b-7086d2c4ddd3/download/hno-2017-sahel-nutrition.csv",
                "name": "HNO -2017 -Sahel-nutrition.csv",
                "format": "csv",
                "hdx_id": "2527ac5b-66fe-46f0-8b9b-7086d2c4ddd3",
                "update_date": datetime(2017, 3, 10, 10, 8, 37, tzinfo=timezone.utc),
            },
            "headers": 1,
            "name": "test",
            "source": "Multiple organisations",
            "source_date": {
                "default_date": {
                    "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                    "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
                }
            },
            "source_url": "https://data.humdata.org/dataset/sahel-humanitarian-needs-overview",
            "time_period": {
                "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
            },
            "url": "https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/2527ac5b-66fe-46f0-8b9b-7086d2c4ddd3/download/hno-2017-sahel-nutrition.csv",
        }

        headers, iterator = reader.read(
            datasetinfo,
            filename="education_closures_school_closures.csv",
            format="csv",
        )
        assert headers == ["Date", "ISO", "Country", "Status", "Note"]

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
            "dataset": "sahel-humanitarian-needs-overview",
            "filename": "hno_2017_sahel_people_in_need.xlsx",
            "format": "xlsx",
            "hapi_dataset_metadata": {
                "hdx_id": "47f6ef46-500f-421a-9fa2-fefd93facf95",
                "hdx_stub": "sahel-humanitarian-needs-overview",
                "hdx_provider_stub": "ocha-rowca",
                "hdx_provider_name": "OCHA West and Central Africa (ROWCA)",
                "license": "[Creative Commons Attribution International](http://www.opendefinition.org/licenses/cc-by)",
                "time_period": {
                    "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                    "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
                },
                "title": "Sahel : Humanitarian Needs Overview",
            },
            "hapi_resource_metadata": {
                "download_url": "https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/d9248be4-7bfb-4a81-a7aa-c035dcb737a2/download/hno-2017-sahel-people-in-need.xlsx",
                "name": "HNO-2017-Sahel- People in need.xlsx",
                "format": "xlsx",
                "hdx_id": "d9248be4-7bfb-4a81-a7aa-c035dcb737a2",
                "update_date": datetime(2017, 3, 10, 10, 8, 37, tzinfo=timezone.utc),
            },
            "headers": 1,
            "name": "test",
            "resource": "HNO-2017-Sahel- People in need.xlsx",
            "sheet": 1,
            "source": "Multiple organisations",
            "source_date": {
                "default_date": {
                    "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                    "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
                }
            },
            "source_url": "https://data.humdata.org/dataset/sahel-humanitarian-needs-overview",
            "time_period": {
                "end": datetime(2016, 9, 1, 23, 59, 59, tzinfo=timezone.utc),
                "start": datetime(2016, 9, 1, 0, 0, tzinfo=timezone.utc),
            },
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

    def test_hxl_info_file(self, input_folder):
        with temp_dir("TestReader") as temp_folder:
            with Download(user_agent="test") as downloader:
                with Read(
                    downloader,
                    temp_folder,
                    input_folder,
                    temp_folder,
                    save=False,
                    use_saved=True,
                    today=parse_date("2021-02-01"),
                ) as reader:
                    result = reader.hxl_info_file(
                        "school_closures",
                        "csv",
                        "http://lala",
                        file_prefix="education_closures",
                    )
                    assert result == {
                        "format": "CSV",
                        "sheets": [
                            {
                                "has_merged_cells": False,
                                "header_hash": "25b5a88ea9ae5b8c72b5c91a2326a277",
                                "headers": [
                                    "Date",
                                    "ISO",
                                    "Country",
                                    "Status",
                                    "Note",
                                ],
                                "hxl_header_hash": None,
                                "hxl_headers": None,
                                "is_hidden": False,
                                "is_hxlated": False,
                                "name": "__DEFAULT__",
                                "ncols": 5,
                                "nrows": 4,
                            }
                        ],
                        "url_or_filename": "tests/fixtures/input/education_closures_school_closures.csv",
                    }
                    result = reader.hxl_info_file(
                        "broken",
                        "xls",
                        "http://lala",
                        file_prefix="education_closures",
                    )
                    assert result is None
                    with pytest.raises(IOError):
                        reader.hxl_info_file(
                            "school_closures1",
                            "csv",
                            "http://lala",
                            file_prefix="education_closures",
                        )
