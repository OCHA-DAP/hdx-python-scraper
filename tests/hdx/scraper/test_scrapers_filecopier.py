from collections import UserDict
from os.path import exists, join

import pytest
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.filecopier import FileCopier
from hdx.scraper.runner import Runner


class TestScrapersFileCopier:
    @pytest.fixture(scope="function")
    def output_file(self, fixtures):
        return join(fixtures, "test_output.xlsx")

    @pytest.fixture(scope="function")
    def dataset(self, output_file):
        class MyDataset(UserDict):
            @staticmethod
            def get_resources():
                rs = Resource(
                    {
                        "name": "test_output",
                        "format": "xlsx",
                        "url": output_file,
                    }
                )
                return [rs]

            @staticmethod
            def get_date_of_dataset(today):
                return {"enddate": today}

            @staticmethod
            def get_hdx_url():
                return "https://hdx/test_output.xlsx"

        ds = MyDataset({"dataset_source": "MySource"})
        return lambda x: ds

    def test_filecopier(
        self, monkeypatch, configuration, dataset, output_file
    ):
        with temp_dir(
            "TestScraperFileCopier",
            delete_on_success=True,
            delete_on_failure=False,
        ) as temp_folder:
            BaseScraper.population_lookup = dict()
            adminone = AdminOne(configuration)
            iso3s = ("AFG", "MMR")
            today = parse_date("2020-10-01")
            runner = Runner(iso3s, adminone, today)
            scrapers = FileCopier.get_scrapers(
                configuration["copyfiles"], today, temp_folder
            )
            scraper_names = runner.add_customs(scrapers, add_to_run=True)
            name = "filecopier_xlsx"
            assert scraper_names == [name]
            monkeypatch.setattr(
                Dataset,
                "read_from_hdx",
                dataset,
            )
            runner.run()
            assert runner.get_sources() == [
                (
                    "#xlsx",
                    "2020-10-01",
                    "MySource",
                    "https://hdx/test_output.xlsx",
                )
            ]
            assert exists(output_file)
