from os.path import exists, join

import pytest
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner


class TestScrapersResourceDownloader:
    @pytest.fixture(scope="function")
    def output_file(self, fixtures):
        return join(fixtures, "test_output.xlsx")

    def test_resourcedownloader(self, configuration, output_file):
        with temp_dir(
            "TestScraperResourceDownloader",
            delete_on_success=True,
            delete_on_failure=False,
        ) as temp_folder:
            BaseScraper.population_lookup = dict()
            adminone = AdminOne(configuration)
            iso3s = ("AFG", "MMR")
            today = parse_date("2020-10-01")
            runner = Runner(iso3s, adminone, today)
            scraper_names = runner.add_resource_downloaders(
                configuration["download_resources"], temp_folder
            )
            name = "resource_downloader_xlsx"
            assert scraper_names == [name]
            runner.run()
            assert runner.get_sources() == [
                (
                    "#xlsx",
                    "2022-04-14",
                    "Multiple Sources",
                    "https://data.humdata.org/dataset/ukraine-border-crossings",
                )
            ]
            assert exists(output_file)
