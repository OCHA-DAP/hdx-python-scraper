from os.path import exists, join

import pytest
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

    def test_filecopier(self, configuration, output_file):
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
                configuration["copyfiles"], temp_folder
            )
            scraper_names = runner.add_customs(scrapers, add_to_run=True)
            name = "filecopier_xlsx"
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
