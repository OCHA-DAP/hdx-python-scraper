from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .conftest import run_check_scraper


class TestScraperSubnational:
    def test_get_tabular_subnational(self, configuration):
        BaseScraper.population_lookup = dict()
        with Download(user_agent="test") as downloader:
            today = parse_date("2020-10-01")
            adminone = AdminOne(configuration)
            level = "subnational"
            scraper_configuration = configuration[f"scraper_{level}"]
            runner = Runner(("AFG",), adminone, downloader, dict(), today)
            keys = runner.add_configurables(scraper_configuration, level)
            assert keys == ["gam"]

            name = "gam"
            headers = (
                ["Malnutrition Estimate"],
                ["#severity+malnutrition+num+subnational"],
            )
            values = [
                {
                    "AF17": 3.371688,
                    "AF31": 3.519166,
                    "AF09": 1.524646,
                    "AF21": 1.319626,
                    "AF10": 1.40426,
                    "AF24": 1.043487,
                    "AF33": 2.745447,
                    "AF29": 2.478977,
                    "AF11": 1.022871,
                    "AF23": 1.340286,
                    "AF30": 1.677612,
                    "AF32": 1.687488,
                    "AF28": 0.6210205,
                    "AF01": 1.282291,
                    "AF27": 1.378641,
                    "AF02": 3.552082,
                    "AF14": 0.7653555,
                    "AF15": 0.953823,
                    "AF19": 1.684882,
                    "AF07": 2.090165,
                    "AF05": 0.9474334,
                    "AF06": 2.162038,
                    "AF34": 1.6455,
                    "AF16": 1.927783,
                    "AF12": 4.028857,
                    "AF13": 9.150105,
                    "AF08": 1.64338,
                    "AF03": 2.742952,
                    "AF20": 1.382376,
                    "AF22": 1.523334,
                    "AF18": 0.9578965,
                    "AF25": 0.580423,
                    "AF04": 0.501081,
                    "AF26": 4.572629,
                }
            ]
            sources = [
                (
                    "#severity+malnutrition+num+subnational",
                    "2020-10-01",
                    "UNICEF",
                    "tests/fixtures/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)

            scraper_configuration = configuration["other"]
            runner.add_configurables(scraper_configuration, level)
            name = "gam_other"
            headers = (
                ["Malnutrition Estimate"],
                ["#severity+malnutrition+num+subnational"],
            )
            values = [{"AF09": 1.524646, "AF24": 1.043487}]
            sources = [
                (
                    "#severity+malnutrition+num+subnational",
                    "2020-10-01",
                    "UNICEF",
                    "tests/fixtures/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)
