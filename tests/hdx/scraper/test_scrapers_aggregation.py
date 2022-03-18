from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.aggregator import Aggregator
from hdx.scraper.runner import Runner

from .conftest import check_scrapers, run_check_scraper
from .unhcr_myanmar_idps import idps_post_run


class TestScrapersAggregation:
    def test_get_aggregation(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        with Download(user_agent="test") as downloader:
            today = parse_date("2020-10-01")
            adminone = AdminOne(configuration)
            level = "national"
            scraper_configuration = configuration[f"scraper_{level}"]
            iso3s = ("AFG", "MMR")
            runner = Runner(iso3s, adminone, downloader, dict(), today)
            runner.add_configurables(scraper_configuration, level)

            name = "population"
            headers = (["Population"], ["#population"])
            national_values = [{"AFG": 38041754, "MMR": 54045420}]
            sources = [
                (
                    "#population",
                    "2020-10-01",
                    "World Bank",
                    "https://data.humdata.org/organization/world-bank-group",
                )
            ]
            run_check_scraper(
                name,
                runner,
                level,
                headers,
                national_values,
                sources,
                population_lookup=national_values[0],
                source_urls=[
                    "https://data.humdata.org/organization/world-bank-group"
                ],
            )

            adm_aggregation = {"AFG": ("ROAP",), "MMR": ("ROAP",)}

            aggregator_configuration = configuration["aggregation_sum"]
            scrapers = Aggregator.get_scrapers(
                aggregator_configuration,
                "national",
                "regional",
                adm_aggregation,
                runner,
            )
            scraper_names = runner.add_customs(scrapers, add_to_run=True)
            name = "population_regional"
            assert scraper_names == [name]

            level = "regional"
            values = [{"ROAP": 92087174}]
            run_check_scraper(
                name,
                runner,
                level,
                headers,
                values,
                list(),
                population_lookup=national_values[0] | values[0],
                source_urls=list(),
            )

            aggregator_configuration = configuration["aggregation_mean"]
            scrapers = Aggregator.get_scrapers(
                aggregator_configuration,
                "national",
                "regional",
                adm_aggregation,
                runner,
            )
            runner.add_customs(scrapers, add_to_run=True)

            level = "regional"
            values = [{"ROAP": 46043587}]
            run_check_scraper(
                name,
                runner,
                level,
                headers,
                values,
                list(),
                population_lookup=national_values[0] | values[0],
                source_urls=list(),
            )

            aggregator_configuration = configuration["aggregation_range"]
            scrapers = Aggregator.get_scrapers(
                aggregator_configuration,
                "national",
                "regional",
                adm_aggregation,
                runner,
            )
            runner.add_customs(scrapers, add_to_run=True)

            level = "regional"
            values = [{"ROAP": "38041754-54045420"}]
            run_check_scraper(
                name,
                runner,
                level,
                headers,
                values,
                list(),
                source_urls=list(),
            )

            aggregator_configuration = configuration["aggregation_sum_global"]
            adm_aggregation = ("AFG", "MMR")
            scrapers = Aggregator.get_scrapers(
                aggregator_configuration,
                "national",
                "regional",
                adm_aggregation,
                runner,
            )
            runner.add_customs(scrapers, add_to_run=True)

            BaseScraper.population_lookup = dict()
            level = "regional"
            pop = 92087174
            values = [{"value": pop}]
            run_check_scraper(
                name,
                runner,
                level,
                headers,
                values,
                list(),
                population_lookup={"global": pop},
                source_urls=list(),
            )
