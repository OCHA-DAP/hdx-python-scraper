from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.aggregator import Aggregator
from hdx.scraper.runner import Runner

from .conftest import run_check_scrapers


class TestRunnerGetResults:
    def test_get_results(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)

        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG", "MMR")
        runner = Runner(iso3s, adminone, today)
        runner.add_configurables(scraper_configuration, level)

        level = "national"
        BaseScraper.population_lookup = dict()
        iso3s = ("AFG", "PSE")
        runner = Runner(iso3s, adminone, today)
        runner.add_configurables(scraper_configuration, level)

        names = ("population", "who_national")
        headers = (
            [
                "Population",
                "CasesPer100000",
                "DeathsPer100000",
            ],
            [
                "#population",
                "#affected+infected+per100000",
                "#affected+killed+per100000",
            ],
        )
        national_values = [
            {"AFG": 38041754, "PSE": 4685306},
            {"AFG": "96.99", "PSE": "362.43"},
            {"AFG": "3.41", "PSE": "1.98"},
        ]
        sources = [
            (
                "#population",
                "2020-10-01",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+per100000",
                "2020-08-06",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
            (
                "#affected+killed+per100000",
                "2020-08-06",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
        ]
        source_urls = [
            "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            "https://data.humdata.org/organization/world-bank-group",
        ]
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            national_values,
            sources,
            source_urls=source_urls,
            set_not_run=False,
        )

        level = "global"
        configuration_hxl = configuration["aggregation_hxl"]
        aggregator_configuration = configuration_hxl[f"aggregation_{level}"]
        adm_aggregation = ("AFG", "PSE")
        scrapers = Aggregator.get_scrapers(
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            runner,
        )
        names = runner.add_customs(scrapers, add_to_run=True)
        assert names == [
            f"population_{level}",
            f"affected_infected_per100000_{level}",
            f"affected_infected_perpop_{level}",
        ]
        pop = 42727060
        headers = (
            ["Population", "CasesPer100000", "CasesPerPopulation"],
            [
                "#population",
                "#affected+infected+per100000",
                "#affected+infected+perpop",
            ],
        )
        values = [{"value": pop}, {"value": "229.71"}, {"value": "0.5376"}]
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            list(),
            population_lookup=national_values[0] | {"global": pop},
            source_urls=source_urls,
            set_not_run=False,
        )

        regions = ("ROAP",)
        aggregator_configuration = configuration_hxl["aggregation_sum"]
        scrapers = Aggregator.get_scrapers(
            aggregator_configuration,
            "national",
            "regional",
            {"AFG": regions, "PSE": ("somewhere",)},
            runner,
        )
        regional_names = runner.add_customs(scrapers, add_to_run=True)
        runner.run(regional_names)
        results = runner.get_results()
        assert results == {
            "national": {
                "headers": (
                    ["Population", "CasesPer100000", "DeathsPer100000"],
                    [
                        "#population",
                        "#affected+infected+per100000",
                        "#affected+killed+per100000",
                    ],
                ),
                "values": [
                    {"AFG": 38041754, "PSE": 4685306},
                    {"AFG": "96.99", "PSE": "362.43"},
                    {"AFG": "3.41", "PSE": "1.98"},
                ],
                "sources": [
                    (
                        "#population",
                        "2020-10-01",
                        "World Bank",
                        "https://data.humdata.org/organization/world-bank-group",
                    ),
                    (
                        "#affected+infected+per100000",
                        "2020-08-06",
                        "WHO",
                        "https://covid19.who.int/WHO-COVID-19-global-data.csv",
                    ),
                    (
                        "#affected+killed+per100000",
                        "2020-08-06",
                        "WHO",
                        "https://covid19.who.int/WHO-COVID-19-global-data.csv",
                    ),
                ],
                "fallbacks": [],
            },
            "global": {
                "headers": (
                    ["Population", "CasesPer100000", "CasesPerPopulation"],
                    [
                        "#population",
                        "#affected+infected+per100000",
                        "#affected+infected+perpop",
                    ],
                ),
                "values": [
                    {"value": 42727060},
                    {"value": "229.71"},
                    {"value": "0.5376"},
                ],
                "sources": [],
                "fallbacks": [],
            },
            "regional": {
                "headers": (["Population"], ["#population"]),
                "values": [{"ROAP": 38041754, "somewhere": 4685306}],
                "sources": [],
                "fallbacks": [],
            },
        }
        results = runner.get_results(
            names=("population_global",), levels=("national", "global")
        )
        assert results == {
            "global": {
                "headers": (["Population"], ["#population"]),
                "values": [{"value": 42727060}],
                "sources": [],
                "fallbacks": [],
            }
        }
        results = runner.get_results(
            names=("population_global",),
            levels=("world", "global"),
            overrides={
                "population_global": {"national": "world", "global": "world"}
            },
        )
        assert results == {
            "world": {
                "headers": (["Population"], ["#population"]),
                "values": [{"value": 42727060}],
                "sources": [],
                "fallbacks": [],
            }
        }
