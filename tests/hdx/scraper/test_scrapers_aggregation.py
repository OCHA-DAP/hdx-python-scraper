from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources

from .conftest import run_check_scraper, run_check_scrapers


class TestScrapersAggregation:
    def test_get_aggregation_hxl(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")

        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG", "MMR")
        runner = Runner(iso3s, today)
        runner.add_configurables(scraper_configuration, level)

        name = "population"
        headers = (["Population"], ["#population"])
        national_values = [{"AFG": 38041754, "MMR": 54045420}]
        sources = [
            (
                "#population",
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            )
        ]
        source_urls = [
            "https://data.humdata.org/organization/world-bank-group"
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            national_values,
            sources,
            population_lookup=national_values[0],
            source_urls=source_urls,
            set_not_run=False,
        )

        regions = ("ROAP",)
        adm_aggregation = {"AFG": regions, "MMR": regions}

        configuration_hxl = configuration["aggregation_hxl"]
        level = "regional"
        aggregator_configuration = configuration_hxl["aggregation_sum"]
        names = runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )
        name = "population_regional"
        assert names == [name]

        values = [{"ROAP": 92087174}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            population_lookup=national_values[0] | values[0],
            source_urls=source_urls,
        )

        source_configuration = Sources.create_source_configuration(
            suffix_attribute="regional"
        )
        aggregator_configuration = configuration_hxl["aggregation_mean"]
        runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            source_configuration,
            force_add_to_run=True,
        )

        level = "regional"
        values = [{"ROAP": 46043587}]
        sources2 = [
            (
                "#population+regional",
                "Dec 31, 2022",
                "aggsource",
                "http://aggsource",
            )
        ]
        source_urls2 = [
            "http://aggsource",
            "https://data.humdata.org/organization/world-bank-group",
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources2,
            population_lookup=national_values[0] | values[0],
            source_urls=source_urls2,
        )

        aggregator_configuration = configuration_hxl["aggregation_range"]
        runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )

        level = "regional"
        values = [{"ROAP": "38041754-54045420"}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=source_urls,
        )

        level = "national"
        BaseScraper.population_lookup = dict()
        iso3s = ("AFG", "PSE")
        runner = Runner(iso3s, today)
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
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+per100000",
                "Aug 6, 2020",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
            (
                "#affected+killed+per100000",
                "Aug 6, 2020",
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
        aggregator_configuration = configuration_hxl[f"aggregation_{level}"]
        adm_aggregation = ("AFG", "PSE")
        names = runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )
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
        sources = [
            (
                "#population",
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+per100000",
                "Aug 6, 2020",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
            (
                "#affected+infected+perpop",
                "Aug 6, 2020",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
        ]
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            population_lookup=national_values[0] | {"global": pop},
            source_urls=source_urls,
        )

    def test_get_aggregation_nohxl(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        configuration_nohxl = configuration["aggregation_nohxl"]
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG", "MMR")
        runner = Runner(iso3s, today)
        runner.add_configurables(scraper_configuration, level)

        name = "population"
        headers = (["Population"], ["#population"])
        national_values = [{"AFG": 38041754, "MMR": 54045420}]
        sources = [
            (
                "#population",
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            )
        ]
        source_urls = [
            "https://data.humdata.org/organization/world-bank-group"
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            national_values,
            sources,
            population_lookup=national_values[0],
            source_urls=source_urls,
            set_not_run=False,
        )

        adm_aggregation = {"AFG": ("ROAP",), "MMR": ("ROAP",)}

        level = "regional"
        aggregator_configuration = configuration_nohxl["aggregation_sum"]
        source_configuration = Sources.create_source_configuration(
            suffix_attribute="global"
        )
        names = runner.add_aggregators(
            False,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            source_configuration,
            force_add_to_run=True,
        )
        name = "population_regional"
        assert names == [name]

        values = [{"ROAP": 92087174}]
        sources2 = [
            (
                "#population+global",
                "Jan-Dec 2020",
                "World Bank",
                "https://data.humdata.org/dataset/population",
            )
        ]
        source_urls2 = [
            "https://data.humdata.org/dataset/population",
            "https://data.humdata.org/organization/world-bank-group",
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources2,
            population_lookup=national_values[0] | values[0],
            source_urls=source_urls2,
        )

        aggregator_configuration = configuration_nohxl["aggregation_mean"]
        runner.add_aggregators(
            False,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )

        level = "regional"
        values = [{"ROAP": 46043587}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            population_lookup=national_values[0] | values[0],
            source_urls=source_urls,
        )

        aggregator_configuration = configuration_nohxl["aggregation_range"]
        runner.add_aggregators(
            False,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )

        level = "regional"
        values = [{"ROAP": "38041754-54045420"}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=source_urls,
        )

        level = "national"
        BaseScraper.population_lookup = dict()
        iso3s = ("AFG", "PSE")
        runner = Runner(iso3s, today)
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
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+per100000",
                "Aug 6, 2020",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
            (
                "#affected+killed+per100000",
                "Aug 6, 2020",
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

        level = "regional"
        aggregator_configuration = configuration_nohxl["aggregation_global"]
        adm_aggregation = ("AFG", "PSE")
        names = runner.add_aggregators(
            False,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )
        assert names == [
            f"population_{level}",
            f"casesper100000_{level}",
            f"casesperpopulation_{level}",
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
        sources = [
            (
                "#population",
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+per100000",
                "Oct 1, 2020",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#affected+infected+perpop",
                "Aug 6, 2020",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
        ]
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            population_lookup=national_values[0] | {"allregions": pop},
            source_urls=source_urls,
        )
