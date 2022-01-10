from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download

from hdx.scraper.scrapers import run_scrapers
from tests.hdx.scraper import get_fallbacks


class TestScraperGlobal:

    def test_get_tabular_global(self, configuration, fallback_data):
        with Download(user_agent="test") as downloader:
            today = parse_date("2020-10-01")
            adminone = AdminOne(configuration)
            population_lookup = dict()
            level = "global"
            scraper_configuration = configuration[f"scraper_{level}"]
            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["covax"],
            )
            assert results["headers"] == (
                [
                    "Covax Interim Forecast Doses",
                    "Covax Delivered Doses",
                    "Other Delivered Doses",
                    "Total Delivered Doses",
                    "Covax Pfizer-BioNTech Doses",
                    "Covax Astrazeneca-SII Doses",
                    "Covax Astrazeneca-SKBio Doses",
                ],
                [
                    "#capacity+doses+forecast+covax",
                    "#capacity+doses+delivered+covax",
                    "#capacity+doses+delivered+others",
                    "#capacity+doses+delivered+total",
                    "#capacity+doses+covax+pfizerbiontech",
                    "#capacity+doses+covax+astrazenecasii",
                    "#capacity+doses+covax+astrazenecaskbio",
                ],
            )
            assert results["values"] == [
                {"global": "73248240"},
                {"global": "12608040"},
                {"global": "23728358"},
                {"global": "36336398"},
                {"global": "271440"},
                {"global": "67116000"},
                {"global": "5860800"},
            ]
            assert results["sources"] == [
                (
                    "#capacity+doses+forecast+covax",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+delivered+covax",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+delivered+others",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+delivered+total",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+covax+pfizerbiontech",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+covax+astrazenecasii",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
                (
                    "#capacity+doses+covax+astrazenecaskbio",
                    "2020-08-07",
                    "covax",
                    "tests/fixtures/COVID-19 Vaccine Doses in HRP Countries - Data HXL.csv",
                ),
            ]

            cerf_headers = (
                [
                    "CBPFFunding",
                    "CBPFFundingGMEmpty",
                    "CBPFFundingGM0",
                    "CBPFFundingGM1",
                    "CBPFFundingGM2",
                    "CBPFFundingGM3",
                    "CBPFFundingGM4",
                    "CERFFunding",
                    "CERFFundingGMEmpty",
                    "CERFFundingGM0",
                    "CERFFundingGM1",
                    "CERFFundingGM2",
                    "CERFFundingGM3",
                    "CERFFundingGM4",
                ],
                [
                    "#value+cbpf+funding+total+usd",
                    "#value+cbpf+funding+gmempty+total+usd",
                    "#value+cbpf+funding+gm0+total+usd",
                    "#value+cbpf+funding+gm1+total+usd",
                    "#value+cbpf+funding+gm2+total+usd",
                    "#value+cbpf+funding+gm3+total+usd",
                    "#value+cbpf+funding+gm4+total+usd",
                    "#value+cerf+funding+total+usd",
                    "#value+cerf+funding+gmempty+total+usd",
                    "#value+cerf+funding+gm0+total+usd",
                    "#value+cerf+funding+gm1+total+usd",
                    "#value+cerf+funding+gm2+total+usd",
                    "#value+cerf+funding+gm3+total+usd",
                    "#value+cerf+funding+gm4+total+usd",
                ],
            )
            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["cerf_global"],
            )
            assert results["headers"] == cerf_headers
            assert results["values"] == [
                {"global": 906790749.5500005},
                {"global": 829856355.4100008},
                {"global": 37432868.04999999},
                {"global": 39501526.08999999},
                {},
                {},
                {},
                {"global": 848145238.0},
                {},
                {"global": 50042305.0},
                {"global": 75349572.0},
                {"global": 224560378.0},
                {"global": 349338181.0},
                {"global": 147855321.0},
            ]
            assert results["sources"] == [
                (
                    "#value+cbpf+funding+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gmempty+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm0+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm1+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm2+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm3+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm4+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gmempty+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm0+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm1+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm2+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm3+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm4+total+usd",
                    "2020-10-01",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
            ]
            assert results["fallbacks"] == list()

            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["ourworldindata"],
            )
            assert results["headers"] == (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            assert results["values"] == [dict()]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2020-10-01",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]

            today = parse_date("2021-05-03")
            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                scrapers=["cerf_global"],
                population_lookup=population_lookup,
            )
            assert results["headers"] == cerf_headers
            assert results["values"] == [
                {"global": 7811774.670000001},
                {"global": 7811774.670000001},
                {},
                {},
                {},
                {},
                {},
                {"global": 89298919.0},
                {"global": 6747034.0},
                {},
                {"global": 2549855.0},
                {"global": 10552572.0},
                {"global": 26098816.0},
                {"global": 43350642.0},
            ]
            assert results["sources"] == [
                (
                    "#value+cbpf+funding+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gmempty+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm0+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm1+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm2+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm3+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cbpf+funding+gm4+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gmempty+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm0+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm1+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm2+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm3+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
                (
                    "#value+cerf+funding+gm4+total+usd",
                    "2021-05-03",
                    "CERF and CBPF",
                    "https://data.humdata.org/dataset/cerf-covid-19-allocations",
                ),
            ]
            assert results["fallbacks"] == list()

            # Test fallbacks with subsets
            fallbacks = get_fallbacks(fallback_data, level)
            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                fallbacks=fallbacks,
                scrapers=["broken_cerf_url"],
            )
            assert results["headers"] == cerf_headers
            assert results["values"] == [
                {"global": 7811775.670000001},
                {"global": 7811775.670000001},
                {},
                {},
                {},
                {},
                {},
                {"global": 89298924.0},
                {"global": 6747035.0},
                {},
                {"global": 2549856.0},
                {"global": 10552573.0},
                {"global": 26098817.0},
                {"global": 43350643.0},
            ]
            assert results["sources"] == [
                (
                    "#value+cbpf+funding+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gmempty+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gm0+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gm1+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gm2+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gm3+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cbpf+funding+gm4+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gmempty+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gm0+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gm1+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gm2+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gm3+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
                (
                    "#value+cerf+funding+gm4+total+usd",
                    "2020-09-01",
                    "CERF and CBPF",
                    "tests/fixtures/fallbacks.json",
                ),
            ]
            assert results["fallbacks"] == ["broken_cerf_url"]

            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["ourworldindata"],
            )
            assert results["headers"] == (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            assert results["values"] == [{"global": "13413871"}]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            scraper_configuration = configuration["other"]
            results = run_scrapers(
                scraper_configuration,
                level,
                configuration["HRPs"],
                adminone,
                downloader,
                today=today,
                scrapers=["ourworldindata"],
                population_lookup=population_lookup,
            )
            assert results["headers"] == (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            assert results["values"] == [{"global": "1175451507"}]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
