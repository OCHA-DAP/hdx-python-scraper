import logging

from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download

from hdx.scraper.runner import Runner
from tests.hdx.scraper.conftest import run_check_scraper


class TestScraperGlobal:
    def test_get_tabular_global(self, caplog, configuration, fallbacks):
        with Download(user_agent="test") as downloader:
            today = parse_date("2020-10-01")
            adminone = AdminOne(configuration)
            level = "global"
            scraper_configuration = configuration[f"scraper_{level}"]
            runner = Runner(
                configuration["HRPs"], adminone, downloader, dict(), today
            )
            runner.add_configurables(scraper_configuration, level)
            name = "covax"
            headers = (
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
            values = [
                {"global": "73248240"},
                {"global": "12608040"},
                {"global": "23728358"},
                {"global": "36336398"},
                {"global": "271440"},
                {"global": "67116000"},
                {"global": "5860800"},
            ]
            sources = [
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
            run_check_scraper(name, runner, level, headers, values, sources)

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
            name = "cerf_global"
            headers = cerf_headers
            values = [
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
            sources = [
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
            run_check_scraper(name, runner, level, headers, values, sources)

            name = "ourworldindata"
            headers = (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            values = [dict()]
            sources = [
                (
                    "#capacity+doses+administered+total",
                    "2020-10-01",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)

            today = parse_date("2021-05-03")
            runner = Runner(
                configuration["HRPs"], adminone, downloader, dict(), today
            )
            runner.add_configurables(scraper_configuration, level)
            name = "cerf_global"
            headers = cerf_headers
            values = [
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
            sources = [
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
            run_check_scraper(name, runner, level, headers, values, sources)

            # Test fallbacks with subsets
            with caplog.at_level(logging.ERROR):
                name = "broken_cerf_url"
                headers = cerf_headers
                values = [
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
                sources = [
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
                run_check_scraper(
                    name,
                    runner,
                    level,
                    headers,
                    values,
                    sources,
                    fallbacks_used=True,
                )
                assert f"Using fallbacks for {name}!" in caplog.text
                assert (
                    "Getting tabular stream for NOTEXIST.csv failed!"
                    in caplog.text
                )

            name = "ourworldindata"
            headers = (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            values = [{"global": "13413871"}]
            sources = [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)

            scraper_configuration = configuration["other"]
            runner.add_configurables(scraper_configuration, level)
            name = "ourworldindata_other"
            headers = (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            values = [{"global": "1175451507"}]
            sources = [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)

            name = "altworldindata"
            headers = (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            values = [{"global": "1175451507"}]
            sources = [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            run_check_scraper(name, runner, level, headers, values, sources)
