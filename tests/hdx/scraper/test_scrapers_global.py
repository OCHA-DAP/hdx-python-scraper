import logging

import pytest
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .conftest import run_check_scraper


class TestScraperGlobal:
    @pytest.fixture(scope="class")
    def cerf_headers(self):
        return (
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

    def test_get_global_2020(self, configuration, cerf_headers):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "single"
        level_name = "global"
        scraper_configuration = configuration[f"scraper_{level_name}"]
        runner = Runner(configuration["HRPs"], adminone, today)
        keys = runner.add_configurables(
            scraper_configuration, level, level_name
        )
        assert keys == [
            "covax",
            "ourworldindata",
            "cerf_global",
            "broken_cerf_url",
        ]

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
            {"value": "73248240"},
            {"value": "12608040"},
            {"value": "23728358"},
            {"value": "36336398"},
            {"value": "271440"},
            {"value": "67116000"},
            {"value": "5860800"},
        ]
        sources = [
            (
                "#capacity+doses+forecast+covax",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+delivered+covax",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+delivered+others",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+delivered+total",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+covax+pfizerbiontech",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+covax+astrazenecasii",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
            (
                "#capacity+doses+covax+astrazenecaskbio",
                "2020-08-07",
                "covax",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vTVzu79PPTfaA2syevOQfyRRjy63dJWitqu0fFbXIQCzoUn9K9TiMWMRvFGg1RBsnLmgYugzSEiAye2/pub?gid=992438980&single=true&output=csv",
            ),
        ]
        run_check_scraper(
            name,
            runner,
            level_name,
            headers,
            values,
            sources,
            set_not_run=False,
        )
        rows = runner.get_rows("global", ("value",))
        expected_rows = [
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
            [
                "73248240",
                "12608040",
                "23728358",
                "36336398",
                "271440",
                "67116000",
                "5860800",
            ],
        ]
        assert rows == expected_rows
        rows = runner.get_rows(
            "gho", ("value",), overrides={name: {"global": "gho"}}
        )
        assert rows == expected_rows
        runner.set_not_run(name)

        name = "cerf_global"
        headers = cerf_headers
        values = [
            {"value": 906790749.5500005},
            {"value": 829856355.4100008},
            {"value": 37432868.04999999},
            {"value": 39501526.08999999},
            {},
            {},
            {},
            {"value": 848145238.0},
            {},
            {"value": 50042305.0},
            {"value": 75349572.0},
            {"value": 224560378.0},
            {"value": 349338181.0},
            {"value": 147855321.0},
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
        run_check_scraper(name, runner, level_name, headers, values, sources)

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
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            )
        ]
        run_check_scraper(name, runner, level_name, headers, values, sources)

    def test_get_global_2021(
        self, cerf_headers, caplog, configuration, fallbacks
    ):
        BaseScraper.population_lookup = dict()
        today = parse_date("2021-05-03")
        adminone = AdminOne(configuration)
        level = "single"
        level_name = "global"
        scraper_configuration = configuration[f"scraper_{level_name}"]
        runner = Runner(configuration["HRPs"], adminone, today)
        runner.add_configurables(scraper_configuration, level, level_name)
        name = "cerf_global"
        headers = cerf_headers
        values = [
            {"value": 7811774.670000001},
            {"value": 7811774.670000001},
            {},
            {},
            {},
            {},
            {},
            {"value": 89298919.0},
            {"value": 6747034.0},
            {},
            {"value": 2549855.0},
            {"value": 10552572.0},
            {"value": 26098816.0},
            {"value": 43350642.0},
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
        run_check_scraper(name, runner, level_name, headers, values, sources)

        # Test fallbacks with subsets
        with caplog.at_level(logging.ERROR):
            name = "broken_cerf_url"
            headers = cerf_headers
            values = [
                {"value": 7811775.670000001},
                {"value": 7811775.670000001},
                {},
                {},
                {},
                {},
                {},
                {"value": 89298924.0},
                {"value": 6747035.0},
                {},
                {"value": 2549856.0},
                {"value": 10552573.0},
                {"value": 26098817.0},
                {"value": 43350643.0},
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
                level_name,
                headers,
                values,
                sources,
                fallbacks_used=True,
            )
            assert f"Using fallbacks for {name}!" in caplog.text
            assert (
                "No such file or directory: 'tests/fixtures/broken_cerf_url_notexist.csv'"
                in caplog.text
            )

        name = "ourworldindata"
        headers = (
            ["TotalDosesAdministered"],
            ["#capacity+doses+administered+total"],
        )
        values = [{"value": "13413871"}]
        sources = [
            (
                "#capacity+doses+administered+total",
                "2021-05-03",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            )
        ]
        run_check_scraper(name, runner, level_name, headers, values, sources)

        scraper_configuration = configuration["other"]
        runner.add_configurables(scraper_configuration, level, level_name)
        name = "population_other"
        headers = (
            ["Population"],
            ["#population"],
        )
        val = 7673533972
        values = [{"value": val}]
        pop_value = {"global": val}
        sources = [
            (
                "#population",
                "2021-05-03",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            )
        ]
        run_check_scraper(
            name,
            runner,
            level_name,
            headers,
            values,
            sources,
            population_lookup=pop_value,
        )

        name = "ourworldindata_other"
        headers = (
            ["TotalDosesAdministered"],
            ["#capacity+doses+administered+total"],
        )
        values = [{"value": "1175451507"}]
        sources = [
            (
                "#capacity+doses+administered+total",
                "2021-05-03",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            )
        ]
        run_check_scraper(name, runner, level_name, headers, values, sources)

        name = "altworldindata"
        headers = (
            [
                "TotalDosesAdministered",
                "PopulationCoverageAdministeredDoses",
            ],
            [
                "#capacity+doses+administered+total",
                "#capacity+doses+administered+coverage+pct",
            ],
        )
        values = [{"value": 1175451507}, {"value": "0.0766"}]
        sources = [
            (
                "#capacity+doses+administered+total",
                "2021-05-03",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            ),
            (
                "#capacity+doses+administered+coverage+pct",
                "2021-05-03",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            ),
        ]
        run_check_scraper(name, runner, level_name, headers, values, sources)
