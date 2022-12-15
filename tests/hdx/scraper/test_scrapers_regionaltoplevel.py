from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.scraper.utilities.writer import Writer

from .conftest import run_check_scraper, run_check_scrapers


class TestScrapersRegionalToplevel:
    access_national_headers = (
        [
            "% of visas pending or denied",
            "% of travel authorizations or movements denied",
            "Number of incidents reported in previous year",
            "Number of incidents reported since start of year",
            "Number of incidents reported since start of previous year",
            "% of CERF projects affected by insecurity and inaccessibility",
            "% of CBPF projects affected by insecurity and inaccessibility",
            "Campaign Vaccine",
            "Campaign Vaccine Status",
            "Number of learners enrolled from pre-primary to tertiary education",
        ],
        [
            "#access+visas+pct",
            "#access+travel+pct",
            "#event+year+previous+num",
            "#event+year+todate+num",
            "#event+year+previous+todate+num",
            "#activity+cerf+project+insecurity+pct",
            "#activity+cbpf+project+insecurity+pct",
            "#service+name",
            "#status+name",
            "#population+education",
        ],
    )
    access_national_values = [
        {"AFG": 0.2, "PSE": None},
        {"AFG": "N/A", "PSE": None},
        {"AFG": "20", "PSE": "7"},
        {"AFG": "2", "PSE": None},
        {"AFG": "22", "PSE": "7"},
        {"AFG": 0.5710000000000001, "PSE": 0.0},
        {"AFG": 0.04, "PSE": 0.205},
        {"AFG": "bivalent Oral Poliovirus", "PSE": None},
        {"AFG": "Postponed", "PSE": "N/A"},
        {"AFG": 9979405, "PSE": None},
    ]
    access_national_sources = [
        (
            "#access+visas+pct",
            "Oct 1, 2020",
            "OCHA",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#access+travel+pct",
            "Oct 1, 2020",
            "OCHA",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#event+year+previous+num",
            "Oct 1, 2020",
            "Aid Workers Database",
            "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
        ),
        (
            "#event+year+todate+num",
            "Oct 1, 2020",
            "Aid Workers Database",
            "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
        ),
        (
            "#event+year+previous+todate+num",
            "Oct 1, 2020",
            "Aid Workers Database",
            "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
        ),
        (
            "#activity+cerf+project+insecurity+pct",
            "Oct 1, 2020",
            "UNCERF",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#activity+cbpf+project+insecurity+pct",
            "Oct 1, 2020",
            "UNCERF",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#service+name",
            "Oct 1, 2020",
            "Multiple sources",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#status+name",
            "Oct 1, 2020",
            "Multiple sources",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
        (
            "#population+education",
            "Oct 1, 2020",
            "UNESCO",
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
        ),
    ]
    access_names = [
        "access_visas_pct_global",
        "access_travel_pct_global",
        "event_year_todate_num_global",
        "activity_cerf_project_insecurity_pct_global",
    ]
    access_headers = (
        [
            "% of visas pending or denied",
            "% of travel authorizations or movements denied",
            "Number of incidents reported since start of year",
            "% of CERF projects affected by insecurity and inaccessibility",
        ],
        [
            "#access+visas+pct",
            "#access+travel+pct",
            "#event+year+todate+num",
            "#activity+cerf+project+insecurity+pct",
        ],
    )
    access_values = [
        {"value": "0.2"},
        {"value": ""},
        {"value": 2},
        {"value": "0.2855"},
    ]
    access_source_urls = [
        "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
    ]

    def test_regionaltoplevel(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")

        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
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
        configuration_hxl = configuration["aggregation_hxl"]
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
            set_not_run=False,
        )

        regions = ("ROAP",)
        aggregator_configuration = configuration_hxl["aggregation_sum"]
        regional_names = runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            "regional",
            {"AFG": regions, "PSE": ("somewhere",)},
            force_add_to_run=True,
        )
        runner.run(regional_names)
        level = "regional"
        toplevel = "allregions"
        mapping = {"global": toplevel}
        jsonout = JsonFile(configuration["json"], [level, toplevel])
        outputs = {"json": jsonout}
        writer = Writer(runner, outputs)
        regional_rows = writer.get_regional_rows(regions, names=regional_names)
        allregions_rows = writer.get_toplevel_rows(
            names=names,
            overrides={
                names[0]: mapping,
                names[1]: mapping,
                names[2]: mapping,
            },
            toplevel=toplevel,
        )
        writer.update_regional(
            regional_rows,
            allregions_rows,
            toplevel_hxltags=allregions_rows[1],
            tab=level,
            toplevel=toplevel,
        )
        assert jsonout.json[f"{level}_data"] == [
            {"#population": "38041754", "#region+name": "ROAP"},
            {
                "#region+name": toplevel,
                "#population": "42727060",
                "#affected+infected+per100000": "229.71",
                "#affected+infected+perpop": "0.5376",
            },
        ]
        regional_rows = (
            ("regionnames", "MyColumn"),
            ("#region+name", "#mycolumn"),
            (toplevel, 12345),
        )
        writer.update_toplevel(
            allregions_rows,
            regional_rows=regional_rows,
            regional_adm=toplevel,
            regional_hxltags=["#mycolumn"],
        )
        assert jsonout.json["allregions_data"] == [
            {
                "#population": "42727060",
                "#affected+infected+per100000": "229.71",
                "#affected+infected+perpop": "0.5376",
                "#mycolumn": "12345",
            }
        ]
        jsonout.json["allregions_data"] = list()
        writer.update_toplevel(
            allregions_rows,
            regional_rows=regional_rows,
            regional_adm=toplevel,
            regional_hxltags=["#mycolumn"],
            regional_first=True,
        )
        assert jsonout.json["allregions_data"] == [
            {
                "#mycolumn": "12345",
                "#population": "42727060",
                "#affected+infected+per100000": "229.71",
                "#affected+infected+perpop": "0.5376",
            }
        ]

    def test_sourceperhxltag_nosources(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")

        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG", "PSE")
        runner = Runner(iso3s, today)
        runner.add_configurables(scraper_configuration, level)

        run_check_scraper(
            "access",
            runner,
            level,
            self.access_national_headers,
            self.access_national_values,
            self.access_national_sources,
            source_urls=self.access_source_urls,
            set_not_run=False,
        )

        level = "global"
        aggregator_configuration = configuration[
            "aggregation_regionaltoplevel"
        ]
        adm_aggregation = ("AFG", "PSE")
        source_configuration = Sources.create_source_configuration(
            no_sources=True
        )
        names = runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            source_configuration=source_configuration,
            force_add_to_run=True,
        )
        assert names == self.access_names
        run_check_scrapers(
            names,
            runner,
            level,
            self.access_headers,
            self.access_values,
            list(),
            source_urls=self.access_source_urls,
            set_not_run=False,
        )

    def test_sourceperhxltag(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")

        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG", "PSE")
        runner = Runner(iso3s, today)
        runner.add_configurables(scraper_configuration, level)

        run_check_scraper(
            "access",
            runner,
            level,
            self.access_national_headers,
            self.access_national_values,
            self.access_national_sources,
            source_urls=self.access_source_urls,
            set_not_run=False,
        )

        level = "global"
        aggregator_configuration = configuration[
            "aggregation_regionaltoplevel"
        ]
        adm_aggregation = ("AFG", "PSE")
        names = runner.add_aggregators(
            True,
            aggregator_configuration,
            "national",
            level,
            adm_aggregation,
            force_add_to_run=True,
        )
        assert names == self.access_names
        sources = [
            (
                "#access+visas+pct",
                "Oct 1, 2020",
                "OCHA",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
            ),
            (
                "#event+year+todate+num",
                "Oct 1, 2020",
                "Aid Workers Database",
                "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
            ),
            (
                "#activity+cerf+project+insecurity+pct",
                "Oct 1, 2020",
                "UNCERF",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
            ),
        ]
        run_check_scrapers(
            names,
            runner,
            level,
            self.access_headers,
            self.access_values,
            sources,
            source_urls=self.access_source_urls,
            set_not_run=False,
        )
