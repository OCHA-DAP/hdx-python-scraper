from os.path import join

import pytest
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_json

from hdx.scraper.scrapers import run_scrapers


class TestScraper:
    @pytest.fixture(scope="class")
    def fallback_path(self):
        return join("tests", "fixtures", "fallbacks.json")

    def test_get_tabular(self, configuration, fallback_path):
        with Download(user_agent="test") as downloader:
            today = parse_date("2020-10-01")
            adminone = AdminOne(configuration)
            population_lookup = dict()
            level = "national"
            scraper_configuration = configuration[f"scraper_{level}"]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["population"],
            )
            assert results["headers"] == (["Population"], ["#population"])
            assert results["values"] == [{"AFG": 38041754}]
            assert results["sources"] == [
                (
                    "#population",
                    "2020-10-01",
                    "World Bank",
                    "https://data.humdata.org/organization/world-bank-group",
                )
            ]
            assert population_lookup == {"AFG": 38041754}
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["who"],
            )
            assert results["headers"] == (
                [
                    "CasesPer100000",
                    "DeathsPer100000",
                    "Cases2Per100000",
                    "Deaths2Per100000",
                ],
                [
                    "#affected+infected+per100000",
                    "#affected+killed+per100000",
                    "#affected+infected+2+per100000",
                    "#affected+killed+2+per100000",
                ],
            )
            assert results["values"] == [
                {"AFG": "96.99"},
                {"AFG": "3.41"},
                {"AFG": "96.99"},
                {"AFG": "3.41"},
            ]
            assert results["sources"] == [
                (
                    "#affected+infected+per100000",
                    "2020-08-06",
                    "WHO",
                    "tests/fixtures/WHO-COVID-19-global-data.csv",
                ),
                (
                    "#affected+killed+per100000",
                    "2020-08-06",
                    "WHO",
                    "tests/fixtures/WHO-COVID-19-global-data.csv",
                ),
                (
                    "#affected+infected+2+per100000",
                    "2020-08-06",
                    "WHO",
                    "tests/fixtures/WHO-COVID-19-global-data.csv",
                ),
                (
                    "#affected+killed+2+per100000",
                    "2020-08-06",
                    "WHO",
                    "tests/fixtures/WHO-COVID-19-global-data.csv",
                ),
            ]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["access"],
            )
            assert results["headers"] == (
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
            assert results["values"] == [
                {"AFG": 0.2},
                {"AFG": "N/A"},
                {"AFG": "20"},
                {"AFG": "2"},
                {"AFG": "22"},
                {"AFG": 0.5710000000000001},
                {"AFG": 0.04},
                {"AFG": "bivalent Oral Poliovirus"},
                {"AFG": "Postponed"},
                {"AFG": 9979405},
            ]
            assert results["sources"] == [
                (
                    "#access+visas+pct",
                    "2020-10-01",
                    "OCHA",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#access+travel+pct",
                    "2020-10-01",
                    "OCHA",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#event+year+previous+num",
                    "2020-10-01",
                    "Aid Workers Database",
                    "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
                ),
                (
                    "#event+year+todate+num",
                    "2020-10-01",
                    "Aid Workers Database",
                    "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
                ),
                (
                    "#event+year+previous+todate+num",
                    "2020-10-01",
                    "Aid Workers Database",
                    "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
                ),
                (
                    "#activity+cerf+project+insecurity+pct",
                    "2020-10-01",
                    "UNCERF",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#activity+cbpf+project+insecurity+pct",
                    "2020-10-01",
                    "UNCERF",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#service+name",
                    "2020-10-01",
                    "Multiple sources",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#status+name",
                    "2020-10-01",
                    "Multiple sources",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
                (
                    "#population+education",
                    "2020-10-01",
                    "UNESCO",
                    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
                ),
            ]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["sadd"],
            )
            assert results["headers"] == (
                [
                    "Cases (% male)",
                    "Cases (% female)",
                    "Deaths (% male)",
                    "Deaths (% female)",
                ],
                [
                    "#affected+infected+m+pct",
                    "#affected+f+infected+pct",
                    "#affected+killed+m+pct",
                    "#affected+f+killed+pct",
                ],
            )
            assert results["values"] == [
                {"AFG": "0.7044"},
                {"AFG": "0.2956"},
                {"AFG": "0.7498"},
                {"AFG": "0.2502"},
            ]
            assert results["sources"] == [
                (
                    "#affected+infected+m+pct",
                    "2020-08-07",
                    "SADD",
                    "tests/fixtures/covid-19-sex-disaggregated-data.csv",
                ),
                (
                    "#affected+f+infected+pct",
                    "2020-08-07",
                    "SADD",
                    "tests/fixtures/covid-19-sex-disaggregated-data.csv",
                ),
                (
                    "#affected+killed+m+pct",
                    "2020-08-07",
                    "SADD",
                    "tests/fixtures/covid-19-sex-disaggregated-data.csv",
                ),
                (
                    "#affected+f+killed+pct",
                    "2020-08-07",
                    "SADD",
                    "tests/fixtures/covid-19-sex-disaggregated-data.csv",
                ),
            ]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG", "PHL"],
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
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG", "PHL"],
                adminone,
                downloader,
                today=today,
                scrapers=["covidtests"],
                population_lookup=population_lookup,
            )
            assert results["headers"] == (
                [
                    "New Tests",
                    "New Tests Per Thousand",
                    "New Tests Per Thousand (7-day)",
                    "Positive Test Rate",
                ],
                [
                    "#affected+tested",
                    "#affected+tested+per1000",
                    "#affected+tested+avg+per1000",
                    "#affected+tested+positive+pct",
                ],
            )
            assert results["values"] == [
                {"PHL": 39611},
                {"PHL": 0.361},
                {"PHL": 0.312},
                {"PHL": 0.072},
            ]
            assert results["sources"] == [
                (
                    "#affected+tested",
                    "2020-10-01",
                    "Our World in Data",
                    "https://data.humdata.org/dataset/total-covid-19-tests-performed-by-country",
                ),
                (
                    "#affected+tested+per1000",
                    "2020-10-01",
                    "Our World in Data",
                    "https://data.humdata.org/dataset/total-covid-19-tests-performed-by-country",
                ),
                (
                    "#affected+tested+avg+per1000",
                    "2020-10-01",
                    "Our World in Data",
                    "https://data.humdata.org/dataset/total-covid-19-tests-performed-by-country",
                ),
                (
                    "#affected+tested+positive+pct",
                    "2020-10-01",
                    "Our World in Data",
                    "https://data.humdata.org/dataset/total-covid-19-tests-performed-by-country",
                ),
            ]
            today = parse_date("2021-05-03")
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG", "PHL"],
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
            assert results["values"] == [{"AFG": "240000"}]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2021-05-03",
                    "Our World in Data",
                    "tests/fixtures/ourworldindata_vaccinedoses.csv",
                )
            ]
            assert results["fallbacks"] == list()
            fallback_data = load_json(fallback_path)
            data = fallback_data["national_data"]
            sources = fallback_data["sources"]
            fallbacks = {
                "data": data,
                "admin hxltag": "#country+code",
                "sources": sources,
                "sources hxltags": [
                    "#indicator+name",
                    "#date",
                    "#meta+source",
                    "#meta+url",
                ],
            }
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG", "PHL"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                fallbacks=fallbacks,
                scrapers=["testbrokenurl"],
            )
            assert results["headers"] == (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
            assert results["values"] == [{"AFG": "230000"}]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2020-09-01",
                    "Our World in Data",
                    "tests/fixtures/fallbacks.json",
                )
            ]
            assert results["fallbacks"] == ["testbrokenurl"]

            today = parse_date("2020-10-01")
            level = "subnational"
            scraper_configuration = configuration[f"scraper_{level}"]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["gam"],
            )
            assert results["headers"] == (
                ["Malnutrition Estimate"],
                ["#severity+malnutrition+num+subnational"],
            )
            assert results["values"] == [
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
            assert results["sources"] == [
                (
                    "#severity+malnutrition+num+subnational",
                    "2020-10-01",
                    "UNICEF",
                    "tests/fixtures/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
                )
            ]
            assert results["fallbacks"] == list()
            scraper_configuration = configuration["other"]
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                scrapers=["gam"],
            )
            assert results["headers"] == (
                ["Malnutrition Estimate"],
                ["#severity+malnutrition+num+subnational"],
            )
            assert results["values"] == [{"AF09": 1.524646, "AF24": 1.043487}]
            assert results["sources"] == [
                (
                    "#severity+malnutrition+num+subnational",
                    "2020-10-01",
                    "UNICEF",
                    "tests/fixtures/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
                )
            ]
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
            assert results["headers"] == (
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
            assert results["headers"] == (
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