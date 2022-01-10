from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download

from hdx.scraper.scrapers import run_scrapers
from tests.hdx.scraper import get_fallbacks


class TestScraperNational:

    def test_get_tabular_national(self, configuration, fallback_data):
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

            owd_headers = (
                ["TotalDosesAdministered"],
                ["#capacity+doses+administered+total"],
            )
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
            assert results["headers"] == owd_headers
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

            # Test fallbacks
            fallbacks = get_fallbacks(fallback_data, level)
            results = run_scrapers(
                scraper_configuration,
                level,
                ["AFG", "PHL"],
                adminone,
                downloader,
                today=today,
                population_lookup=population_lookup,
                fallbacks=fallbacks,
                scrapers=["broken_owd_url"],
            )
            assert results["headers"] == owd_headers
            assert results["values"] == [{"AFG": "230000"}]
            assert results["sources"] == [
                (
                    "#capacity+doses+administered+total",
                    "2020-09-01",
                    "Our World in Data",
                    "tests/fixtures/fallbacks.json",
                )
            ]
            assert results["fallbacks"] == ["broken_owd_url"]
