from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .conftest import check_scrapers, run_check_scraper
from .unhcr_myanmar_idps import idps_post_run


class TestScrapersNational:
    def test_get_national_afg(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        iso3s = ("AFG",)
        runner = Runner(iso3s, adminone, today)
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == [
            "population",
            "who_national",
            "who_national2",
            "access",
            "sadd",
            "ourworldindata",
            "broken_owd_url",
            "covidtests",
            "idps",
            "casualties",
        ]

        def assert_auth_header(name, expected):
            downloader = Retrieve.get_retriever(name).downloader
            assert downloader.session.headers.get("Authorization") == expected

        assert_auth_header("population", "pop_12345")
        assert_auth_header("who_national", "who_abc")

        def assert_auth(name, expected):
            downloader = Retrieve.get_retriever(name).downloader
            assert downloader.session.auth == expected

        assert_auth("access", ("acc_12345", "acc_abc"))
        assert_auth("who_national2", ("who_def", "who_12345"))

        def assert_params(name, expected):
            downloader = Retrieve.get_retriever(name).downloader
            assert downloader.session.params == expected

        assert_params("sadd", {"user": "sadd_123", "pass": "sadd_abc"})
        assert_params("ourworldindata", {"auth": "owid_abc"})

        name = "population"
        headers = (["Population"], ["#population"])
        values = [{"AFG": 38041754}]
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
            values,
            sources,
            population_lookup=values[0],
            source_urls=[
                "https://data.humdata.org/organization/world-bank-group"
            ],
        )

        names = ("who_national", "who_national2")
        headers = (
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
        values = [
            {"AFG": "96.99"},
            {"AFG": "3.41"},
            {"AFG": "96.99"},
            {"AFG": "3.41"},
        ]
        sources = [
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
            (
                "#affected+infected+2+per100000",
                "2020-08-06",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
            (
                "#affected+killed+2+per100000",
                "2020-08-06",
                "WHO",
                "https://covid19.who.int/WHO-COVID-19-global-data.csv",
            ),
        ]
        runner.run(names)
        check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://covid19.who.int/WHO-COVID-19-global-data.csv"
            ],
        )

        def passthrough_fn(x):
            return x

        fns = (passthrough_fn,)
        rows = runner.get_rows(
            "national", iso3s, (("iso3",), ("#country+code",)), fns, names
        )
        assert rows == [
            [
                "iso3",
                "CasesPer100000",
                "DeathsPer100000",
                "Cases2Per100000",
                "Deaths2Per100000",
            ],
            [
                "#country+code",
                "#affected+infected+per100000",
                "#affected+killed+per100000",
                "#affected+infected+2+per100000",
                "#affected+killed+2+per100000",
            ],
            ["AFG", "96.99", "3.41", "96.99", "3.41"],
        ]
        combined_sources = [
            (
                "#date+start+conflict",
                "2022-02-24",
                "Meduza",
                "https://meduza.io/en/news/2022/02/24/putin-announces-start-of-military-operation-in-eastern-ukraine",
            ),
        ] + sources
        assert (
            runner.get_sources(
                additional_sources=configuration["additional_sources"]
            )
            == combined_sources
        )
        runner.set_not_run_many(names)

        name = "access"
        headers = (
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
        values = [
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
        sources = [
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
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/security-incidents-on-aid-workers",
                "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv",
            ],
        )

        name = "sadd"
        headers = (
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
        values = [
            {"AFG": "0.7044"},
            {"AFG": "0.2956"},
            {"AFG": "0.7498"},
            {"AFG": "0.2502"},
        ]
        sources = [
            (
                "#affected+infected+m+pct",
                "2020-08-07",
                "SADD",
                "https://globalhealth5050.org/?_covid-data=dataset-fullvars&_extype=csv",
            ),
            (
                "#affected+f+infected+pct",
                "2020-08-07",
                "SADD",
                "https://globalhealth5050.org/?_covid-data=dataset-fullvars&_extype=csv",
            ),
            (
                "#affected+killed+m+pct",
                "2020-08-07",
                "SADD",
                "https://globalhealth5050.org/?_covid-data=dataset-fullvars&_extype=csv",
            ),
            (
                "#affected+f+killed+pct",
                "2020-08-07",
                "SADD",
                "https://globalhealth5050.org/?_covid-data=dataset-fullvars&_extype=csv",
            ),
        ]
        run_check_scraper(name, runner, level, headers, values, sources)

    def test_get_national_ukr(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        runner = Runner(("UKR",), adminone, today)
        runner.add_configurables(scraper_configuration, level)

        name = "casualties"
        headers = (
            ["CiviliansKilled", "CiviliansInjured"],
            ["#affected+killed", "#affected+injured"],
        )
        values = [{"UKR": "351"}, {"UKR": "707"}]
        sources = [
            (
                "#affected+killed",
                "2020-10-01",
                "OHCHR",
                "https://data.humdata.org/dataset/ukraine-key-figures-2022",
            ),
            (
                "#affected+injured",
                "2020-10-01",
                "OHCHR",
                "https://data.humdata.org/dataset/ukraine-key-figures-2022",
            ),
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/ukraine-key-figures-2022"
            ],
        )

    def test_get_national_afg_phl(self, configuration, fallbacks):
        BaseScraper.population_lookup = {"AFG": 38041754}
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]
        runner = Runner(("AFG", "PHL"), adminone, today)
        runner.add_configurables(scraper_configuration, level)

        name = "ourworldindata"
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
        values = [dict(), dict()]
        sources = [
            (
                "#capacity+doses+administered+total",
                "2020-10-01",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            ),
            (
                "#capacity+doses+administered+coverage+pct",
                "2020-10-01",
                "Our World in Data",
                "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view",
            ),
        ]
        run_check_scraper(name, runner, level, headers, values, sources)

        name = "covidtests"
        headers = (
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
        values = [
            {"PHL": 39611},
            {"PHL": 0.361},
            {"PHL": 0.312},
            {"PHL": 0.072},
        ]
        sources = [
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
        run_check_scraper(name, runner, level, headers, values, sources)

        today = parse_date("2021-05-03")
        errors_on_exit = ErrorsOnExit()
        runner = Runner(
            ("AFG", "PHL"),
            adminone,
            today,
            errors_on_exit=errors_on_exit,
        )
        runner.add_configurables(scraper_configuration, level)
        name = "ourworldindata"
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
        values = [{"AFG": 240000}, {"AFG": "0.0032"}]
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
        run_check_scraper(name, runner, level, headers, values, sources)

        # Test fallbacks
        name = "broken_owd_url"
        headers = (
            ["TotalDosesAdministered"],
            ["#capacity+doses+administered+total"],
        )
        values = [{"AFG": "230000"}]
        sources = [
            (
                "#capacity+doses+administered+total",
                "2020-09-01",
                "Our World in Data",
                "tests/fixtures/fallbacks.json",
            )
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
        assert errors_on_exit.errors == [
            "Using fallbacks for broken_owd_url! Error: [scheme-error] The data source could not be successfully loaded: [Errno 2] No such file or directory: 'tests/fixtures/broken_owd_url_notexist.csv'"
        ]

    def test_get_national_afg_mmr_phl(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        today = parse_date("2021-05-03")
        errors_on_exit = ErrorsOnExit()
        adminone = AdminOne(configuration)
        level = "national"
        scraper_configuration = configuration[f"scraper_{level}"]

        runner = Runner(
            ("AFG", "MMR", "PHL"),
            adminone,
            today,
            errors_on_exit=errors_on_exit,
        )
        runner.add_configurables(scraper_configuration, level)
        name = "idps"
        headers = (
            ["TotalIDPs"],
            ["#affected+displaced"],
        )
        values = [{"AFG": 4664000, "MMR": 509600, "PHL": 298000}]
        sources = [
            (
                "#affected+displaced",
                "2020-12-31",
                "IDMC",
                "https://data.humdata.org/dataset/idmc-internally-displaced-persons-idps",
            )
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/idmc-internally-displaced-persons-idps"
            ],
        )

        runner.add_instance_variables(
            "idps", overrideinfo=configuration["unhcr_myanmar_idps"]
        )
        runner.add_post_run("idps", idps_post_run)
        values = [{"AFG": 4664000, "MMR": 569591, "PHL": 298000}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/idmc-internally-displaced-persons-idps",
                "https://data.unhcr.org/population/?widget_id=264111&geo_id=693&population_group=5407,4999",
            ],
        )
        assert errors_on_exit.errors == list()
        runner = Runner(
            ("AFG", "MMR", "PHL"),
            adminone,
            today,
            errors_on_exit=errors_on_exit,
        )
        runner.add_configurables(scraper_configuration, level)
        runner.add_instance_variables(
            "idps", overrideinfo={"url": "NOT EXIST"}
        )
        runner.add_post_run("idps", idps_post_run)
        values = [{"AFG": 4664000, "MMR": 509600, "PHL": 298000}]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/idmc-internally-displaced-persons-idps"
            ],
        )
        assert errors_on_exit.errors == [
            "Not using UNHCR Myanmar IDPs override! Error: [Errno 2] No such file or directory: 'tests/fixtures/idps_override_not-exist.json'",
        ]
