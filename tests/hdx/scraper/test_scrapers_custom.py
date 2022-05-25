from copy import deepcopy

from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .conftest import check_scraper, check_scrapers
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment


class TestScrapersCustom:
    def test_get_custom(self, configuration, fallbacks):
        BaseScraper.population_lookup = dict()
        source_date = "2022-04-30"
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "national"
        countries = ("AFG",)

        class Region:
            iso3_to_region_and_hrp = {"AFG": ("ROAP",)}

        region = Region()
        runner = Runner(("AFG",), adminone, today)
        datasetinfo = configuration["education_closures"]
        education_closures = EducationClosures(
            datasetinfo, today, countries, region
        )
        runner.add_custom(education_closures)
        runner.run()
        name = education_closures.name
        headers = (["School Closure"], ["#impact+type"])
        values = [{"AFG": "Closed due to COVID-19"}]
        sources = [
            (
                "#impact+type",
                source_date,
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            )
        ]
        check_scraper(name, runner, "national", headers, values, sources)
        headers = (["No. closed countries"], ["#status+country+closed"])
        values = [{"ROAP": 1}]
        sources = [
            (
                "#status+country+closed",
                source_date,
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            )
        ]
        check_scraper(name, runner, "regional", headers, values, sources)

        scraper_configuration = configuration[f"scraper_{level}"]
        runner.add_configurables(scraper_configuration, level)
        runner.run_one("population")
        check_scraper(name, runner, "regional", headers, values, sources)
        names = ("population", name)
        assert (
            runner.get_headers(
                names=names,
                levels=("regional",),
                hxltags=("#population", "#status+country+closed"),
            )["regional"]
            == headers
        )
        headers = (
            ["Population", "School Closure"],
            ["#population", "#impact+type"],
        )
        values = [{"AFG": 38041754}, {"AFG": "Closed due to COVID-19"}]
        sources = [
            (
                "#population",
                "2020-10-01",
                "World Bank",
                "https://data.humdata.org/organization/world-bank-group",
            ),
            (
                "#impact+type",
                source_date,
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
        ]
        assert (
            runner.get_headers(
                names=names,
                levels=("national",),
                headers=("Population", "School Closure"),
            )["national"]
            == headers
        )
        check_scrapers(
            names,
            runner,
            "national",
            headers,
            values,
            sources,
        )
        assert runner.get_source_urls() == [
            "https://data.humdata.org/dataset/global-school-closures-covid19",
            "https://data.humdata.org/organization/world-bank-group",
        ]

        runner = Runner(("AFG",), adminone, today)
        datasetinfo = deepcopy(datasetinfo)
        datasetinfo["url"] = "NOTEXIST.csv"
        education_closures = EducationClosures(
            datasetinfo, today, countries, Region()
        )
        runner.add_custom(education_closures)
        runner.run()
        name = education_closures.name
        headers = (["School Closure"], ["#impact+type"])
        values = [{"AFG": "Closed due to COVID-19"}]
        sources = [
            (
                "#impact+type",
                "2020-09-01",
                "UNESCO",
                "tests/fixtures/fallbacks.json",
            )
        ]
        check_scraper(
            name,
            runner,
            "national",
            headers,
            values,
            sources,
            fallbacks_used=True,
        )
        headers = (["No. closed countries"], ["#status+country+closed"])
        values = [{"ROAP": 3}]
        sources = [
            (
                "#status+country+closed",
                "2020-09-01",
                "UNESCO",
                "tests/fixtures/fallbacks.json",
            )
        ]
        check_scraper(
            name,
            runner,
            "regional",
            headers,
            values,
            sources,
            fallbacks_used=True,
        )

        datasetinfo = configuration["education_enrolment"]
        education_enrolment = EducationEnrolment(
            datasetinfo, education_closures, countries, region
        )
        runner.add_custom(education_enrolment)
        runner.run()
        name = education_enrolment.name
        headers = (
            [
                "No. pre-primary to upper-secondary learners",
                "No. tertiary learners",
                "No. affected learners",
            ],
            [
                "#population+learners+pre_primary_to_secondary",
                "#population+learners+tertiary",
                "#affected+learners",
            ],
        )
        values = [{"AFG": 9865894}, {"AFG": 430980}, {"AFG": 10296874}]
        sources = [
            (
                "#population+learners+pre_primary_to_secondary",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#population+learners+tertiary",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#affected+learners",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
        ]
        check_scraper(name, runner, "national", headers, values, sources)
        headers = (
            ["No. affected learners", "Percentage affected learners"],
            ["#affected+learners", "#affected+learners+pct"],
        )
        values = [{"ROAP": 10296874}, {"ROAP": "1.0000"}]
        sources = [
            (
                "#affected+learners",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#affected+learners+pct",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
        ]
        check_scraper(name, runner, "regional", headers, values, sources)

        # closures used fallbacks so there is no no url from that scraper
        assert runner.get_source_urls() == [
            "https://data.humdata.org/dataset/global-school-closures-covid19"
        ]

        datasetinfo = configuration["education_closures"]
        education_closures = EducationClosures(
            datasetinfo, today, countries, region
        )
        runner.add_custom(education_closures)
        runner.run()
        assert runner.get_source_urls() == [
            "https://data.humdata.org/dataset/global-school-closures-covid19",
        ]

        assert runner.get_scraper_names() == [
            "education_closures",
            "education_enrolment",
        ]
        runner.prioritise_scrapers(("education_enrolment",))
        assert runner.get_scraper_names() == [
            "education_enrolment",
            "education_closures",
        ]

        education_closures.add_hxltag_source("test", "#lala")
        sources = education_closures.get_sources("test")
        assert sources == [
            (
                "#lala",
                "2022-04-30",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            )
        ]
