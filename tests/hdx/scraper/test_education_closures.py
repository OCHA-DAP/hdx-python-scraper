from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download

from hdx.scraper.runner import Runner
from tests.hdx.scraper.conftest import check_scraper
from tests.hdx.scraper.education_closures import EducationClosures


class TestScraperEducationClosures:
    def test_get_education_closures(self, configuration, fallbacks):
        with Download(user_agent="test") as downloader:
            today_str = "2020-10-01"
            today = parse_date(today_str)
            adminone = AdminOne(configuration)
            level = "national"
            countries = ("AFG",)

            class Region:
                iso3_to_region_and_hrp = {"AFG": ("ROAP",)}

            runner = Runner(("AFG",), adminone, downloader, dict(), today)
            datasetinfo = {
                "format": "csv",
                "dataset": "global-school-closures-covid19",
                "date": today_str,
                "url": "tests/fixtures/covid_impact_education.csv",
                "resource": "School Closures",
                "headers": 1,
            }
            education_closures = EducationClosures(
                datasetinfo, today, countries, Region(), downloader
            )
            runner.add_custom(education_closures)
            runner.run()
            name = education_closures.name
            headers = (["School Closure"], ["#impact+type"])
            values = [{"AFG": "Closed due to COVID-19"}]
            sources = [
                (
                    "#impact+type",
                    today_str,
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
                    today_str,
                    "UNESCO",
                    "https://data.humdata.org/dataset/global-school-closures-covid19",
                )
            ]
            check_scraper(name, runner, "regional", headers, values, sources)

            scraper_configuration = configuration[f"scraper_{level}"]
            runner.add_configurables(scraper_configuration, level)
            runner.run_one("population")
            check_scraper(name, runner, "regional", headers, values, sources)
            headers = (
                ["School Closure", "Population"],
                ["#impact+type", "#population"],
            )
            values = [{"AFG": "Closed due to COVID-19"}, {"AFG": 38041754}]
            sources = [
                (
                    "#impact+type",
                    today_str,
                    "UNESCO",
                    "https://data.humdata.org/dataset/global-school-closures-covid19",
                ),
                (
                    "#population",
                    "2020-10-01",
                    "World Bank",
                    "https://data.humdata.org/organization/world-bank-group",
                ),
            ]
            check_scraper(name, runner, "national", headers, values, sources)

            runner = Runner(("AFG",), adminone, downloader, dict(), today)
            datasetinfo = {
                "format": "csv",
                "dataset": "global-school-closures-covid19",
                "url": "NOTEXIST.csv",
                "resource": "School Closures",
                "headers": 1,
            }
            education_closures = EducationClosures(
                datasetinfo, today, countries, Region(), downloader
            )
            runner.add_custom(education_closures)
            runner.run()
            name = education_closures.name
            headers = (["School Closure"], ["#impact+type"])
            values = [{"AFG": "Partially open"}]
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