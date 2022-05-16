from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.timeseries import TimeSeries
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.runner import Runner


class TestScrapersTimeSeries:
    def test_timeseries(self, configuration):
        BaseScraper.population_lookup = dict()
        adminone = AdminOne(configuration)
        iso3s = ("AFG", "MMR")
        today = parse_date("2022-03-06")
        runner = Runner(iso3s, adminone, today)
        name = "timeseries_casualties"
        jsonout = JsonFile(configuration["json"], [name])
        outputs = {"json": jsonout}
        scrapers = TimeSeries.get_scrapers(
            configuration["timeseries"], today, outputs
        )
        runner.add_customs(scrapers)
        scraper_names = runner.add_customs(scrapers, add_to_run=True)
        assert scraper_names == [name]
        runner.run()
        assert jsonout.json[f"{name}_data"] == [
            {
                "#affected+injured": "553",
                "#affected+killed": "249",
                "#date": "2022-03-03",
            },
            {
                "#affected+injured": "675",
                "#affected+killed": "331",
                "#date": "2022-03-04",
            },
            {
                "#affected+injured": "707",
                "#affected+killed": "351",
                "#date": "2022-03-05",
            },
        ]
