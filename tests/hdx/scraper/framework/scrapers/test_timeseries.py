from hdx.scraper.framework.base_scraper import BaseScraper
from hdx.scraper.framework.outputs.json import JsonFile
from hdx.scraper.framework.runner import Runner
from hdx.utilities.dateparse import parse_date


class TestTimeSeries:
    def test_timeseries(self, configuration):
        BaseScraper.population_lookup = {}
        iso3s = ("AFG", "MMR")
        today = parse_date("2022-03-06")
        runner = Runner(iso3s, today)
        name = "timeseries_casualties"
        jsonout = JsonFile(configuration["json"], [name])
        outputs = {"json": jsonout}
        scraper_names = runner.add_timeseries_scrapers(
            configuration["timeseries"], outputs
        )
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
