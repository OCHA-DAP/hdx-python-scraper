from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .conftest import run_check_scraper


class TestScrapersMultipleURLs:
    def test_get_key_figures(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration["scraper_multiple_urls"]
        iso3s = (
            "ETH",
            "KEN",
            "SOM",
        )
        runner = Runner(iso3s, today)
        name = "key_figures"
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == [name]

        headers = (
            [
                "Total Fund Requirement",
                "Funded",
                "Funded %",
                "PiN",
                "Total Targeted",
                "Targeted %",
                "Total Reached",
                "Reached %",
                "Food Insecurity",
                "SAM",
                "MAM",
                "GAM",
                "Internal Displacement",
                "Water Insecurity",
            ],
            [
                "#value+funding+required+usd",
                "#value+funding+total+usd",
                "#value+funding+pct",
                "#inneed+total",
                "#targeted+total",
                "#targeted+pct",
                "#reached+total",
                "#reached+pct",
                "#affected+food",
                "#affected+sam",
                "#affected+mam",
                "#affected+gam",
                "#affected+idps",
                "#affected+water",
            ],
        )
        values = [
            {"ETH": "1658000000", "KEN": "250461585", "SOM": "1457795661"},
            {"ETH": "700000000", "KEN": "329014517", "SOM": "1147579629"},
            {"ETH": "0.42", "KEN": "1.31", "SOM": "0.79"},
            {"ETH": "24106527", "KEN": "4200000", "SOM": "7800000"},
            {"ETH": "16986639", "KEN": "2000000", "SOM": "6400000"},
            {"ETH": "0.70", "KEN": "0.48", "SOM": "0.82"},
            {"ETH": "13800000", "KEN": "533694", "SOM": "4100000"},
            {"ETH": "0.81", "KEN": "0.27", "SOM": "0.64"},
            {"ETH": "9873983.98", "KEN": "3500000", "SOM": "7100000"},
            {"ETH": "704499", "KEN": "222720", "SOM": "386400"},
            {"ETH": "1481983", "KEN": "661744", "SOM": "1113600"},
            {"ETH": "2186482", "KEN": "884464", "SOM": "1500000"},
            {"ETH": "595717", "KEN": None, "SOM": "1001700"},
            {"ETH": "8200000", "KEN": "4100000", "SOM": "3900000"},
        ]
        sources = [
            (
                "#value+funding+required+usd",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#value+funding+total+usd",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#value+funding+pct",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#inneed+total",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#targeted+total",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#targeted+pct",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#reached+total",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#reached+pct",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+food",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+sam",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+mam",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+gam",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+idps",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
            (
                "#affected+water",
                "Oct 01, 2020",
                "multiple",
                "https://data.humdata.org/dataset/key-figures-2022",
            ),
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=["https://data.humdata.org/dataset/key-figures-2022"],
        )
