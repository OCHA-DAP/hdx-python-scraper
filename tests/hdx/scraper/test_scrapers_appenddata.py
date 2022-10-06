from copy import deepcopy

from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.outputs.update_tabs import update_sources
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import create_source_configuration

from .conftest import run_check_scrapers


# Test that scrapers can add to output from previous scrapers
# Also test producing sources per admin unit eg. #targeted+total+eth
class TestScrapersAppendData:
    def test_get_key_figures(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration["scraper_append_data"]
        iso3s = (
            "ETH",
            "KEN",
            "SOM",
        )
        runner = Runner(iso3s, today)
        names = ["key_figures_som", "key_figures_eth", "key_figures_ken"]
        keys = runner.add_configurables(
            scraper_configuration,
            level,
            source_configuration=create_source_configuration(
                admin_sources=True
            ),
        )
        assert keys == names

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
                "#value+funding+required+usd+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#value+funding+total+usd+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#value+funding+pct+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#inneed+total+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#targeted+total+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#targeted+pct+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#reached+total+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#reached+pct+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+food+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+sam+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+mam+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+gam+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+idps+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#affected+water+som",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ),
            (
                "#value+funding+required+usd+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#value+funding+total+usd+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#value+funding+pct+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#inneed+total+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#targeted+total+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#targeted+pct+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#reached+total+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#reached+pct+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+food+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+sam+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+mam+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+gam+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+idps+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#affected+water+eth",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/eth-key-figures-2022",
            ),
            (
                "#value+funding+required+usd+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#value+funding+total+usd+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#value+funding+pct+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#inneed+total+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#targeted+total+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#targeted+pct+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#reached+total+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#reached+pct+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+food+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+sam+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+mam+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+gam+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+idps+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+water+ken",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
        ]
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/eth-key-figures-2022",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ],
        )

    def test_sourceoverwrite(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration["scraper_append_data"]
        iso3s = (
            "ETH",
            "KEN",
            "SOM",
        )
        runner = Runner(iso3s, today)
        names = ["key_figures_som", "key_figures_eth", "key_figures_ken"]
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == names

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
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#value+funding+total+usd",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#value+funding+pct",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#inneed+total",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#targeted+total",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#targeted+pct",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#reached+total",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#reached+pct",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+food",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+sam",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+mam",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+gam",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+idps",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
            (
                "#affected+water",
                "2020-10-01",
                "multiple",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
            ),
        ]

        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/eth-key-figures-2022",
                "https://data.humdata.org/dataset/ken-key-figures-2022",
                "https://data.humdata.org/dataset/som-key-figures-2022",
            ],
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], ["sources"])
        outputs = {"json": jsonout}
        additional_sources = deepcopy(configuration["additional_sources"])
        additional_sources.append(
            {
                "indicator": "#value+funding+required+usd",
                "source_date": "2020-10-01",
                "source": "multiple",
                "source_url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            }
        )
        update_sources(
            runner,
            outputs,
            configuration["additional_sources"],
            secondary_runner=runner,  # to check we don't get duplicate sources
        )
        assert jsonout.json["sources_data"] == [
            {
                "#date": "2022-02-24",
                "#indicator+name": "#date+start+conflict",
                "#meta+source": "Meduza",
                "#meta+url": "https://meduza.io/en/news/2022/02/24/putin-announces-start-of-military-operation-in-eastern-ukraine",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#value+funding+required+usd",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#value+funding+total+usd",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#value+funding+pct",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#inneed+total",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#targeted+total",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#targeted+pct",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#reached+total",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#reached+pct",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+food",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+sam",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+mam",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+gam",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+idps",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
            {
                "#date": "2020-10-01",
                "#indicator+name": "#affected+water",
                "#meta+source": "multiple",
                "#meta+url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            },
        ]
