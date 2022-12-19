from copy import deepcopy

import pytest
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.sources import Sources
from hdx.scraper.utilities.writer import Writer

from .conftest import run_check_scrapers


# Test that scrapers can add to output from previous scrapers
# Also test producing sources per admin unit eg. #targeted+total+eth
class TestScrapersAppendData:
    @pytest.fixture
    def iso3s(self):
        return (
            "ETH",
            "KEN",
            "SOM",
        )

    @pytest.fixture
    def headers(self):
        return (
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

    @pytest.fixture
    def values(self):
        return [
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

    @staticmethod
    def get_expected_sources(
        headers, date, countryiso3, country_attribute=False
    ):
        sources = list()
        countryname = Country.get_country_name_from_iso3(countryiso3).lower()
        for hxltag in headers[1]:
            if country_attribute:
                hxltag = f"{hxltag}+{countryiso3.lower()}"
            sources.append(
                (
                    hxltag,
                    date,
                    "Multiple Source (Humanitarian Partners)",
                    f"https://data.humdata.org/dataset/{countryname}-drought-related-key-figures",
                )
            )
        return sources

    @staticmethod
    def get_expected_sources_data(headers, date, countryiso3):
        sources_data = [
            {
                "#date": "Feb 24, 2022",
                "#indicator+name": "#date+start+conflict",
                "#meta+source": "Meduza",
                "#meta+url": "https://meduza.io/en/news/2022/02/24/putin-announces-start-of-military-operation-in-eastern-ukraine",
            }
        ]
        countryname = Country.get_country_name_from_iso3(countryiso3).lower()
        for hxltag in headers[1]:
            sources_data.append(
                {
                    "#date": date,
                    "#indicator+name": hxltag,
                    "#meta+source": "Multiple Source (Humanitarian Partners)",
                    "#meta+url": f"https://data.humdata.org/dataset/{countryname}-drought-related-key-figures",
                },
            )
        return sources_data

    def test_get_key_figures(self, configuration, iso3s, headers, values):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration["scraper_append_data"]
        runner = Runner(iso3s, today)
        names = ["key_figures_eth", "key_figures_ken", "key_figures_som"]
        keys = runner.add_configurables(
            scraper_configuration,
            level,
            source_configuration=Sources.create_source_configuration(
                admin_sources=True
            ),
        )
        assert keys == names

        sources = self.get_expected_sources(
            headers, "Nov 1, 2022", "ETH", True
        )
        sources += self.get_expected_sources(
            headers, "Oct 1, 2020", "KEN", True
        )
        sources += self.get_expected_sources(
            headers, "Oct 1, 2020", "SOM", True
        )
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            sources,
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
            ],
            set_not_run=False,
        )

        names = runner.add_aggregators(
            True,
            configuration["aggregate_append_data"],
            "national",
            "regional",
            iso3s,
            force_add_to_run=True,
        )
        assert names == [
            "affected_sam_regional",
            "affected_mam_regional",
            "affected_gam_regional",
            "affected_water_regional",
        ]
        reg_headers = (
            [
                "SAM",
                "MAM",
                "GAM",
                "Water Insecurity",
            ],
            [
                "#affected+sam",
                "#affected+mam",
                "#affected+gam",
                "#affected+water",
            ],
        )
        reg_values = [
            {"value": 1313619},
            {"value": 3257327},
            {"value": 4570946},
            {"value": 16200000},
        ]
        reg_sources = [
            (
                "#affected+sam",
                "Oct 2021-Nov 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset?groups=eth&groups=ken&groups=som&organization=ocha-rosea&vocab_Topics=droughts&q=&sort=score%20desc%2C%20if(gt(last_modified%2Creview_date)%2Clast_modified%2Creview_date)%20desc&ext_page_size=25",
            ),
            (
                "#affected+mam",
                "01-10-2021:01-11-2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset?groups=eth&groups=ken&groups=som&organization=ocha-rosea&vocab_Topics=droughts&q=&sort=score%20desc%2C%20if(gt(last_modified%2Creview_date)%2Clast_modified%2Creview_date)%20desc&ext_page_size=25",
            ),
            (
                "#affected+gam",
                "Nov 1, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset?groups=eth&groups=ken&groups=som&organization=ocha-rosea&vocab_Topics=droughts&q=&sort=score%20desc%2C%20if(gt(last_modified%2Creview_date)%2Clast_modified%2Creview_date)%20desc&ext_page_size=25",
            ),
            (
                "#affected+water",
                "Nov 1, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset?groups=eth&groups=ken&groups=som&organization=ocha-rosea&vocab_Topics=droughts&q=&sort=score%20desc%2C%20if(gt(last_modified%2Creview_date)%2Clast_modified%2Creview_date)%20desc&ext_page_size=25",
            ),
        ]
        run_check_scrapers(
            names,
            runner,
            "regional",
            reg_headers,
            reg_values,
            reg_sources,
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
                "https://data.humdata.org/dataset?groups=eth&groups=ken&groups=som&organization=ocha-rosea&vocab_Topics=droughts&q=&sort=score%20desc%2C%20if(gt(last_modified%2Creview_date)%2Clast_modified%2Creview_date)%20desc&ext_page_size=25",
            ],
        )

    def test_sourceoverwrite(self, configuration, iso3s, headers, values):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration["scraper_append_data"]
        runner = Runner(iso3s, today)
        names = ["key_figures_eth", "key_figures_ken", "key_figures_som"]
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == names
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            self.get_expected_sources(headers, "Nov 1, 2022", "ETH"),
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
            ],
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], ["sources"])
        outputs = {"json": jsonout}
        additional_sources = deepcopy(configuration["additional_sources"])
        source_url = "https://data.humdata.org/dataset/kenya-drought-related-key-figures"
        additional_sources.append(
            {
                "indicator": "#value+funding+required+usd",
                "source_date": "Nov 1, 2022",
                "source": "Multiple Source (Humanitarian Partners)",
                "source_url": source_url,
            }
        )
        writer = Writer(runner, outputs)
        writer.update_sources(
            additional_sources,
            secondary_runner=runner,  # to check we don't get duplicate sources
        )
        result = self.get_expected_sources_data(headers, "Nov 1, 2022", "ETH")
        result[1]["#meta+url"] = source_url
        assert jsonout.json["sources_data"] == result

        Sources.set_should_overwrite_sources(True)
        runner = Runner(iso3s, today)
        names = ["key_figures_eth", "key_figures_ken", "key_figures_som"]
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == names
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            self.get_expected_sources(headers, "Oct 1, 2020", "SOM"),
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
            ],
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], ["sources"])
        outputs = {"json": jsonout}
        additional_sources = deepcopy(configuration["additional_sources"])
        additional_sources.append(
            {
                "indicator": "#value+funding+required+usd",
                "source_date": "Oct 1, 2020",
                "source": "multiple",
                "source_url": "https://data.humdata.org/dataset/ken-key-figures-2022",
            }
        )
        writer = Writer(runner, outputs)
        writer.update_sources(
            additional_sources,
            secondary_runner=runner,  # to check we don't get duplicate sources
        )
        assert jsonout.json["sources_data"] == self.get_expected_sources_data(
            headers, "Oct 1, 2020", "SOM"
        )
        Sources.set_should_overwrite_sources(False)

    def test_source_overwrite_yaml(
        self, configuration, iso3s, headers, values
    ):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration[
            "scraper_append_data_should_overwrite"
        ]
        runner = Runner(iso3s, today)
        names = ["key_figures_eth", "key_figures_ken", "key_figures_som"]
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == names
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            self.get_expected_sources(headers, "Oct 1, 2020", "KEN"),
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
            ],
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], ["sources"])
        outputs = {"json": jsonout}
        additional_sources = deepcopy(configuration["additional_sources"])
        additional_sources.append(
            {
                "indicator": "#value+funding+required+usd",
                "source_date": "Nov 1, 2022",
                "source": "Multiple Source (Humanitarian Partners)",
                "source_url": "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
            }
        )
        writer = Writer(runner, outputs)
        writer.update_sources(
            additional_sources,
            secondary_runner=runner,  # to check we don't get duplicate sources
        )
        assert jsonout.json["sources_data"] == self.get_expected_sources_data(
            headers, "Oct 1, 2020", "KEN"
        )

    def test_additional_source_overwrite(
        self, configuration, iso3s, headers, values
    ):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        level = "national"
        scraper_configuration = configuration[
            "scraper_append_data_should_overwrite"
        ]
        runner = Runner(iso3s, today)
        names = ["key_figures_eth", "key_figures_ken", "key_figures_som"]
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == names
        run_check_scrapers(
            names,
            runner,
            level,
            headers,
            values,
            self.get_expected_sources(headers, "Oct 1, 2020", "KEN"),
            source_urls=[
                "https://data.humdata.org/dataset/ethiopia-drought-related-key-figures",
                "https://data.humdata.org/dataset/kenya-drought-related-key-figures",
                "https://data.humdata.org/dataset/somalia-drought-related-key-figures",
            ],
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], ["sources"])
        outputs = {"json": jsonout}
        additional_sources = deepcopy(configuration["additional_sources"])
        source_date = "Nov 1, 2022"
        source_url = "https://data.humdata.org/dataset/kenya-drought-related-key-figures"
        additional_sources.append(
            {
                "indicator": "#value+funding+required+usd",
                "source_date": source_date,
                "source": "Multiple Source (Humanitarian Partners)",
                "source_url": source_url,
                "should_overwrite_source": True,
            }
        )
        writer = Writer(runner, outputs)
        writer.update_sources(
            additional_sources,
            secondary_runner=runner,  # to check we don't get duplicate sources
        )
        result = self.get_expected_sources_data(headers, "Oct 1, 2020", "KEN")
        result[1]["#date"] = source_date
        result[1]["#meta+url"] = source_url
        assert jsonout.json["sources_data"] == result
