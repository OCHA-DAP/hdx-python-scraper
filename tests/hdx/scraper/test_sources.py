import pytest
from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.scraper import ConfigurableScraper
from hdx.scraper.utilities.sources import Sources


class TestSources:
    @pytest.fixture(scope="class")
    def startdate(self):
        return parse_date("2021-09-23")

    @pytest.fixture(scope="class")
    def enddate(self):
        return parse_date("2022-01-01")

    def test_set_defaults(self):
        format = Sources.default_source_date_format
        new_format = "%y-%m-%d"
        Sources.set_default_source_date_format(new_format)
        assert Sources.default_source_date_format == new_format
        Sources.set_default_source_date_format(format)
        separator = Sources.default_date_range_separator
        new_separator = "/"
        Sources.set_default_date_range_separator(new_separator)
        assert Sources.default_date_range_separator == new_separator
        Sources.set_default_date_range_separator(separator)

    def test_standardise_datasetinfo_source_date(self, startdate, enddate):
        datasetinfo = {"source_date": enddate}
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result == enddate
        assert datasetinfo["source_date"] == {"default_date": {"end": enddate}}
        datasetinfo["source_date"] = {"start": startdate, "end": enddate}
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result == enddate
        assert datasetinfo["source_date"] == {
            "default_date": {"start": startdate, "end": enddate}
        }
        datasetinfo["source_date"] = {"#mytag": startdate}
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result is None
        assert datasetinfo["source_date"] is None
        datasetinfo = {
            "source_date": {"default_date": enddate, "#mytag": startdate}
        }
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result == enddate
        assert datasetinfo["source_date"] == {
            "default_date": {"end": enddate},
            "#mytag": {"end": startdate},
        }
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result == enddate
        assert datasetinfo["source_date"] == {
            "default_date": {"end": enddate},
            "#mytag": {"end": startdate},
        }
        datasetinfo["source_date"]["default_date"]["start"] = startdate
        datasetinfo["source_date"]["#mytag"]["start"] = startdate
        result = Sources.standardise_datasetinfo_source_date(datasetinfo)
        assert result == enddate
        assert datasetinfo["source_date"] == {
            "default_date": {"start": startdate, "end": enddate},
            "#mytag": {"start": startdate, "end": startdate},
        }

    def test_get_hxltag_source_date(self, startdate, enddate):
        datasetinfo = {"source_date": enddate}
        Sources.standardise_datasetinfo_source_date(datasetinfo)
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "Jan 01, 2022"
        source_date_format = "%Y-%m-%d"
        datasetinfo["source_date_format"] = source_date_format
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "2022-01-01"
        source_date_format = {"end": "%Y-%m-%d"}
        datasetinfo["source_date_format"] = source_date_format
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "2022-01-01"
        source_date_format = {"date": "%Y-%m-%d"}
        datasetinfo["source_date_format"] = source_date_format
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "2022-01-01"
        datasetinfo["source_date"] = {"start": startdate, "end": enddate}
        Sources.standardise_datasetinfo_source_date(datasetinfo)
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "2022-01-01"
        source_date_format = {
            "start": "%b %d, %Y",
            "separator": " : ",
            "end": "%b %d, %Y",
        }
        datasetinfo["source_date_format"] = source_date_format
        result = Sources.get_hxltag_source_date(datasetinfo, "default_date")
        assert result == "Sep 23, 2021 : Jan 01, 2022"

    def test_create_source_configuration(self, configuration):
        result = Sources.create_source_configuration()
        assert result is None
        suffix_attribute = "suf"
        result = Sources.create_source_configuration(
            suffix_attribute=suffix_attribute
        )
        assert result == {"suffix_attribute": "suf"}
        adminlevel = AdminLevel(configuration)
        d = adminlevel.pcode_to_iso3
        adminlevel.pcode_to_iso3 = {k: d[k] for k in list(d)[:5]}
        result = Sources.create_source_configuration(
            suffix_attribute=suffix_attribute, adminlevel=adminlevel
        )
        assert result == {"suffix_attribute": "suf"}
        result = Sources.create_source_configuration(adminlevel=adminlevel)
        assert result == {
            "admin_mapping": {
                "AF01": "AFG",
                "AF02": "AFG",
                "AF03": "AFG",
                "AF04": "AFG",
                "AF05": "AFG",
            },
            "admin_sources": True,
        }
        adminlevel2 = AdminLevel(configuration["admin1"])
        d = adminlevel2.pcode_to_iso3
        adminlevel2.pcode_to_iso3 = {k: d[k] for k in list(d)[:5]}
        result = Sources.create_source_configuration(
            adminlevel=(adminlevel, adminlevel2)
        )
        assert result == {
            "admin_mapping": {
                "AF01": "AFG",
                "AF02": "AFG",
                "AF03": "AFG",
                "AF04": "AFG",
                "AF05": "AFG",
                "ET02": "ETH",
                "ET03": "ETH",
                "ET06": "ETH",
                "ET14": "ETH",
                "ET15": "ETH",
            },
            "admin_sources": True,
        }
        admin_mapping_dict = {"MY01": "MY", "MY02": "MY"}
        result = Sources.create_source_configuration(
            admin_mapping_dict=admin_mapping_dict
        )
        assert result == {
            "admin_mapping": {"MY01": "MY", "MY02": "MY"},
            "admin_sources": True,
        }

    def test_scraper_add_sources(self, configuration, enddate):
        BaseScraper.population_lookup = dict()
        adminlevel = AdminLevel(configuration)
        level = "subnational"
        scraper = ConfigurableScraper(
            "test", dict(), level, ["AFG"], adminlevel
        )

        class MyRowParser:
            max_date = 0
            datetype = None

            @classmethod
            def get_maxdate(cls):
                return cls.max_date

        scraper.rowparser = MyRowParser
        with pytest.raises(ValueError):
            scraper.add_sources()

        MyRowParser.max_date = 1577836800
        with pytest.raises(ValueError):
            scraper.add_sources()

        scraper.datasetinfo = {
            "source": "MySource",
            "source_url": "https://myurl",
        }

        MyRowParser.datetype = "int"
        scraper.add_sources()
        assert scraper.datasetinfo["source_date"] == {
            "default_date": {"end": parse_date("2020-01-01")}
        }

        del scraper.datasetinfo["source_date"]
        date = "2019-01-01"
        MyRowParser.max_date = date
        MyRowParser.datetype = "date"
        scraper.add_sources()
        assert scraper.datasetinfo["source_date"] == {
            "default_date": {"end": parse_date("2019-01-01")}
        }
