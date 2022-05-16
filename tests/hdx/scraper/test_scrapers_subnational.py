from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.outputs.update_tabs import update_subnational
from hdx.scraper.runner import Runner

from .conftest import run_check_scraper


class TestScrapersSubnational:
    def test_get_subnational(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminone = AdminOne(configuration)
        level = "subnational"
        scraper_configuration = configuration[f"scraper_{level}"]
        runner = Runner(("AFG",), adminone, today)
        keys = runner.add_configurables(scraper_configuration, level)
        assert keys == ["gam"]

        name = "gam"
        headers = (
            ["Malnutrition Estimate"],
            ["#severity+malnutrition+num+subnational"],
        )
        values = [
            {
                "AF17": 3.371688,
                "AF31": 3.519166,
                "AF09": 1.524646,
                "AF21": 1.319626,
                "AF10": 1.40426,
                "AF24": 1.043487,
                "AF33": 2.745447,
                "AF29": 2.478977,
                "AF11": 1.022871,
                "AF23": 1.340286,
                "AF30": 1.677612,
                "AF32": 1.687488,
                "AF28": 0.6210205,
                "AF01": 1.282291,
                "AF27": 1.378641,
                "AF02": 3.552082,
                "AF14": 0.7653555,
                "AF15": 0.953823,
                "AF19": 1.684882,
                "AF07": 2.090165,
                "AF05": 0.9474334,
                "AF06": 2.162038,
                "AF34": 1.6455,
                "AF16": 1.927783,
                "AF12": 4.028857,
                "AF13": 9.150105,
                "AF08": 1.64338,
                "AF03": 2.742952,
                "AF20": 1.382376,
                "AF22": 1.523334,
                "AF18": 0.9578965,
                "AF25": 0.580423,
                "AF04": 0.501081,
                "AF26": 4.572629,
            }
        ]
        sources = [
            (
                "#severity+malnutrition+num+subnational",
                "2020-10-01",
                "UNICEF",
                "https://data.humdata.org/dataset/87b86e7d-e9b2-4922-a48e-1f10afd528e6/resource/eabba7e7-16d0-436c-a62c-df6edd03be7c/download/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
            )
        ]
        run_check_scraper(
            name,
            runner,
            level,
            headers,
            values,
            sources,
            set_not_run=False,
        )

        jsonout = JsonFile(configuration["json"], [level])
        outputs = {"json": jsonout}
        update_subnational(
            runner,
            adminone,
            outputs,
            names=(name,),
        )
        assert jsonout.json[f"{level}_data"] == [
            {
                "#adm1+code": "AF01",
                "#adm1+name": "Kabul",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.282291",
            },
            {
                "#adm1+code": "AF02",
                "#adm1+name": "Kapisa",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "3.552082",
            },
            {
                "#adm1+code": "AF03",
                "#adm1+name": "Parwan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "2.742952",
            },
            {
                "#adm1+code": "AF04",
                "#adm1+name": "Maidan Wardak",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.501081",
            },
            {
                "#adm1+code": "AF05",
                "#adm1+name": "Logar",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.9474334",
            },
            {
                "#adm1+code": "AF06",
                "#adm1+name": "Nangarhar",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "2.162038",
            },
            {
                "#adm1+code": "AF07",
                "#adm1+name": "Laghman",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "2.090165",
            },
            {
                "#adm1+code": "AF08",
                "#adm1+name": "Panjsher",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.64338",
            },
            {
                "#adm1+code": "AF09",
                "#adm1+name": "Baghlan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.524646",
            },
            {
                "#adm1+code": "AF10",
                "#adm1+name": "Bamyan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.40426",
            },
            {
                "#adm1+code": "AF11",
                "#adm1+name": "Ghazni",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.022871",
            },
            {
                "#adm1+code": "AF12",
                "#adm1+name": "Paktika",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "4.028857",
            },
            {
                "#adm1+code": "AF13",
                "#adm1+name": "Paktya",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "9.150105",
            },
            {
                "#adm1+code": "AF14",
                "#adm1+name": "Khost",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.7653555",
            },
            {
                "#adm1+code": "AF15",
                "#adm1+name": "Kunar",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.953823",
            },
            {
                "#adm1+code": "AF16",
                "#adm1+name": "Nuristan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.927783",
            },
            {
                "#adm1+code": "AF17",
                "#adm1+name": "Badakhshan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "3.371688",
            },
            {
                "#adm1+code": "AF18",
                "#adm1+name": "Takhar",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.9578965",
            },
            {
                "#adm1+code": "AF19",
                "#adm1+name": "Kunduz",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.684882",
            },
            {
                "#adm1+code": "AF20",
                "#adm1+name": "Samangan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.382376",
            },
            {
                "#adm1+code": "AF21",
                "#adm1+name": "Balkh",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.319626",
            },
            {
                "#adm1+code": "AF22",
                "#adm1+name": "Sar E Pul",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.523334",
            },
            {
                "#adm1+code": "AF23",
                "#adm1+name": "Ghor",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.340286",
            },
            {
                "#adm1+code": "AF24",
                "#adm1+name": "Daykundi",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.043487",
            },
            {
                "#adm1+code": "AF25",
                "#adm1+name": "Uruzgan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.580423",
            },
            {
                "#adm1+code": "AF26",
                "#adm1+name": "Zabul",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "4.572629",
            },
            {
                "#adm1+code": "AF27",
                "#adm1+name": "Kandahar",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.378641",
            },
            {
                "#adm1+code": "AF28",
                "#adm1+name": "Jawzjan",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "0.6210205",
            },
            {
                "#adm1+code": "AF29",
                "#adm1+name": "Faryab",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "2.478977",
            },
            {
                "#adm1+code": "AF30",
                "#adm1+name": "Hilmand",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.677612",
            },
            {
                "#adm1+code": "AF31",
                "#adm1+name": "Badghis",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "3.519166",
            },
            {
                "#adm1+code": "AF32",
                "#adm1+name": "Hirat",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.687488",
            },
            {
                "#adm1+code": "AF33",
                "#adm1+name": "Farah",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "2.745447",
            },
            {
                "#adm1+code": "AF34",
                "#adm1+name": "Nimroz",
                "#country+code": "AFG",
                "#country+name": "Afghanistan",
                "#severity+malnutrition+num+subnational": "1.6455",
            },
        ]

        scraper_configuration = configuration["other"]
        runner.add_configurables(scraper_configuration, level)
        name = "gam_other"
        headers = (
            ["Malnutrition Estimate"],
            ["#severity+malnutrition+num+subnational"],
        )
        values = [{"AF09": 1.524646, "AF24": 1.043487}]
        sources = [
            (
                "#severity+malnutrition+num+subnational",
                "2020-10-01",
                "UNICEF",
                "https://data.humdata.org/dataset/87b86e7d-e9b2-4922-a48e-1f10afd528e6/resource/eabba7e7-16d0-436c-a62c-df6edd03be7c/download/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
            )
        ]
        run_check_scraper(name, runner, level, headers, values, sources)
