from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.outputs.json import JsonFile
from hdx.scraper.runner import Runner
from hdx.scraper.utilities.writer import Writer

from .conftest import run_check_scraper


class TestScrapersSubnational:
    def test_get_subnational(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminlevel = AdminLevel(configuration)
        level = "subnational"
        scraper_configuration = configuration[f"scraper_{level}"]
        runner = Runner(("AFG",), today)
        keys = runner.add_configurables(
            scraper_configuration, level, adminlevel=adminlevel
        )
        assert keys == ["gam", "ipc_somalia", "idps_somalia"]

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
                "Oct 01, 2020",
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
        writer = Writer(runner, outputs)
        writer.update_subnational(
            adminlevel,
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
        runner.add_configurables(
            scraper_configuration, level, adminlevel=adminlevel
        )
        name = "gam_other"
        headers = (
            ["Malnutrition Estimate"],
            ["#severity+malnutrition+num+subnational"],
        )
        values = [{"AF09": 1.524646, "AF24": 1.043487}]
        sources = [
            (
                "#severity+malnutrition+num+subnational",
                "Oct 01, 2020",
                "UNICEF",
                "https://data.humdata.org/dataset/87b86e7d-e9b2-4922-a48e-1f10afd528e6/resource/eabba7e7-16d0-436c-a62c-df6edd03be7c/download/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx",
            )
        ]
        run_check_scraper(name, runner, level, headers, values, sources)

    def test_fixed_country(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2020-10-01")
        adminlevel = AdminLevel(configuration["admin1"])
        level = "subnational"
        scraper_configuration = configuration[f"scraper_{level}"]
        runner = Runner(("SOM",), today)
        runner.add_configurables(
            scraper_configuration, level, adminlevel=adminlevel
        )
        name = "ipc_somalia"
        headers = (
            ["FoodInsecurityIPCP3+"],
            ["#affected+food+ipc+p3plus+num"],
        )
        values = [
            {
                "SO11": 141240,
                "SO12": 304640,
                "SO13": 266310,
                "SO14": 190270,
                "SO15": 206550,
                "SO16": 377780,
                "SO17": 328470,
                "SO18": 640920,
                "SO19": 380590,
                "SO20": 192450,
                "SO21": 378550,
                "SO22": 774030,
                "SO23": 412570,
                "SO24": 924800,
                "SO25": 276220,
                "SO26": 356170,
                "SO27": 145310,
                "SO28": 383900,
            }
        ]
        sources = [
            (
                "#affected+food+ipc+p3plus+num",
                "Jan-Dec 2022",
                "National IPC Technical Working Group",
                "https://data.humdata.org/dataset/somalia-acute-food-insecurity-country-data",
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

        adminlevel = AdminLevel(configuration["admin2"])
        runner = Runner(("SOM",), today)
        runner.add_configurables(
            scraper_configuration, level, adminlevel=adminlevel
        )
        name = "idps_somalia"
        headers = (
            ["IDPs"],
            ["#affected+idps+ind"],
        )
        values = [
            {
                "SO1101": 5578,
                "SO1102": 1681,
                "SO1103": 1654,
                "SO1104": 820,
                "SO1201": 1329,
                "SO1202": 128,
                "SO1203": 77,
                "SO1301": 41365,
                "SO1302": 7446,
                "SO1303": 10596,
                "SO1401": 13199,
                "SO1402": 3376,
                "SO1403": 835,
                "SO1404": 4777,
                "SO1501": 12728,
                "SO1502": 1904,
                "SO1503": 6871,
                "SO1601": 14332,
                "SO1602": 1148,
                "SO1603": 21,
                "SO1604": 13691,
                "SO1605": 826,
                "SO1606": 20093,
                "SO1701": 1025,
                "SO1702": 592,
                "SO1703": 393,
                "SO1801": 70167,
                "SO1802": 9020,
                "SO1803": 25856,
                "SO1804": 5676,
                "SO1901": 58294,
                "SO1902": 38557,
                "SO1903": 31702,
                "SO1904": 216,
                "SO1905": 275,
                "SO2001": 7396,
                "SO2002": 1299,
                "SO2003": 741,
                "SO2101": 8060,
                "SO2102": 1723,
                "SO2103": 1662,
                "SO2104": 1225,
                "SO2201": 325969,
                "SO2301": 5683,
                "SO2302": 10925,
                "SO2303": 902,
                "SO2304": 2320,
                "SO2305": 1266,
                "SO2306": 296,
                "SO2307": 209,
                "SO2401": 59560,
                "SO2403": 21844,
                "SO2404": 20635,
                "SO2501": 10210,
                "SO2502": 10866,
                "SO2503": 15,
                "SO2504": 220,
                "SO2505": 18,
                "SO2601": 12019,
                "SO2602": 47535,
                "SO2603": 10906,
                "SO2604": 2412,
                "SO2605": 21335,
                "SO2606": 18875,
                "SO2701": 56,
                "SO2702": 176,
                "SO2703": 20835,
                "SO2801": 30655,
                "SO2802": 24442,
                "SO2803": 6443,
                "SO2804": 6678,
            }
        ]
        sources = [
            (
                "#affected+idps+ind",
                "2022-08-31",
                "UNHCR",
                "https://data.humdata.org/dataset/somalia-internally-displaced-persons-idps",
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
