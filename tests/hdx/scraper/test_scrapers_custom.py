from copy import deepcopy

from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.runner import Runner

from .affected_targeted_reached import AffectedTargetedReached
from .conftest import check_scraper, check_scrapers
from .education_closures import EducationClosures
from .education_enrolment import EducationEnrolment


class TestScrapersCustom:
    def test_get_custom(self, configuration, fallbacks_json):
        BaseScraper.population_lookup = dict()
        source_date = "Apr 30, 2022"
        today = parse_date("2020-10-01")
        level = "national"
        countries = ("AFG",)

        class Region:
            iso3_to_region_and_hrp = {"AFG": ("ROAP",)}

        region = Region()
        runner = Runner(("AFG",), today)
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
                "Oct 1, 2020",
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

        runner = Runner(("AFG",), today)
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
                fallbacks_json,
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
                fallbacks_json,
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
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#population+learners+tertiary",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#affected+learners",
                "Apr 30, 2022",
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
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#affected+learners+pct",
                "Apr 30, 2022",
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

        education_closures.add_hxltag_source("#lala", key="test")
        sources = education_closures.get_sources("test")
        assert sources == [
            (
                "#lala",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            )
        ]
        education_closures.add_hxltag_sources(
            ("#haha", "#papa"),
            datasetinfo=education_closures.datasetinfo,
            key="test",
            suffix_attributes=("AFG", "PSE"),
        )
        assert sources == [
            (
                "#lala",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#haha+afg",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#haha+pse",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#papa+afg",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
            (
                "#papa+pse",
                "Apr 30, 2022",
                "UNESCO",
                "https://data.humdata.org/dataset/global-school-closures-covid19",
            ),
        ]

    def test_affected_targeted_reached(self, configuration):
        BaseScraper.population_lookup = dict()
        today = parse_date("2022-09-30")
        countries = ("ETH", "KEN", "SOM")
        adminone = AdminLevel(configuration["admin1"])
        admintwo = AdminLevel(
            configuration["admin2"],
            admin_level=2,
            admin_level_overrides={"KEN": 1},
        )

        runner = Runner(countries, today)
        datasetinfo = configuration["affected_targeted_reached"]
        affectedtargetedreached = AffectedTargetedReached(
            datasetinfo, today, adminone, admintwo
        )
        runner.add_custom(affectedtargetedreached)
        runner.run()
        name = affectedtargetedreached.name
        headers = (
            ["Total Affected", "Total Targeted", "Total Reached", "Priority"],
            [
                "#affected+total",
                "#targeted+total",
                "#reached+total",
                "#priority",
            ],
        )
        values = [
            {
                "ET01": "937314",
                "ET02": "2146641",
                "ET03": "1441261",
                "ET04": "7451551",
                "ET05": "6091323",
                "ET07": "4270980",
                "ET11": "796710",
                "ET12": "18850",
                "ET13": "146620",
                "ET15": "239938",
                "ET16": "638306",
                "KE002": "107659",
                "KE003": "321350",
                "KE004": "139674",
                "KE005": "45694",
                "KE006": "42311",
                "KE007": "371951",
                "KE008": "386725",
                "KE009": "383491",
                "KE010": "278170",
                "KE011": "118480",
                "KE012": "168275",
                "KE013": "48833",
                "KE015": "249961",
                "KE017": "87111",
                "KE019": "89278",
                "KE023": "409804",
                "KE024": "274642",
                "KE025": "137191",
                "KE030": "294767",
                "KE031": "114083",
                "KE034": "159924",
                "SO11": "134461",
                "SO12": "463340",
                "SO13": "376381",
                "SO14": "251992",
                "SO15": "166473",
                "SO16": "507221",
                "SO17": "312975",
                "SO18": "759749",
                "SO19": "397524",
                "SO20": "255571",
                "SO21": "362755",
                "SO22": "1073325",
                "SO23": "528895",
                "SO24": "745695",
                "SO25": "316958",
                "SO26": "445724",
                "SO27": "191803",
                "SO28": "556047",
            },
            {
                "ET01": "795208",
                "ET02": "1478902",
                "ET03": "879120",
                "ET04": "5616019",
                "ET05": "3991110",
                "ET07": "2925097",
                "ET11": "593998",
                "ET12": "14978",
                "ET13": "91640",
                "ET15": "204359",
                "ET16": "490590",
                "KE003": "130841",
                "KE004": "83409",
                "KE007": "168271",
                "KE008": "214847",
                "KE009": "216865",
                "KE010": "172420",
                "KE011": "73701",
                "KE012": "95611",
                "KE015": "142023",
                "KE023": "247500",
                "KE024": "111823",
                "KE025": "80100",
                "KE030": "150026",
                "KE031": "51856",
                "KE034": "63969",
                "SO11": "58411",
                "SO12": "420750",
                "SO13": "376381",
                "SO14": "217437",
                "SO15": "166473",
                "SO16": "492699",
                "SO17": "312975",
                "SO18": "759749",
                "SO19": "397524",
                "SO20": "237708",
                "SO21": "314579",
                "SO22": "1073325",
                "SO23": "528895",
                "SO24": "745695",
                "SO25": "316958",
                "SO26": "445724",
                "SO27": "191803",
                "SO28": "556047",
            },
            {},
            {
                "ET01": "3",
                "ET02": "2",
                "ET03": "3",
                "ET04": "2",
                "ET05": "2",
                "ET07": "3",
                "ET11": "3",
                "ET12": "3",
                "ET13": "3",
                "ET15": "3",
                "ET16": "3",
                "KE002": "3",
                "KE003": "2",
                "KE004": "2",
                "KE005": "2",
                "KE006": "3",
                "KE007": "1",
                "KE008": "1",
                "KE009": "1",
                "KE010": "1",
                "KE011": "2",
                "KE012": "2",
                "KE013": "3",
                "KE014": "3",
                "KE015": "2",
                "KE017": "3",
                "KE019": "3",
                "KE023": "1",
                "KE024": "2",
                "KE025": "2",
                "KE030": "2",
                "KE031": "2",
                "KE033": "3",
                "KE034": "2",
                "SO11": "2",
                "SO12": "2",
                "SO13": "2",
                "SO14": "2",
                "SO15": "2",
                "SO16": "2",
                "SO17": "1",
                "SO18": "1",
                "SO19": "1",
                "SO20": "2",
                "SO21": "2",
                "SO22": "1",
                "SO23": "1",
                "SO24": "1",
                "SO25": "1",
                "SO26": "2",
                "SO27": "2",
                "SO28": "2",
            },
        ]
        sources = [
            (
                "#affected+total+eth",
                "Jul 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/ethiopia-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#affected+total+ken",
                "Oct 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/kenya-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#affected+total+som",
                "Dec 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/somalia-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#targeted+total+eth",
                "Jul 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/ethiopia-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#targeted+total+ken",
                "Oct 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/kenya-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#targeted+total+som",
                "Dec 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/somalia-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#priority+eth",
                "Jul 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/ethiopia-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#priority+ken",
                "Oct 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/kenya-pin-targeted-reached-by-location-and-cluster",
            ),
            (
                "#priority+som",
                "Dec 31, 2022",
                "Multiple Source (Humanitarian Partners)",
                "https://data.humdata.org/dataset/somalia-pin-targeted-reached-by-location-and-cluster",
            ),
        ]
        check_scraper(name, runner, "adminone", headers, values, sources)
        values = [
            {
                "ET0103": "283434",
                "ET0104": "369082",
                "ET0106": "284798",
                "ET0201": "741963",
                "ET0202": "475830",
                "ET0203": "344036",
                "ET0204": "324528",
                "ET0205": "260284",
                "ET0303": "381600",
                "ET0304": "188135",
                "ET0305": "680672",
                "ET0310": "190854",
                "ET0404": "38277",
                "ET0405": "107933",
                "ET0406": "198160",
                "ET0407": "261996",
                "ET0408": "717412",
                "ET0409": "1114406",
                "ET0410": "753233",
                "ET0411": "442200",
                "ET0412": "966511",
                "ET0414": "828741",
                "ET0415": "726103",
                "ET0417": "584578",
                "ET0418": "65804",
                "ET0420": "185896",
                "ET0421": "460301",
                "ET0501": "873833",
                "ET0502": "839543",
                "ET0503": "775467",
                "ET0504": "371751",
                "ET0505": "917173",
                "ET0506": "492617",
                "ET0507": "446325",
                "ET0508": "399185",
                "ET0509": "277712",
                "ET0510": "449721",
                "ET0511": "247996",
                "ET0701": "17362",
                "ET0702": "534903",
                "ET0703": "42632",
                "ET0705": "1249295",
                "ET0706": "205562",
                "ET0707": "663036",
                "ET0710": "324397",
                "ET0712": "98944",
                "ET0713": "100839",
                "ET0714": "83375",
                "ET0715": "626845",
                "ET0720": "96595",
                "ET0721": "29306",
                "ET0722": "63624",
                "ET0723": "134265",
                "ET1102": "179831",
                "ET1104": "136077",
                "ET1105": "373447",
                "ET1106": "107355",
                "ET1201": "18850",
                "ET1301": "146620",
                "ET1501": "102758",
                "ET1502": "137180",
                "ET1600": "638306",
                "KE002": "107659",
                "KE003": "321350",
                "KE004": "139674",
                "KE005": "45694",
                "KE006": "42311",
                "KE007": "371951",
                "KE008": "386725",
                "KE009": "383491",
                "KE010": "278170",
                "KE011": "118480",
                "KE012": "168275",
                "KE013": "48833",
                "KE015": "249961",
                "KE017": "87111",
                "KE019": "89278",
                "KE023": "409804",
                "KE024": "274642",
                "KE025": "137191",
                "KE030": "294767",
                "KE031": "114083",
                "KE034": "159924",
                "SO1101": "75240",
                "SO1102": "11604",
                "SO1103": "24264",
                "SO1104": "23353",
                "SO1201": "47322",
                "SO1202": "57731",
                "SO1203": "358287",
                "SO1301": "246579",
                "SO1302": "52438",
                "SO1303": "46684",
                "SO1304": "30680",
                "SO1401": "133983",
                "SO1402": "39876",
                "SO1403": "38395",
                "SO1404": "39738",
                "SO1501": "66154",
                "SO1502": "34655",
                "SO1503": "65664",
                "SO1601": "322254",
                "SO1602": "18247",
                "SO1603": "42038",
                "SO1604": "24942",
                "SO1605": "25610",
                "SO1606": "74130",
                "SO1701": "159917",
                "SO1702": "64898",
                "SO1703": "88160",
                "SO1801": "417872",
                "SO1802": "100381",
                "SO1803": "105171",
                "SO1804": "103525",
                "SO1805": "32800",
                "SO1901": "122989",
                "SO1902": "95095",
                "SO1903": "78290",
                "SO1904": "58983",
                "SO1905": "42167",
                "SO2001": "187045",
                "SO2002": "46863",
                "SO2003": "21663",
                "SO2101": "164655",
                "SO2102": "25843",
                "SO2103": "143887",
                "SO2104": "28370",
                "SO2201": "1073325",
                "SO2301": "86478",
                "SO2302": "188889",
                "SO2303": "16433",
                "SO2304": "24497",
                "SO2305": "50618",
                "SO2306": "18184",
                "SO2307": "143796",
                "SO2401": "465562",
                "SO2402": "112322",
                "SO2403": "95571",
                "SO2404": "72240",
                "SO2501": "73912",
                "SO2502": "39238",
                "SO2503": "97803",
                "SO2504": "53058",
                "SO2505": "52947",
                "SO2601": "87792",
                "SO2602": "103360",
                "SO2603": "73287",
                "SO2604": "56937",
                "SO2605": "52638",
                "SO2606": "71710",
                "SO2701": "66984",
                "SO2702": "77823",
                "SO2703": "46996",
                "SO2801": "149769",
                "SO2802": "140111",
                "SO2803": "39237",
                "SO2804": "226930",
            },
            {
                "ET0103": "279057",
                "ET0104": "225178",
                "ET0106": "290973",
                "ET0201": "511879",
                "ET0202": "306001",
                "ET0203": "247004",
                "ET0204": "288129",
                "ET0205": "125889",
                "ET0303": "184943",
                "ET0304": "178546",
                "ET0305": "363856",
                "ET0310": "151775",
                "ET0404": "19139",
                "ET0405": "107933",
                "ET0406": "128281",
                "ET0407": "73722",
                "ET0408": "604498",
                "ET0409": "843262",
                "ET0410": "569534",
                "ET0411": "273293",
                "ET0412": "782888",
                "ET0414": "705360",
                "ET0415": "572936",
                "ET0417": "496756",
                "ET0418": "9914",
                "ET0420": "166578",
                "ET0421": "261925",
                "ET0501": "521998",
                "ET0502": "633253",
                "ET0503": "447603",
                "ET0504": "278679",
                "ET0505": "580372",
                "ET0506": "327409",
                "ET0507": "360009",
                "ET0508": "245137",
                "ET0509": "217868",
                "ET0510": "237947",
                "ET0511": "140835",
                "ET0701": "17362",
                "ET0702": "359714",
                "ET0703": "14497",
                "ET0705": "802759",
                "ET0706": "129265",
                "ET0707": "567891",
                "ET0710": "232282",
                "ET0712": "49472",
                "ET0713": "62664",
                "ET0714": "61577",
                "ET0715": "440697",
                "ET0720": "33298",
                "ET0721": "17181",
                "ET0722": "2173",
                "ET0723": "134265",
                "ET1102": "179061",
                "ET1104": "76219",
                "ET1105": "300666",
                "ET1106": "38052",
                "ET1201": "14978",
                "ET1301": "91640",
                "ET1501": "75173",
                "ET1502": "129186",
                "ET1600": "490590",
                "KE003": "130841",
                "KE004": "83409",
                "KE007": "168271",
                "KE008": "214847",
                "KE009": "216865",
                "KE010": "172420",
                "KE011": "73701",
                "KE012": "95611",
                "KE015": "142023",
                "KE023": "247500",
                "KE024": "111823",
                "KE025": "80100",
                "KE030": "150026",
                "KE031": "51856",
                "KE034": "63969",
                "SO1101": "9420",
                "SO1102": "1374",
                "SO1103": "24264",
                "SO1104": "23353",
                "SO1201": "358287",
                "SO1202": "4732",
                "SO1203": "57731",
                "SO1301": "52438",
                "SO1302": "246579",
                "SO1303": "46684",
                "SO1304": "30680",
                "SO1401": "133983",
                "SO1402": "39876",
                "SO1403": "3840",
                "SO1404": "39738",
                "SO1501": "66154",
                "SO1502": "34655",
                "SO1503": "65664",
                "SO1601": "322254",
                "SO1602": "18247",
                "SO1603": "42038",
                "SO1604": "10420",
                "SO1605": "25610",
                "SO1606": "74130",
                "SO1701": "159917",
                "SO1702": "64898",
                "SO1703": "88160",
                "SO1801": "417872",
                "SO1802": "100381",
                "SO1803": "105171",
                "SO1804": "103525",
                "SO1805": "32800",
                "SO1901": "122989",
                "SO1902": "95095",
                "SO1903": "78290",
                "SO1904": "58983",
                "SO1905": "42167",
                "SO2001": "187045",
                "SO2002": "46863",
                "SO2003": "3800",
                "SO2101": "164655",
                "SO2102": "3200",
                "SO2103": "143887",
                "SO2104": "2837",
                "SO2201": "1073325",
                "SO2301": "86478",
                "SO2302": "188889",
                "SO2303": "16433",
                "SO2304": "24497",
                "SO2305": "50618",
                "SO2306": "18184",
                "SO2307": "143796",
                "SO2401": "465562",
                "SO2402": "112322",
                "SO2403": "95571",
                "SO2404": "72240",
                "SO2501": "73912",
                "SO2502": "39238",
                "SO2503": "97803",
                "SO2504": "53058",
                "SO2505": "52947",
                "SO2601": "87792",
                "SO2602": "103360",
                "SO2603": "73287",
                "SO2604": "56937",
                "SO2605": "52638",
                "SO2606": "71710",
                "SO2701": "66984",
                "SO2702": "77823",
                "SO2703": "46996",
                "SO2801": "149769",
                "SO2802": "140111",
                "SO2803": "39237",
                "SO2804": "226930",
            },
            {},
            {
                "ET0103": "3",
                "ET0104": "3",
                "ET0106": "3",
                "ET0201": "2",
                "ET0202": "2",
                "ET0203": "2",
                "ET0204": "2",
                "ET0205": "2",
                "ET0303": "3",
                "ET0304": "3",
                "ET0305": "3",
                "ET0310": "3",
                "ET0404": "3",
                "ET0405": "3",
                "ET0406": "3",
                "ET0407": "3",
                "ET0408": "3",
                "ET0409": "2",
                "ET0410": "2",
                "ET0411": "2",
                "ET0412": "2",
                "ET0414": "2",
                "ET0415": "3",
                "ET0417": "3",
                "ET0418": "3",
                "ET0420": "3",
                "ET0421": "2",
                "ET0501": "3",
                "ET0502": "3",
                "ET0503": "2",
                "ET0504": "2",
                "ET0505": "2",
                "ET0506": "2",
                "ET0507": "2",
                "ET0508": "2",
                "ET0509": "2",
                "ET0510": "2",
                "ET0511": "1",
                "ET0701": "3",
                "ET0702": "2",
                "ET0703": "2",
                "ET0705": "3",
                "ET0706": "3",
                "ET0707": "2",
                "ET0710": "3",
                "ET0712": "2",
                "ET0713": "2",
                "ET0714": "3",
                "ET0715": "3",
                "ET0720": "2",
                "ET0721": "2",
                "ET0722": "2",
                "ET0723": "2",
                "ET1102": "2",
                "ET1104": "3",
                "ET1105": "3",
                "ET1106": "3",
                "ET1201": "3",
                "ET1301": "3",
                "ET1501": "3",
                "ET1502": "2",
                "ET1600": "3",
                "KE002": "3",
                "KE003": "2",
                "KE004": "2",
                "KE005": "2",
                "KE006": "3",
                "KE007": "1",
                "KE008": "1",
                "KE009": "1",
                "KE010": "1",
                "KE011": "2",
                "KE012": "2",
                "KE013": "3",
                "KE014": "3",
                "KE015": "2",
                "KE017": "3",
                "KE019": "3",
                "KE023": "1",
                "KE024": "2",
                "KE025": "2",
                "KE030": "2",
                "KE031": "2",
                "KE033": "3",
                "KE034": "2",
                "SO1101": "3",
                "SO1102": "3",
                "SO1103": "2",
                "SO1104": "2",
                "SO1201": "2",
                "SO1202": "3",
                "SO1203": "2",
                "SO1301": "2",
                "SO1302": "1",
                "SO1303": "2",
                "SO1304": "2",
                "SO1401": "1",
                "SO1402": "2",
                "SO1403": "3",
                "SO1404": "2",
                "SO1501": "2",
                "SO1502": "2",
                "SO1503": "2",
                "SO1601": "2",
                "SO1602": "2",
                "SO1603": "2",
                "SO1604": "3",
                "SO1605": "2",
                "SO1606": "2",
                "SO1701": "2",
                "SO1702": "1",
                "SO1703": "1",
                "SO1801": "1",
                "SO1802": "1",
                "SO1803": "1",
                "SO1804": "1",
                "SO1805": "2",
                "SO1901": "1",
                "SO1902": "1",
                "SO1903": "1",
                "SO1904": "1",
                "SO1905": "1",
                "SO2001": "1",
                "SO2002": "1",
                "SO2003": "3",
                "SO2101": "2",
                "SO2102": "3",
                "SO2103": "2",
                "SO2104": "3",
                "SO2201": "1",
                "SO2301": "1",
                "SO2302": "1",
                "SO2303": "2",
                "SO2304": "1",
                "SO2305": "1",
                "SO2306": "2",
                "SO2307": "1",
                "SO2401": "1",
                "SO2402": "1",
                "SO2403": "1",
                "SO2404": "1",
                "SO2501": "1",
                "SO2502": "1",
                "SO2503": "1",
                "SO2504": "1",
                "SO2505": "1",
                "SO2601": "2",
                "SO2602": "2",
                "SO2603": "2",
                "SO2604": "2",
                "SO2605": "1",
                "SO2606": "2",
                "SO2701": "2",
                "SO2702": "2",
                "SO2703": "2",
                "SO2801": "2",
                "SO2802": "2",
                "SO2803": "2",
                "SO2804": "1",
            },
        ]
        check_scraper(name, runner, "admintwo", headers, values, sources)
