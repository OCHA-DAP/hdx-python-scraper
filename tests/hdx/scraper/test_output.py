import filecmp
from os import getenv
from os.path import join

import numpy as np
import pandas
import pytest
from hdx.utilities.path import temp_dir

from hdx.scraper.outputs.base import BaseOutput
from hdx.scraper.outputs.excelfile import ExcelFile
from hdx.scraper.outputs.googlesheets import GoogleSheets
from hdx.scraper.outputs.json import JsonFile


class TestOutput:
    @pytest.fixture(scope="class")
    def hxltags(self):
        return {
            "Country Code": "#country+code",
            "Country Name": "#country+name",
            "Population": "#population",
        }

    def test_save(self, configuration, fixtures, hxltags):
        with temp_dir(
            "TestScraperSave", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            tabs = configuration["tabs"]
            sheetname = list(tabs.values())[0]
            noout = BaseOutput(tabs)
            excelout = ExcelFile(join(tempdir, "test_output.xlsx"), tabs, tabs)
            gsheet_auth = getenv("GSHEET_AUTH")
            if not gsheet_auth:
                raise ValueError("No gsheet authorisation supplied!")
            googleout = GoogleSheets(
                configuration["googlesheets"],
                gsheet_auth,
                None,
                tabs,
                tabs,
            )
            jsonout = JsonFile(configuration["json"], tabs)
            output = [
                list(hxltags.keys()),
                list(hxltags.values()),
                ["AFG", "Afghanistan", 38041754],
            ]

            # won't do anything as wrong tab name
            excelout.update_tab("lala", output, hxltags=hxltags)
            googleout.update_tab("lala", output, hxltags=hxltags)
            jsonout.update_tab("lala", output, hxltags=hxltags)

            noout.update_tab("national", output, hxltags=hxltags)
            excelout.update_tab("national", output, hxltags=hxltags)
            googleout.update_tab("national", output, hxltags=hxltags)
            jsonout.update_tab("national", output, hxltags=hxltags)
            noout.add_additional()
            jsonout.add_additional()
            noout.save()
            excelout.save()
            filepaths = jsonout.save(tempdir, countries_to_save=["AFG"])
            excelsheet = excelout.workbook[sheetname]

            def get_list_from_cells(cells):
                result = [list(), list(), list()]
                for i, row in enumerate(excelsheet[cells]):
                    for column in row:
                        result[i].append(column.value)
                return result

            assert get_list_from_cells("A1:C3") == output
            spreadsheet = googleout.gc.open_by_url(
                configuration["googlesheets"]["test"]
            )
            googletab = spreadsheet.worksheet(sheetname)
            result = googletab.get("A1:C3")
            result[2][2] = int(result[2][2])
            assert result == output
            assert filecmp.cmp(
                filepaths[0], join(fixtures, "test_scraper_all.json")
            )
            assert filecmp.cmp(
                filepaths[1],
                join(fixtures, "test_scraper_population.json"),
            )
            assert filecmp.cmp(
                filepaths[2],
                join(fixtures, "test_scraper_population.json"),
            )
            assert filecmp.cmp(
                filepaths[3], join(fixtures, "test_scraper_other.json")
            )

            jsonout.json = dict()
            df = pandas.DataFrame(output[2:], columns=output[0])
            noout.update_tab("national", df, hxltags=hxltags)
            excelout.update_tab("national", df, hxltags=hxltags)
            googleout.update_tab("national", df, hxltags=hxltags)
            jsonout.update_tab("national", df, hxltags=hxltags)
            jsonout.add_additional()
            filepaths = jsonout.save(tempdir, countries_to_save=["AFG"])
            assert get_list_from_cells("A1:C3") == output
            result = googletab.get("A1:C3")
            result[2][2] = int(result[2][2])
            assert result == output
            assert filecmp.cmp(
                filepaths[0], join(fixtures, "test_scraper_all.json")
            )
            assert filecmp.cmp(
                filepaths[1],
                join(fixtures, "test_scraper_population.json"),
            )
            assert filecmp.cmp(
                filepaths[2],
                join(fixtures, "test_scraper_population.json"),
            )
            assert filecmp.cmp(
                filepaths[3], join(fixtures, "test_scraper_other.json")
            )

            df = pandas.DataFrame(output[1:], columns=output[0])
            googleout.update_tab("national", df, limit=2)
            result = googletab.get("A1:C3")
            result[2][2] = int(result[2][2])
            assert result == output

            output = [
                list(hxltags.keys()),
                list(hxltags.values()),
                ["AFG", "Afghanistan", 38041754],
                ["BDI", "Burundi", np.NaN],
                ["PAK", "Pakistan", -np.inf],
            ]
            df = pandas.DataFrame(output[1:], columns=output[0])
            googleout.update_tab("national", df)
            result = googletab.get("A1:C5")
            result[2][2] = int(result[2][2])
            assert result[3][2] == "NaN"
            result[3][2] = np.NaN
            assert result[4][2] == "-inf"
            result[4][2] = -np.inf
            assert result == output

    def test_jsonoutput(self, configuration, fixtures, hxltags):
        tabs = configuration["tabs"]
        noout = BaseOutput(tabs)
        jsonout = JsonFile(configuration["json"], tabs)
        rows = [
            {
                "Country Code": "#country+code",
                "Country Name": "#country+name",
                "Population": "#population",
            },
            {
                "Country Code": "AFG",
                "Country Name": "Afghanistan",
                "Population": 38041754,
            },
        ]
        noout.add_data_rows_by_key("test", "AFG", rows, hxltags)
        jsonout.add_data_rows_by_key("test", "AFG", rows, hxltags)
        assert jsonout.json == {
            "test_data": {
                "AFG": [
                    {
                        "#country+code": "#country+code",
                        "#country+name": "#country+name",
                        "#population": "#population",
                    },
                    {
                        "#country+code": "AFG",
                        "#country+name": "Afghanistan",
                        "#population": 38041754,
                    },
                ]
            }
        }
        df = pandas.DataFrame.from_records([rows[1]])
        jsonout.json = dict()
        noout.add_dataframe_rows("test", df, rows[0])
        jsonout.add_dataframe_rows("test", df, rows[0])
        assert jsonout.json == {
            "test_data": [
                {
                    "#country+code": "AFG",
                    "#country+name": "Afghanistan",
                    "#population": 38041754,
                }
            ]
        }

        noout.add_data_row("test", rows[1])  # doesn't do anything
