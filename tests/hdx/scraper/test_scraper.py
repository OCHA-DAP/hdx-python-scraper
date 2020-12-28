import filecmp
from os import getenv
from os.path import join

import pandas
import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from hdx.scraper.exceloutput import exceloutput
from hdx.scraper.googlesheets import googlesheets
from hdx.scraper.jsonoutput import jsonoutput
from hdx.scraper.nooutput import nooutput
from hdx.scraper.tabularparser import get_tabular


class TestScraper:
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, hdx_site='prod', user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'pse', 'title': 'State of Palestine'}])
        return Configuration.read()

    @pytest.fixture(scope='function')
    def folder(self):
        return join('tests', 'fixtures')

    def test_get_tabular(self, configuration):
        with Download(user_agent='test') as downloader:
            today = parse_date('2020-10-01')
            adminone = AdminOne(configuration)
            population_lookup = dict()
            population_headers, population_columns, population_sources = get_tabular(configuration, ['AFG'], adminone, 'national', downloader, today=today, scrapers=['population'], population_lookup=population_lookup)
            assert population_headers == [['Population'], ['#population']]
            assert population_columns == [{'AFG': 38041754}]
            assert population_sources == [('#population', '2020-10-01', 'World Bank', 'https://data.humdata.org/organization/world-bank-group')]

    def test_save(self, configuration, folder):
        with temp_dir('TestScraper', delete_on_success=True, delete_on_failure=False) as tempdir:
            with Download(user_agent='test') as downloader:
                tabs = configuration['tabs']
                sheetname = list(tabs.values())[0]
                noout = nooutput(tabs)
                excelout = exceloutput(join(tempdir, 'test_output.xlsx'), tabs, tabs)
                gsheet_auth = getenv('GSHEET_AUTH')
                if not gsheet_auth:
                    raise ValueError('No gsheet authorisation supplied!')
                googleout = googlesheets(configuration, gsheet_auth, None, tabs, tabs)
                jsonout = jsonoutput(configuration, tabs)
                hxltags = {'Country Code': '#country+code', 'Country Name': '#country+name', 'Population': '#population'}
                output = [list(hxltags.keys()), list(hxltags.values()), ['AFG', 'Afghanistan', 38041754]]

                # won't do anything as wrong tab name
                excelout.update_tab('lala', output, hxltags=hxltags)
                googleout.update_tab('lala', output, hxltags=hxltags)
                jsonout.update_tab('lala', output, hxltags=hxltags)

                noout.update_tab('national', output, hxltags=hxltags)
                excelout.update_tab('national', output, hxltags=hxltags)
                googleout.update_tab('national', output, hxltags=hxltags)
                jsonout.update_tab('national', output, hxltags=hxltags)
                jsonout.add_additional_json(downloader, today=parse_date('2020-10-01'))
                noout.save()
                excelout.save()
                filepaths = jsonout.save(tempdir, countries_to_save=['AFG'])
                excelsheet = excelout.workbook.get_sheet_by_name(sheetname)

                def get_list_from_cells(cells):
                    result = [list(), list(), list()]
                    for i, row in enumerate(excelsheet[cells]):
                        for column in row:
                            result[i].append(column.value)
                    return result

                assert get_list_from_cells('A1:C3') == output
                spreadsheet = googleout.gc.open_by_url(configuration['googlesheets']['test'])
                googletab = spreadsheet.worksheet_by_title(sheetname)
                result = googletab.get_values(start=(1, 1), end=(3, 3), returnas='matrix')
                result[2][2] = int(result[2][2])
                assert result == output
                assert filecmp.cmp(filepaths[0], join(folder, 'test_tabular_all.json'))
                assert filecmp.cmp(filepaths[1], join(folder, 'test_tabular_population.json'))
                assert filecmp.cmp(filepaths[2], join(folder, 'test_tabular_population_2.json'))

                jsonout.json = dict()
                df = pandas.DataFrame(output[2:], columns=output[0])
                noout.update_tab('national', df, hxltags=hxltags)
                excelout.update_tab('national', df, hxltags=hxltags)
                googleout.update_tab('national', df, hxltags=hxltags)
                jsonout.update_tab('national', df, hxltags=hxltags)
                jsonout.add_additional_json(downloader, today=parse_date('2020-10-01'))
                filepaths = jsonout.save(tempdir, countries_to_save=['AFG'])
                assert get_list_from_cells('A1:C3') == output
                result = googletab.get_values(start=(1, 1), end=(3, 3), returnas='matrix')
                result[2][2] = int(result[2][2])
                assert result == output
                assert filecmp.cmp(filepaths[0], join(folder, 'test_tabular_all.json'))
                assert filecmp.cmp(filepaths[1], join(folder, 'test_tabular_population.json'))
                assert filecmp.cmp(filepaths[2], join(folder, 'test_tabular_population_2.json'))

                df = pandas.DataFrame(output[1:], columns=output[0])
                googleout.update_tab('national', df, limit=2)
                result = googletab.get_values(start=(1, 1), end=(3, 3), returnas='matrix')
                result[2][2] = int(result[2][2])
