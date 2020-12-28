from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.scraper.tabularparser import get_tabular


class TestScraper:
    def test_get_tabular(self, configuration):
        with Download(user_agent='test') as downloader:
            today = parse_date('2020-10-01')
            adminone = AdminOne(configuration)
            population_lookup = dict()
            population_headers, population_columns, population_sources = get_tabular(configuration, ['AFG'], adminone, 'national', downloader, today=today, scrapers=['population'], population_lookup=population_lookup)
            assert population_headers == [['Population'], ['#population']]
            assert population_columns == [{'AFG': 38041754}]
            assert population_sources == [('#population', '2020-10-01', 'World Bank', 'https://data.humdata.org/organization/world-bank-group')]
