import pytest
from hdx.utilities.downloader import Download

from hdx.scraper.readers import read, get_url


class TestReaders:
    def test_read(self, configuration):
        url = get_url('http://{{var}}', var='hello')
        assert url == 'http://hello'
        with Download(user_agent='test') as downloader:
            datasetinfo = {'name': 'test', 'dataset': 'sahel-humanitarian-needs-overview', 'format': 'csv'}
            headers, iterator = read(downloader, datasetinfo, a='b')
            assert headers == ['Country', 'nutrition', 'Affected 2017', 'In Need 2017', 'Targeted 2017', '% targeted']
            assert next(iterator) == {'Country': '#country', 'nutrition': '#sector?', 'Affected 2017': '#affected', 'In Need 2017': '#inneed', 'Targeted 2017': '#targeted', '% targeted': '#targeted+percentage'}
            assert next(iterator) == {'Country': 'Burkina Faso', 'nutrition': 'MAM', 'Affected 2017': '433,412', 'In Need 2017': '433,412', 'Targeted 2017': '             _', '% targeted': '0'}
            assert datasetinfo == {'name': 'test', 'dataset': 'sahel-humanitarian-needs-overview', 'format': 'csv', 'headers': 1, 'date': '2016-09-01',
                                   'source': 'Multiple organisations', 'source_url': 'https://data.humdata.org/dataset/sahel-humanitarian-needs-overview',
                                   'url': 'https://data.humdata.org/dataset/47f6ef46-500f-421a-9fa2-fefd93facf95/resource/2527ac5b-66fe-46f0-8b9b-7086d2c4ddd3/download/hno-2017-sahel-nutrition.csv'}
            with pytest.raises(ValueError):
                datasetinfo = {'name': 'test', 'format': 'unknown'}
                read(downloader, datasetinfo)
            with pytest.raises(ValueError):
                datasetinfo = {'name': 'test', 'dataset': 'sahel-humanitarian-needs-overview', 'format': 'json'}
                read(downloader, datasetinfo)
