# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from os.path import join
from typing import Any, Dict, Tuple, List, Iterator, Union, Optional

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from jsonpath_ng import parse
from olefile import olefile

from hdx.scraper import get_date_from_dataset_date, match_template

logger = logging.getLogger(__name__)


def get_url(url, **kwargs):
    # type: (str, Any) -> str
    """Get url from a string replacing any template arguments

    Args:
        url (str): Url to read
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        str: Url with any template arguments replaced
    """
    for kwarg in kwargs:
        exec('%s="%s"' % (kwarg, kwargs[kwarg]))
    template_string, match_string = match_template(url)
    if template_string:
        replace_string = eval(match_string)
        url = url.replace(template_string, replace_string)
    return url


def read_tabular(downloader, datasetinfo, **kwargs):
    # type: (Download, Dict, Any) -> Tuple[List[str],Iterator[Union[List,Dict]]]
    """Read data from tabular source eg. csv, xls, xlsx

    Args:
        downloader (Download): Download object for downloading files
        datasetinfo (Dict): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Tuple[List[str],Iterator[Union[List,Dict]]]: Tuple (headers, iterator where each row is a list or dictionary)
    """
    url = get_url(datasetinfo['url'], **kwargs)
    sheet = datasetinfo.get('sheet')
    headers = datasetinfo.get('headers')
    if headers is None:
        headers = 1
        datasetinfo['headers'] = 1
    if isinstance(headers, list):
        kwargs['fill_merged_cells'] = True
    format = datasetinfo['format']
    compression = datasetinfo.get('compression')
    if compression:
        kwargs['compression'] = compression
    return downloader.get_tabular_rows(url, sheet=sheet, headers=headers, dict_form=True, format=format, **kwargs)


def read_ole(downloader, datasetinfo, **kwargs):
    # type: (Download, Dict, Any) -> Tuple[List[str],Iterator[Union[List,Dict]]]
    """Read data from OLE Excel source

    Args:
        downloader (Download): Download object for downloading files
        datasetinfo (Dict): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Tuple[List[str],Iterator[Union[List,Dict]]]: Tuple (headers, iterator where each row is a list or dictionary)
    """
    url = get_url(datasetinfo['url'], **kwargs)
    with temp_dir('ole') as folder:
        path = downloader.download_file(url, folder, 'olefile')
        ole = olefile.OleFileIO(path)
        data = ole.openstream('Workbook').getvalue()
        outputfile = join(folder, 'excel_file.xls')
        with open(outputfile, 'wb') as f:
            f.write(data)
        datasetinfo['url'] = outputfile
        datasetinfo['format'] = 'xls'
        return read_tabular(downloader, datasetinfo, **kwargs)


def read_json(downloader, datasetinfo, **kwargs):
    # type: (Download, Dict, Any) -> Optional[Iterator[Union[List,Dict]]]
    """Read data from json source allowing for JSONPath expressions

    Args:
        downloader (Download): Download object for downloading JSON
        datasetinfo (Dict): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Optional[Iterator[Union[List,Dict]]]: Iterator or None
    """
    url = get_url(datasetinfo['url'], **kwargs)
    response = downloader.download(url)
    json = response.json()
    expression = datasetinfo.get('jsonpath')
    if expression:
        expression = parse(expression)
        json = expression.find(json)
    if isinstance(json, list):
        return iter(json)
    return None


def read_hdx_metadata(datasetinfo, today=None):
    # type: (Dict, Optional[datetime]) -> None
    """Read metadata from HDX dataset and add to input dictionary

    Args:
        datasetinfo (Dict): Dictionary of information about dataset
        today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).

    Returns:
        None
    """
    dataset_name = datasetinfo['dataset']
    dataset = Dataset.read_from_hdx(dataset_name)
    format = datasetinfo['format']
    url = datasetinfo.get('url')
    if not url:
        for resource in dataset.get_resources():
            if resource['format'] == format.upper():
                url = resource['url']
                break
        if not url:
            raise ValueError('Cannot find %s resource in %s!' % (format, dataset_name))
        datasetinfo['url'] = url
    if 'date' not in datasetinfo:
        datasetinfo['date'] = get_date_from_dataset_date(dataset, today=today)
    if 'source' not in datasetinfo:
        datasetinfo['source'] = dataset['dataset_source']
    if 'source_url' not in datasetinfo:
        datasetinfo['source_url'] = dataset.get_hdx_url()


def read_hdx(downloader, datasetinfo, today=None):
    # type: (Download, Dict, Optional[datetime]) -> Tuple[List[str],Iterator[Union[List,Dict]]]
    """Read data and metadata from HDX dataset

    Args:
        downloader (Download): Download object for downloading files
        datasetinfo (Dict): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Tuple[List[str],Iterator[Union[List,Dict]]]: Tuple (headers, iterator where each row is a list or dictionary)
    """
    read_hdx_metadata(datasetinfo, today=today)
    return read_tabular(downloader, datasetinfo)


def read(downloader, datasetinfo, today=None, **kwargs):
    # type: (Download, Dict, Optional[datetime], Any) -> Tuple[List[str],Iterator[Union[List,Dict]]]
    """Read data and metadata from HDX dataset

    Args:
        downloader (Download): Download object for downloading files
        datasetinfo (Dict): Dictionary of information about dataset
        today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).
        **kwargs: Variables to use when evaluating template arguments in urls

    Returns:
        Tuple[List[str],Iterator[Union[List,Dict]]]: Tuple (headers, iterator where each row is a list or dictionary)
    """
    format = datasetinfo['format']
    if format == 'json':
        if 'dataset' in datasetinfo:
            read_hdx_metadata(datasetinfo, today=today)
        iterator = read_json(downloader, datasetinfo, **kwargs)
        headers = None
    elif format == 'ole':
        headers, iterator = read_ole(downloader, datasetinfo, **kwargs)
    elif format in ['csv', 'xls', 'xlsx']:
        if 'dataset' in datasetinfo:
            headers, iterator = read_hdx(downloader, datasetinfo, today=today)
        else:
            headers, iterator = read_tabular(downloader, datasetinfo, **kwargs)
    else:
        raise ValueError('Invalid format %s for %s!' % (format, datasetinfo['name']))
    return headers, iterator
