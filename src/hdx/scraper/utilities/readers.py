import logging
from collections.abc import MutableMapping
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import BaseDownload

from . import get_date_from_dataset_date, match_template

logger = logging.getLogger(__name__)

fixed_dataset_date = None


def get_url(url: str, **kwargs: Any) -> str:
    """Get url from a string replacing any template arguments

    Args:
        url (str): Url to read
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        str: Url with any template arguments replaced
    """
    for kwarg in kwargs:
        exec(f'{kwarg}="{kwargs[kwarg]}"')
    template_string, match_string = match_template(url)
    if template_string:
        replace_string = eval(match_string)
        url = url.replace(template_string, replace_string)
    return url


def read_tabular(
    downloader: BaseDownload, datasetinfo: MutableMapping, **kwargs: Any
) -> Tuple[List[str], Iterator[Dict]]:
    """Read data from tabular source eg. csv, xls, xlsx

    Args:
        downloader (BaseDownload): Download object for downloading files
        datasetinfo (MutableMapping): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
    """
    url = get_url(datasetinfo["url"], **kwargs)
    sheet = datasetinfo.get("sheet")
    headers = datasetinfo.get("headers")
    if headers is None:
        headers = 1
        datasetinfo["headers"] = 1
    kwargs["headers"] = headers
    if isinstance(headers, list):
        kwargs["fill_merged_cells"] = True
    format = datasetinfo["format"]
    kwargs["format"] = format
    if not sheet and format in ("xls", "xlsx"):
        sheet = 1
    if sheet:
        kwargs["sheet"] = sheet
    compression = datasetinfo.get("compression")
    if compression:
        kwargs["compression"] = compression
    return downloader.get_tabular_rows(
        url,
        dict_form=True,
        **kwargs,
    )


def read_hdx_metadata(
    datasetinfo: MutableMapping, today: Optional[datetime] = None
) -> Optional[Resource]:
    """Read metadata from HDX dataset and add to input dictionary

    Args:
        datasetinfo (MutableMapping): Dictionary of information about dataset
        today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).

    Returns:
        Optional[Resource]: The resource if a url was not given
    """
    dataset_name = datasetinfo["dataset"]
    dataset = Dataset.read_from_hdx(dataset_name)
    resource = None
    url = datasetinfo.get("url")
    if not url:
        resource_name = datasetinfo.get("resource")
        format = datasetinfo["format"].lower()
        for resource in dataset.get_resources():
            if resource["format"].lower() == format:
                if resource_name and resource["name"] != resource_name:
                    continue
                url = resource["url"]
                break
        if not url:
            raise ValueError(
                f"Cannot find {format} resource in {dataset_name}!"
            )
        datasetinfo["url"] = url
    date = datasetinfo.get("source_date")
    if date:
        if isinstance(date, str):
            datasetinfo["source_date"] = parse_date(date)
    else:
        if fixed_dataset_date:
            datasetinfo["source_date"] = fixed_dataset_date
        else:
            datasetinfo["source_date"] = get_date_from_dataset_date(
                dataset, today=today
            )
    if "source" not in datasetinfo:
        datasetinfo["source"] = dataset["dataset_source"]
    if "source_url" not in datasetinfo:
        datasetinfo["source_url"] = dataset.get_hdx_url()
    return resource


def read_hdx(
    downloader: BaseDownload,
    datasetinfo: MutableMapping,
    today: Optional[datetime] = None,
    **kwargs: Any,
) -> Tuple[List[str], Iterator[Dict]]:
    """Read data and metadata from HDX dataset

    Args:
        downloader (BaseDownload): Download object for downloading files
        datasetinfo (MutableMapping): Dictionary of information about dataset
        **kwargs: Variables to use when evaluating template arguments

    Returns:
        Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
    """
    read_hdx_metadata(datasetinfo, today=today)
    return read_tabular(downloader, datasetinfo, **kwargs)


def read(
    downloader: BaseDownload,
    datasetinfo: MutableMapping,
    today: Optional[datetime] = None,
    **kwargs: Any,
) -> Tuple[List[str], Iterator[Dict]]:
    """Read data and metadata from HDX dataset

    Args:
        downloader (BaseDownload): Download object for downloading files
        datasetinfo (MutableMapping): Dictionary of information about dataset
        today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).
        **kwargs: Variables to use when evaluating template arguments in urls

    Returns:
        Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
    """
    format = datasetinfo["format"]
    if format in ["json", "csv", "xls", "xlsx"]:
        if "dataset" in datasetinfo:
            headers, iterator = read_hdx(
                downloader, datasetinfo, today=today, **kwargs
            )
        else:
            headers, iterator = read_tabular(downloader, datasetinfo, **kwargs)
    else:
        raise ValueError(f"Invalid format {format} for {datasetinfo['name']}!")
    return headers, iterator
