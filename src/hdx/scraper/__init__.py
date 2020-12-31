# -*- coding: utf-8 -*-
import re
from datetime import datetime
from typing import Tuple, Optional, Any, Dict, Union, List

from hdx.data.dataset import Dataset

template = re.compile('{{.*?}}')


def match_template(input):
    # type: (str) -> Tuple[Optional[str], Optional[str]]
    """Try to match {{XXX}} in input string

    Args:
        input (str): String in which to look for template

    Returns:
        Tuple[Optional[str], Optional[str]]: (Matched string with brackets, matched string without brackets)
    """
    match = template.search(input)
    if match:
        template_string = match.group()
        return template_string, template_string[2:-2]
    return None, None


def get_rowval(row, valcol):
    # type: (Dict, str) -> Any
    """Get the value of a particular column in a row expanding any template it contains

    Args:
        row (Dict): Dictionary
        valcol (str): Column which may be a string in which to look for template

    Returns:
        Any: Value of column or template
    """
    if '{{' in valcol:
        repvalcol = valcol
        for match in template.finditer(valcol):
            template_string = match.group()
            replace_string = 'row["%s"]' % template_string[2:-2]
            repvalcol = repvalcol.replace(template_string, replace_string)
        return eval(repvalcol)
    else:
        result = row[valcol]
        if isinstance(result, str):
            return result.strip()
        return result


def get_date_from_dataset_date(dataset, today=None):
    # type: (Union[Dataset, str], datetime) -> Optional[str]
    """Return the date or end date of a dataset

    Args:
        dataset (Union[Dataset, str]): Dataset object or name or id of dataset
        today (datetime): Date to use for today. Defaults to None (datetime.now())

    Returns:
        Any: Value of column or template
    """
    if isinstance(dataset, str):
        dataset = Dataset.read_from_hdx(dataset)
    if today is None:
        date_info = dataset.get_date_of_dataset()
    else:
        date_info = dataset.get_date_of_dataset(today=today)
    enddate = date_info.get('enddate_str')
    if not enddate:
        return None
    return enddate[:10]


def add_population(population_lookup, headers, columns):
    # type: (Dict, List[str], List[Dict]) -> None
    """Add population data to dictionary

    Args:
        population_lookup (Dict): Population dictionary
        headers (List[str]): List of headers
        columns (List[Dict]): List of columns

    Returns:
        None
    """
    if population_lookup is None:
        return
    try:
        population_index = headers[1].index('#population')
    except ValueError:
        population_index = None
    if population_index is not None:
        for key, value in columns[population_index].items():
            try:
                valint = int(value)
                population_lookup[key] = valint
            except ValueError:
                pass
