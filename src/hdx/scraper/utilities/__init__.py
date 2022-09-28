import re
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

from hdx.data.dataset import Dataset
from hdx.utilities.typehint import ListTuple

template = re.compile("{{.*?}}")


def string_params_to_dict(string: str) -> Dict[str, str]:
    params = dict()
    if not string:
        params
    for name_par in string.split(","):
        name, par = name_par.strip().split(":")
        params[name] = par.strip()
    return params


def match_template(input: str) -> Tuple[Optional[str], Optional[str]]:
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


def get_rowval(row: Dict, valcol: str) -> Any:
    """Get the value of a particular column in a row expanding any template it contains

    Args:
        row (Dict): Dictionary
        valcol (str): Column which may be a string in which to look for template

    Returns:
        Any: Value of column or template
    """
    if "{{" in valcol:
        repvalcol = valcol
        for match in template.finditer(valcol):
            template_string = match.group()
            replace_string = f'row["{template_string[2:-2]}"]'
            repvalcol = repvalcol.replace(template_string, replace_string)
        return eval(repvalcol)
    else:
        result = row[valcol]
        if isinstance(result, str):
            return result.strip()
        return result


def get_date_from_dataset_date(
    dataset: Dataset, today: Optional[datetime] = None
) -> Optional[datetime]:
    """Return the date or end date of a dataset

    Args:
        dataset (Dataset): Dataset object
        today (Optional[datetime]): Date to use for today. Defaults to None (datetime.utcnow)

    Returns:
        Optional[datetime]: Date or end date of a dataset
    """
    if today is None:
        date_info = dataset.get_date_of_dataset()
    else:
        date_info = dataset.get_date_of_dataset(today=today)
    return date_info.get("enddate")


def get_isodate_from_dataset_date(
    dataset: Dataset, today: Optional[datetime] = None
) -> Optional[str]:
    """Return the date or end date of a dataset as an iso formatted date

    Args:
        dataset (Dataset): Dataset object
        today (Optional[datetime]): Date to use for today. Defaults to None (datetime.utcnow)

    Returns:
        Optional[str]: Date or end date of a dataset
    """
    date = get_date_from_dataset_date(dataset, today)
    if date:
        date = date.strftime("%Y-%m-%d")
    return date


def add_source_overwrite(
    hxltags: List[str],
    sources: List[ListTuple],
    source: ListTuple[str],
    logger: Logger,
):
    """Add source to sources preventing duplication

    Args:
        hxltags (List[str]): List of HXL hashtags, one for each source name
        sources (List[ListTuple]): List of sources
        source (ListTuple[str]): Source information
        logger (Logger): Logegr to log warnings to

    Returns:
        None
    """
    hxltag = source[0]
    if hxltag in hxltags:
        logger.warning(f"Overwriting source information for {hxltag}!")
        index = hxltags.index(hxltag)
        del hxltags[index]
        del sources[index]
    hxltags.append(hxltag)
    sources.append(source)


def add_sources_overwrite(
    hxltags: List[str],
    sources: List[ListTuple],
    sources_to_add: List[ListTuple],
    logger: Logger,
):
    """Add source to sources preventing duplication

    Args:
        hxltags (List[str]): List of HXL hashtags, one for each source name
        sources (List[ListTuple]): List of sources
        sources_to_add (List[ListTuple]): List of sources to add
        logger (Logger): Logegr to log warnings to

    Returns:
        None
    """
    for source in sources_to_add:
        add_source_overwrite(hxltags, sources, source, logger)
