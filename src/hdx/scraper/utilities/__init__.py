import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from hdx.data.dataset import Dataset

template = re.compile("{{.*?}}")


def string_params_to_dict(string: str) -> Dict[str, str]:
    params = dict()
    if not string:
        return params
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


def get_startend_dates_from_dataset_date(
    dataset: Dataset, today: Optional[datetime] = None
) -> Optional[Dict]:
    """Return the date of a dataset in form required for source_date

    Args:
        dataset (Dataset): Dataset object
        today (Optional[datetime]): Date to use for today. Defaults to None (datetime.utcnow)

    Returns:
        Optional[Dict]: Date of a dataset in form required for source_date
    """
    if today is None:
        date_info = dataset.get_date_of_dataset()
    else:
        date_info = dataset.get_date_of_dataset(today=today)
    startdate = date_info.get("startdate")
    enddate = date_info.get("enddate")
    if enddate is None:
        return None
    if startdate.date() == enddate.date():
        return {"end": enddate}
    return {"start": startdate, "end": enddate}
