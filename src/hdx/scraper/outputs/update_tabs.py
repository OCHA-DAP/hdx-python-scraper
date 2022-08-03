import logging
from copy import deepcopy
from typing import Dict, List, Optional, Tuple, Union

from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.utilities.typehint import ListTuple

from ..runner import Runner
from .base import BaseOutput

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

logger = logging.getLogger(__name__)

regional_headers = (("regionnames",), ("#region+name",))
national_headers = (
    ["iso3", "countryname"],
    ["#country+code", "#country+name"],
)
subnational_headers = (
    ("iso3", "countryname", "adm1_pcode", "adm1_name"),
    ("#country+code", "#country+name", "#adm1+code", "#adm1+name"),
)
sources_headers = (
    ("Indicator", "Date", "Source", "Url"),
    ("#indicator+name", "#date", "#meta+source", "#meta+url"),
)


def update_tab(
    outputs: Dict[str, BaseOutput], name: str, data: Union[List, DataFrame]
) -> None:
    """Run scraper with given name, adding sources and population to global
    dictionary. If scraper run fails and fallbacks have been set up, use them.

    Args:
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        name (str): Name of tab (key in JSON) to update
        data (values: Union[List, DataFrame]): Data to output

    Returns:
        None
    """

    if not data:
        return
    logger.info(f"Updating tab: {name}")
    for output in outputs.values():
        output.update_tab(name, data)


def get_toplevel_rows(
    runner: Runner,
    names: Optional[ListTuple[str]] = None,
    overrides: Dict[str, Dict] = dict(),
    toplevel: str = "allregions",
) -> List[List]:
    """Get rows for the given toplevel for scrapers limiting to those in names if given.
    Rows include header row, HXL hashtag row and a value row.  Sometimes it may be
    necessary to map alternative level names to the top level and this can be done
    using overrides. It is a dictionary with keys being scraper names and values being
    dictionaries which map level names to output levels.

    Args:
        runner (Runner): Runner object
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
        toplevel (str): Name of top level such as "global". Defaults to "allregions".

    Returns:
        List[List]: Rows for a given level
    """
    return runner.get_rows(
        toplevel, ("value",), names=names, overrides=overrides
    )


def get_regional_rows(
    runner: Runner,
    regional: ListTuple[str],
    names: Optional[ListTuple[str]] = None,
    overrides: Dict[str, Dict] = dict(),
    level: str = "regional",
):
    """Get regional rows for scrapers limiting to those in names if given using the
    level name given by level. Rows include header row, HXL hashtag row and value rows,
    one for each regional admin unit. The parameter regional is a list of regional
    admin names. Sometimes it may be necessary to map alternative level names to the
    regional level and this can be done using overrides. It is a dictionary with keys
    being scraper names and values being dictionaries which map level names to output
    levels.

    Args:
        runner (Runner): Runner object
        regional (ListTuple[str]): Regional admin names
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
        level (str): Name of regional level. Defaults to "regional".

    Returns:
        List[List]: Rows for a given level
    """
    return runner.get_rows(
        level,
        regional,
        regional_headers,
        (lambda adm: adm,),
        names=names,
        overrides=overrides,
    )


def update_toplevel(
    outputs: Dict[str, BaseOutput],
    toplevel_rows: List[List],
    tab: str = "allregions",
    regional_rows: Optional[List[List]] = None,
    regional_adm: str = "ALL",
    regional_hxltags: Optional[ListTuple[str]] = None,
    regional_first: bool = False,
) -> None:
    """Update the top level tab (or key in JSON) in the outputs. Optionally, further
    rows to output as top level can be obtained from the regional rows.

    Args:
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        toplevel_rows (List[List]): Header row, HXL tags row and top level value row
        tab (str): Name of tab (key in JSON) to update. Defaults to "allregions".
        regional_rows (Optional[List[List]]): Header, HXL tags and regional values. Defaults to None.
        regional_adm (str): The admin name of the top level in the regional data. Defaults to "ALL".
        regional_hxltags (Optional[ListTuple[str]]): What regional HXL tags to include. Defaults to None (all tags).
        regional_first (bool): Whether regional rows are output before top level rows. Defaults to False.

    Returns:
        None
    """
    if regional_rows is None:
        regional_rows = list()
    if not toplevel_rows:
        toplevel_rows = [list(), list(), list()]
    if regional_rows:
        adm_header = regional_rows[1].index("#region+name")
        rows_to_insert = (list(), list(), list())
        for row in regional_rows[2:]:
            if row[adm_header] == regional_adm:
                for i, hxltag in enumerate(regional_rows[1]):
                    if hxltag == "#region+name":
                        continue
                    if regional_hxltags and hxltag not in regional_hxltags:
                        continue
                    rows_to_insert[0].append(regional_rows[0][i])
                    rows_to_insert[1].append(hxltag)
                    rows_to_insert[2].append(row[i])
        if regional_first:
            toplevel_rows[0] = rows_to_insert[0] + toplevel_rows[0]
            toplevel_rows[1] = rows_to_insert[1] + toplevel_rows[1]
            toplevel_rows[2] = rows_to_insert[2] + toplevel_rows[2]
        else:
            toplevel_rows[0] += rows_to_insert[0]
            toplevel_rows[1] += rows_to_insert[1]
            toplevel_rows[2] += rows_to_insert[2]
    update_tab(outputs, tab, toplevel_rows)


def update_regional(
    outputs: Dict[str, BaseOutput],
    regional_rows: List[List],
    toplevel_rows: Optional[List[List]] = None,
    toplevel_hxltags: Optional[ListTuple[str]] = None,
    tab: str = "regional",
    toplevel: str = "allregions",
) -> None:
    """Update the regional tab (or key in JSON) in the outputs. Optionally, further
    rows to output as regional can be obtained from the top level rows.

    Args:
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        regional_rows (List[List]): Header row, HXL tags row and regional value rows
        toplevel_rows (Optional[List[List]]): Header, HXL tags and top level values. Defaults to None.
        toplevel_hxltags (Optional[ListTuple[str]]): What top level HXL tags to include. Defaults to None (all tags).
        tab (str): Name of tab (key in JSON) to update. Defaults to "regional".
        toplevel (str): Name of top level such as "global". Defaults to "allregions".

    Returns:
        None
    """
    if not regional_rows:
        return
    toplevel_values = dict()
    toplevel_headers = dict()
    if toplevel_rows:
        for i, hxltag in enumerate(toplevel_rows[1]):
            if toplevel_hxltags and hxltag not in toplevel_hxltags:
                continue
            toplevel_values[hxltag] = toplevel_rows[2][i]
            if hxltag not in regional_rows[1]:
                toplevel_headers[hxltag] = toplevel_rows[0][i]
    adm_header = regional_rows[1].index("#region+name")
    found_adm = False

    def add_value(row):
        value_found = False
        for i, hxltag in enumerate(regional_rows[1]):
            value = toplevel_values.get(hxltag)
            if value is None:
                continue
            row[i] = value
            value_found = True
        for hxltag, header in toplevel_headers.items():
            value = toplevel_values.get(hxltag)
            if value is None:
                continue
            regional_rows[0].append(header)
            regional_rows[1].append(hxltag)
            row.append(value)
            value_found = True
        return value_found

    for row in regional_rows[2:]:
        if row[adm_header] == toplevel:
            add_value(row)
            found_adm = True
            break
    if not found_adm:
        row = [toplevel]
        for _ in regional_rows[0][1:]:
            row.append(None)
        if add_value(row):
            regional_rows.append(row)
    length = len(regional_rows[0])
    for row in regional_rows[2:]:
        while len(row) < length:
            row.append(None)
    update_tab(outputs, tab, regional_rows)


def update_national(
    runner: Runner,
    countries: ListTuple[str],
    outputs: Dict[str, BaseOutput],
    names: Optional[ListTuple[str]] = None,
    flag_countries: Optional[Dict] = None,
    iso3_to_region: Optional[Dict] = None,
    ignore_regions: ListTuple[str] = tuple(),
    level="national",
    tab="national",
) -> None:
    """Update the national tab (or key in JSON) in the outputs for scrapers limiting to
    those in names. Certain additional columns can be added. One shows countries to be
    flagged (given a Y or N) and is configured using flag_countries, a dictionary which
    has keys header, hxltag and countries (whose corresponding value is a list or tuple
    of countries). Another shows regions a country is in and is specified by the mapping
    iso3_to_region. Some regions can be ignored using ignore_regions.

    Args:
        runner (Runner): Runner object
        countries (ListTuple[str]): Country names
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        flag_countries (Optional[Dict]): Countries to flag. Defaults to None.
        iso3_to_region (Optional[Dict]): Mapping from iso3 to region. Defaults to None.
        ignore_regions (ListTuple[str]): Regions to ignore. Defaults to tuple().
        level (str): Name of national level. Defaults to "national".
        tab (str): Name of tab (key in JSON) to update. Defaults to "national".

    Returns:
        None
    """
    headers = deepcopy(national_headers)
    fns = [
        lambda adm: adm,
        lambda adm: Country.get_country_name_from_iso3(adm),
    ]

    if flag_countries:
        headers[0].append(flag_countries["header"])
        headers[1].append(flag_countries["hxltag"])
        isfc_fn = (
            lambda adm: "Y" if adm in flag_countries["countries"] else "N"
        )
        fns.append(isfc_fn)

    if iso3_to_region:
        headers[0].append("region")
        headers[1].append("#region+name")

        def region_fn(adm):
            regions = sorted(list(iso3_to_region[adm]))
            for region in reversed(regions):
                if ignore_regions and region in ignore_regions:
                    regions.remove(region)
            return "|".join(regions)

        fns.append(region_fn)

    rows = runner.get_rows(level, countries, headers, fns, names=names)
    if rows:
        update_tab(outputs, tab, rows)


def update_subnational(
    runner: Runner,
    adminone: AdminOne,
    outputs: Dict[str, BaseOutput],
    names: Optional[ListTuple[str]] = None,
    level: str = "subnational",
    tab: str = "subnational",
) -> None:
    """Update the subnational tab (or key in JSON) in the outputs for scrapers limiting
    to those in names.

    Args:
        runner (Runner): Runner object
        adminone (AdminOne): AdminOne object
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        level (str): Name of subnational level. Defaults to "subnational".
        tab (str): Name of tab (key in JSON) to update. Defaults to "subnational".

    Returns:
        None
    """

    def get_country_name(adm):
        countryiso3 = adminone.pcode_to_iso3[adm]
        return Country.get_country_name_from_iso3(countryiso3)

    fns = (
        lambda adm: adminone.pcode_to_iso3[adm],
        get_country_name,
        lambda adm: adm,
        lambda adm: adminone.pcode_to_name[adm],
    )
    rows = runner.get_rows(
        level, adminone.pcodes, subnational_headers, fns, names=names
    )
    update_tab(outputs, tab, rows)


def update_sources(
    runner: Runner,
    outputs: Dict[str, BaseOutput],
    additional_sources: ListTuple[str] = tuple(),
    names: Optional[ListTuple[str]] = None,
    secondary_runner: Optional[Runner] = None,
    custom_sources: ListTuple[Tuple] = list(),
    tab: str = "sources",
) -> None:
    """Update the sources tab (or key in JSON) in the outputs for scrapers limiting to
    those in names. Additional sources can be added. Each is a dictionary with indicator
    (specified with HXL hash tag), dataset or source and source_url as well as the
    source_date or whether to force_date_today. Custom sources can be directly passed
    to be appended. They are of form (indicator, date, source, source_url).

    Args:
        runner (Runner): Runner object
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
        additional_sources (ListTuple[Dict]): Additional sources to add
        names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
        secondary_runner (Optional[Runner]): Secondary Runner object. Defaults to None.
        custom_sources (ListTuple[Tuple]): Custom sources to add
        tab (str): Name of tab (key in JSON) to update. Defaults to "sources".

    Returns:
        None
    """
    sources = runner.get_sources(
        names=names,
        additional_sources=additional_sources,
    )
    if secondary_runner:
        secondary_sources = secondary_runner.get_sources()
        sources.extend(secondary_sources)
    sources.extend(custom_sources)
    update_tab(outputs, tab, list(sources_headers) + sources)
