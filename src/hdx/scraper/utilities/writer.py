import logging
from copy import deepcopy
from typing import Dict, List, Optional, Tuple, Union

from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.typehint import ListTuple

from ..outputs.base import BaseOutput
from ..runner import Runner
from .sources import Sources

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

logger = logging.getLogger(__name__)


class Writer:
    """obtain output data and write it to specified outputs.

    Args:
        runner (Runner): Runner object
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
    """

    regional_headers = (("regionnames",), ("#region+name",))
    national_headers = (
        ["iso3", "countryname"],
        ["#country+code", "#country+name"],
    )
    subnational_headers = {
        1: (
            ("iso3", "countryname", "adm1_pcode", "adm1_name"),
            ("#country+code", "#country+name", "#adm1+code", "#adm1+name"),
        ),
        2: (
            ("iso3", "countryname", "adm2_pcode", "adm2_name"),
            ("#country+code", "#country+name", "#adm2+code", "#adm2+name"),
        ),
        3: (
            ("iso3", "countryname", "adm3_pcode", "adm3_name"),
            ("#country+code", "#country+name", "#adm3+code", "#adm3+name"),
        ),
        4: (
            ("iso3", "countryname", "adm4_pcode", "adm4_name"),
            ("#country+code", "#country+name", "#adm4+code", "#adm4+name"),
        ),
    }
    sources_headers = (
        ("Indicator", "Date", "Source", "Url"),
        ("#indicator+name", "#date", "#meta+source", "#meta+url"),
    )

    def __init__(self, runner: Runner, outputs: Dict[str, BaseOutput]):
        self.runner = runner
        self.outputs = outputs

    def update(self, name: str, data: Union[List, DataFrame]) -> None:
        """Update JSON key or tab in Excel or Google Sheets.

        Args:
            name (str): Name of tab (key in JSON) to update
            data (values: Union[List, DataFrame]): Data to output

        Returns:
            None
        """

        if not data:
            return
        logger.info(f"Updating tab: {name}")
        for output in self.outputs.values():
            output.update_tab(name, data)

    def get_toplevel_rows(
        self,
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
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            toplevel (str): Name of top level such as "global". Defaults to "allregions".

        Returns:
            List[List]: Rows for a given level
        """
        return self.runner.get_rows(
            toplevel, ("value",), names=names, overrides=overrides
        )

    def get_regional_rows(
        self,
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
            regional (ListTuple[str]): Regional admin names
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            level (str): Name of regional level. Defaults to "regional".

        Returns:
            List[List]: Rows for a given level
        """
        return self.runner.get_rows(
            level,
            regional,
            self.regional_headers,
            (lambda adm: adm,),
            names=names,
            overrides=overrides,
        )

    def update_toplevel(
        self,
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
        self.update(tab, toplevel_rows)

    def update_regional(
        self,
        regional_rows: List[List],
        toplevel_rows: Optional[List[List]] = None,
        toplevel_hxltags: Optional[ListTuple[str]] = None,
        tab: str = "regional",
        toplevel: str = "allregions",
    ) -> None:
        """Update the regional tab (or key in JSON) in the outputs. Optionally, further
        rows to output as regional can be obtained from the top level rows.

        Args:
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
        self.update(tab, regional_rows)

    def update_national(
        self,
        countries: ListTuple[str],
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
            countries (ListTuple[str]): Country names
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            flag_countries (Optional[Dict]): Countries to flag. Defaults to None.
            iso3_to_region (Optional[Dict]): Mapping from iso3 to region. Defaults to None.
            ignore_regions (ListTuple[str]): Regions to ignore. Defaults to tuple().
            level (str): Name of national level. Defaults to "national".
            tab (str): Name of tab (key in JSON) to update. Defaults to "national".

        Returns:
            None
        """
        headers = deepcopy(self.national_headers)
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

        rows = self.runner.get_rows(
            level, countries, headers, fns, names=names
        )
        if rows:
            self.update(tab, rows)

    def update_subnational(
        self,
        adminlevel: AdminLevel,
        names: Optional[ListTuple[str]] = None,
        level: str = "subnational",
        tab: str = "subnational",
    ) -> None:
        """Update the subnational tab (or key in JSON) in the outputs for scrapers limiting
        to those in names.

        Args:
            adminlevel (AdminLevel): AdminLevel object from HDX Python Country library
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            level (str): Name of subnational level. Defaults to "subnational".
            tab (str): Name of tab (key in JSON) to update. Defaults to "subnational".

        Returns:
            None
        """

        def get_country_name(adm):
            countryiso3 = adminlevel.pcode_to_iso3[adm]
            return Country.get_country_name_from_iso3(countryiso3)

        fns = (
            lambda adm: adminlevel.pcode_to_iso3[adm],
            get_country_name,
            lambda adm: adm,
            lambda adm: adminlevel.pcode_to_name[adm],
        )
        rows = self.runner.get_rows(
            level,
            adminlevel.pcodes,
            self.subnational_headers[
                adminlevel.admin_level
            ],  # use the main admin level (don't worry about overrides)
            fns,
            names=names,
        )
        self.update(tab, rows)

    def update_sources(
        self,
        additional_sources: ListTuple[Dict] = tuple(),
        names: Optional[ListTuple[str]] = None,
        secondary_runner: Optional[Runner] = None,
        custom_sources: ListTuple[Tuple] = list(),
        tab: str = "sources",
        should_overwrite_sources: Optional[bool] = None,
    ) -> None:
        """Update the sources tab (or key in JSON) in the outputs for scrapers limiting to
        those in names. Additional sources can be added. Each is a dictionary with indicator
        (specified with HXL hashtag), dataset or source and source_url as well as the
        source_date or whether to force_date_today. Custom sources can be directly passed
        to be appended. They are of form (indicator, date, source, source_url).

        Args:
            additional_sources (ListTuple[Dict]): Additional sources to add
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            secondary_runner (Optional[Runner]): Secondary Runner object. Defaults to None.
            custom_sources (ListTuple[Tuple]): Custom sources to add
            tab (str): Name of tab (key in JSON) to update. Defaults to "sources".
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (class default).

        Returns:
            None
        """
        sources = self.runner.get_sources(
            names=names,
            additional_sources=additional_sources,
            should_overwrite_sources=should_overwrite_sources,
        )
        hxltags = [source[0] for source in sources]
        if secondary_runner:
            Sources.add_sources_overwrite(
                hxltags,
                sources,
                secondary_runner.get_sources(),
                logger,
                should_overwrite_sources,
            )
        Sources.add_sources_overwrite(
            hxltags,
            sources,
            custom_sources,
            logger,
            should_overwrite_sources,
        )
        self.update(tab, list(self.sources_headers) + sources)
