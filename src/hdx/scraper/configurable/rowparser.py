import copy
import logging
from datetime import datetime
from operator import itemgetter
from typing import Dict, Generator, Iterator, List, Optional, Tuple

import hxl
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

from ..utilities import match_template

logger = logging.getLogger(__name__)


class RowParser:
    """RowParser class for parsing each row.

    Args:
        name (str): Name of scraper
        countryiso3s (List[str]): List of ISO3 country codes to process
        adminone (AdminOne): AdminOne object from HDX Python Country library that handles processing of admin level_type 1
        level (str): Can be national, subnational or single
        datelevel (str): Can be global, regional, national, subnational
        datasetinfo (Dict): Dictionary of information about dataset
        headers (List[str]): Row headers
        header_to_hxltag (Optional[Dict[str, str]]): Mapping from headers to HXL hashtags or None
        subsets (List[Dict]): List of subset definitions
        maxdateonly (bool): Whether to only take the most recent date. Defaults to True.
    """

    def __init__(
        self,
        name: str,
        countryiso3s: List[str],
        adminone: AdminOne,
        level: str,
        datelevel: str,
        datasetinfo: Dict,
        headers: List[str],
        header_to_hxltag: Optional[Dict[str, str]],
        subsets: List[Dict],
        maxdateonly: bool = True,
    ) -> None:
        def get_level(level: str) -> Optional[int]:
            """Get the level_name as a number. "Single" valued outputs are typically
            regional or global

            Args:
                level (str): Can be national, subnational or single (for a single value)

            Returns:
                Optional[int]: Level as a number
            """
            if level == "single":
                return None
            elif level == "national":
                return 0
            else:
                return 1

        self.name = name
        self.level = get_level(level)
        self.datelevel = get_level(datelevel)
        self.today = datasetinfo["source_date"]
        self.sort = datasetinfo.get("sort")
        self.stop_row = datasetinfo.get("stop_row")
        self.datecol = datasetinfo.get("date")
        self.datetype = datasetinfo.get("date_type")
        if self.datetype:
            if self.datetype == "date":
                date = parse_date("1900-01-01")
            else:
                date = 0
        else:
            date = 0
        self.maxdate = date
        self.single_maxdate = datasetinfo.get("single_maxdate", False)
        self.ignore_future_date = datasetinfo.get("ignore_future_date", True)
        self.adminone = adminone
        self.admcols = datasetinfo.get("admin", list())
        self.admexact = datasetinfo.get("admin_exact", False)
        self.admsingle = datasetinfo.get("admin_single", None)
        if self.admsingle:
            self.datelevel = None
        self.subsets = subsets
        self.filter_cols = datasetinfo.get("filter_cols", list())
        prefilter = datasetinfo.get("prefilter")
        if prefilter is not None:
            prefilter = self.get_filter_str_for_eval(prefilter)
        self.prefilter = prefilter
        adms = datasetinfo.get("admin_filter")
        if adms is None:
            self.adms = [countryiso3s, self.adminone.pcodes]
        else:
            if self.datelevel == 1:
                self.adms = adms
            else:
                self.adms = [adms, self.adminone.pcodes]
        if self.datelevel is None:
            self.maxdates = {i: date for i, _ in enumerate(subsets)}
        else:
            if self.datelevel > len(self.admcols):
                raise ValueError(
                    "No admin columns specified for required level_type!"
                )
            self.maxdates = {
                i: {adm: date for adm in self.adms[self.datelevel]}
                for i, _ in enumerate(subsets)
            }

        self.maxdateonly = maxdateonly
        self.flatteninfo = datasetinfo.get("flatten")
        self.headers = headers
        self.header_to_hxltag: Optional[Dict[str, str]] = header_to_hxltag
        self.filters = dict()
        self.read_external_filter(datasetinfo.get("external_filter"))

    def read_external_filter(self, external_filter: Optional[Dict]) -> None:
        """Read filter list from external url pointing to a HXLated file

        Args:
            external_filter (Optional[Dict]): External filter information in dictionary

        Returns:
            None
        """
        if not external_filter:
            return
        hxltags = external_filter["hxl"]
        data = hxl.data(external_filter["url"])
        for row in data:
            for hxltag in data.columns:
                if hxltag.display_tag in hxltags:
                    if self.header_to_hxltag:
                        header = hxltag.display_tag
                    else:
                        header = hxltag.header
                    dict_of_lists_add(
                        self.filters, header, row.get("#country+code")
                    )

    def get_filter_str_for_eval(self, filter):
        if self.filter_cols:
            for col in self.filter_cols:
                filter = filter.replace(col, f"row['{col}']")
        else:
            if self.datecol:
                filter = filter.replace(self.datecol, f"row['{self.datecol}']")
            for subset in self.subsets:
                for col in subset["input"]:
                    filter = filter.replace(col, f"row['{col}']")
        return filter

    def filter_sort_rows(self, iterator: Iterator[Dict]) -> Iterator[Dict]:
        """Apply prefilter and sort the input data before processing. If date_col is
        specified along with any of sum or process, and sorting is not specified, then
        apply a sort by date to ensure correct results.

        Args:
            iterator (Iterator[Dict]): Input data
        Returns:
            Iterator[Dict]: Input data with prefilter applied if specified and sorted if specified or deemed necessary
        """
        rows = list()
        for row in iterator:
            if self.header_to_hxltag:
                newrow = dict()
                for header in row:
                    newrow[self.header_to_hxltag[header]] = row[header]
                row = newrow
            if self.stop_row:
                if all(
                    row[key] == value for key, value in self.stop_row.items()
                ):
                    break
            for newrow in self.flatten(row):
                rows.append(newrow)
        if not self.sort:
            if self.datecol:
                for subset in self.subsets:
                    apply_sort = subset.get(
                        "sum",
                        subset.get("process", subset.get("input_append")),
                    )
                    if apply_sort:
                        logger.warning(
                            "sum or process used without sorting. Applying sort by date to ensure correct results!"
                        )
                        self.sort = {"keys": [self.datecol], "reverse": True}
                        break
        if self.prefilter:
            rows = [row for row in rows if eval(self.prefilter)]
        if self.sort:
            keys = self.sort["keys"]
            reverse = self.sort.get("reverse", False)
            rows = sorted(list(rows), key=itemgetter(*keys), reverse=reverse)
        return rows

    def flatten(self, row: Dict) -> Generator[Dict, None, None]:
        """Flatten a wide spreadsheet format into a long one

        Args:
            row (Dict): Row to flatten

        Returns:
            Generator[Dict]: Flattened row(s)
        """
        if not self.flatteninfo:
            yield row
            return
        counters = [-1 for _ in self.flatteninfo]
        while True:
            newrow = copy.deepcopy(row)
            for i, flatten in enumerate(self.flatteninfo):
                colname = flatten["original"]
                template_string, replace_string = match_template(colname)
                if not template_string:
                    raise ValueError(
                        "Column name for flattening lacks an incrementing number!"
                    )
                if counters[i] == -1:
                    counters[i] = int(replace_string)
                else:
                    replace_string = f"{counters[i]}"
                colname = colname.replace(template_string, replace_string)
                if colname not in row:
                    return
                newrow[flatten["new"]] = row[colname]
                extracol = flatten.get("extracol")
                if extracol:
                    newrow[extracol] = colname
                counters[i] += 1
            yield newrow

    def get_maxdate(self) -> datetime:
        """Get the most recent date of the rows so far

        Returns:
            datetime: Most recent date in processed rows
        """
        return self.maxdate

    def filtered(self, row: Dict) -> bool:
        """Check if the row should be filtered out

        Args:
            row (Dict): Row to check for filters

        Returns:
            bool: Whether row is filtered out or not
        """
        for header in self.filters:
            if header not in row:
                continue
            if row[header] not in self.filters[header]:
                return True
        return False

    def parse(self, row: Dict) -> Tuple[Optional[str], Optional[List[bool]]]:
        """Parse row checking for valid admin information and if the row should be filtered out in each subset given
        its definition.

        Args:
            row (Dict): Row to parse

        Returns:
            Tuple[Optional[str], Optional[List[bool]]]: (admin name, should process subset list) or (None, None)
        """
        if self.filtered(row):
            return None, None

        adms = [None for _ in range(len(self.admcols))]

        def get_adm(admcol, i):
            template_string, match_string = match_template(admcol)
            if template_string and self.headers:
                admcol = self.headers[int(match_string)]
            adm = row[admcol]
            if not adm:
                return False
            adm = adm.strip()
            adms[i] = adm
            if adm in self.adms[i]:
                return True
            exact = False
            if self.admexact:
                adms[i] = None
            else:
                if i == 0:
                    adms[i], exact = Country.get_iso3_country_code_fuzzy(adm)
                elif i == 1:
                    adms[i], exact = self.adminone.get_pcode(
                        adms[0], adm, self.name
                    )
                if adms[i] not in self.adms[i]:
                    adms[i] = None
            return exact

        for i, admcol in enumerate(self.admcols):
            if admcol is None:
                continue
            if isinstance(admcol, str):
                admcol = [admcol]
            for admcl in admcol:
                exact = get_adm(admcl, i)
                if adms[i] and exact:
                    break
            if not adms[i]:
                return None, None

        should_process_subset = list()
        for subset in self.subsets:
            filter = subset["filter"]
            process = True
            if filter:
                filter = self.get_filter_str_for_eval(filter)
                if not eval(filter):
                    process = False
            should_process_subset.append(process)

        if self.datecol:
            if isinstance(self.datecol, list):
                dates = [str(row[x]) for x in self.datecol]
                date = "".join(dates)
            else:
                date = row[self.datecol]
            if self.datetype == "date":
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
                if date > self.today and self.ignore_future_date:
                    return None, None
            elif self.datetype == "year":
                date = int(date)
                if date > self.today.year and self.ignore_future_date:
                    return None, None
            else:
                date = int(date)
            for i, process in enumerate(should_process_subset):
                if not process:
                    continue
                if date < self.maxdate:
                    if self.single_maxdate:
                        should_process_subset[i] = False
                else:
                    self.maxdate = date
                if self.datelevel is None:
                    if self.maxdateonly:
                        if date < self.maxdates[i]:
                            should_process_subset[i] = False
                        else:
                            self.maxdates[i] = date
                    else:
                        self.maxdates[i] = date
                else:
                    if self.maxdateonly:
                        if date < self.maxdates[i][adms[self.datelevel]]:
                            should_process_subset[i] = False
                        else:
                            self.maxdates[i][adms[self.datelevel]] = date
                    else:
                        self.maxdates[i][adms[self.datelevel]] = date
        if self.level is None:
            return "value", should_process_subset
        if self.admsingle:
            return self.admsingle, should_process_subset
        return adms[self.level], should_process_subset
