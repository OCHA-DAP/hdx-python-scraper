import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import regex
from hdx.location.adminone import AdminOne
from hdx.utilities.dateparse import get_datetime_from_timestamp, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.text import (  # noqa: F401
    get_fraction_str,
    get_numeric_if_possible,
    number_format,
)

from hdx.scraper.fallbacks import use_fallbacks
from hdx.scraper.readers import read
from hdx.scraper.rowparser import RowParser
from hdx.scraper.utils import get_level, get_rowval

logger = logging.getLogger(__name__)


class Scraper:
    """Runs one mini scraper given dataset information and returns headers, values and
    sources.

    Args:
        datasetinfo (Dict): Information about dataset
        level (Optional[int]): Can be None, 0 or 1
        countryiso3s (List[str]): List of ISO3 country codes to process
        adminone (AdminOne): AdminOne object from HDX Python Country library
        downloader (Download): Download object for downloading files
        today (datetime): Value to use for today. Defaults to datetime.now().
        population_lookup (Optional[Dict[str,int]]): Admin code to population dict. Defaults to None.
        fallbacks (Optional[Dict]): Fallback data to use. Defaults to None.
    """

    brackets = r"""
    (?<rec> #capturing group rec
     \( #open parenthesis
     (?: #non-capturing group
      [^()]++ #anything but parenthesis one or more times without backtracking
      | #or
       (?&rec) #recursive substitute of group rec
     )*
     \) #close parenthesis
    )"""

    def __init__(
        self,
        datasetinfo: Dict,
        level: Optional[int],
        countryiso3s: List[str],
        adminone: AdminOne,
        downloader: Download,
        today: datetime = datetime.now(),
        population_lookup: Optional[Dict[str, int]] = None,
        fallbacks: Optional[Dict] = None,
    ):
        self.datasetinfo = datasetinfo
        self.level: Optional[int] = level
        datelevel = datasetinfo.get("date_level")
        if datelevel is None:
            self.datelevel = self.level
        else:
            self.datelevel = get_level(datelevel)
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.downloader = downloader
        self.today = today
        self.population_lookup: Optional[Dict[str, int]] = population_lookup
        self.fallbacks: Optional[Dict] = fallbacks
        self.subsets = self.get_subsets_from_datasetinfo()
        self.headers = (list(), list())
        for subset in self.subsets:
            self.headers[0].extend(subset["output_cols"])
            self.headers[1].extend(subset["output_hxltags"])
        self.rowparser = None

    def get_subsets_from_datasetinfo(self) -> List[Dict]:
        """Get subsets from dataset information

        Returns:
            List[Dict]: List of subsets
        """
        subsets = self.datasetinfo.get("subsets")
        if not subsets:
            subsets = [
                {
                    "filter": self.datasetinfo.get("filter"),
                    "input_cols": self.datasetinfo.get("input_cols", list()),
                    "input_transforms": self.datasetinfo.get(
                        "input_transforms", dict()
                    ),
                    "process_cols": self.datasetinfo.get(
                        "process_cols", list()
                    ),
                    "input_keep": self.datasetinfo.get("input_keep", list()),
                    "input_append": self.datasetinfo.get(
                        "input_append", list()
                    ),
                    "sum_cols": self.datasetinfo.get("sum_cols"),
                    "input_ignore_vals": self.datasetinfo.get(
                        "input_ignore_vals", list()
                    ),
                    "output_cols": self.datasetinfo.get("output_cols", list()),
                    "output_hxltags": self.datasetinfo.get(
                        "output_hxltags", list()
                    ),
                }
            ]
        return subsets

    def get_sources(
        self,
    ) -> List[Tuple]:
        """Get source for each HXL hashtag

        Returns:
            List[Tuple]: Source for each HXL hashtag
        """
        sources = list()
        source = self.datasetinfo["source"]
        if isinstance(source, str):
            source = {"default_source": source}
        source_url = self.datasetinfo["source_url"]
        if isinstance(source_url, str):
            source_url = {"default_url": source_url}

        date = self.datasetinfo.get("date")
        use_date_from_date_col = self.datasetinfo.get(
            "use_date_from_date_col", False
        )
        if not date or use_date_from_date_col:
            date = self.rowparser.get_maxdate()
            if date == 0:
                raise ValueError(
                    "No date given in datasetinfo or as a column!"
                )
            if self.rowparser.datetype == "date":
                if not isinstance(date, datetime):
                    date = parse_date(date)
            elif self.rowparser.datetype == "int":
                date = get_datetime_from_timestamp(date)
            else:
                raise ValueError("No date type specified!")
        date = date.strftime("%Y-%m-%d")

        sources.extend(
            [
                (
                    hxltag,
                    date,
                    source.get(hxltag, source["default_source"]),
                    source_url.get(hxltag, source_url["default_url"]),
                )
                for hxltag in self.headers[1]
            ]
        )
        return sources

    def use_hxl(
        self, headers: List[str], iterator: Iterator[Dict]
    ) -> Optional[Dict]:
        """If the mini scraper configuration defines that HXL is used (use_hxl is True),
        then read the mapping from headers to HXL hash tags. Since each row is a
        dictionary from header to value, the HXL row will be a dictionary from header
        to HXL hashtag. Label #country and #adm1 columns as admin columns. If the
        input columns to use are not specified, use all that have HXL hashtags. If the
        output column headers or hashtags are not specified, use the ones from the
        original file.

        Args:
            headers (List[str]): List of all headers of input file
            iterator (Iterator[Dict]): Iterator over the rows

        Returns:
            Optional[Dict]: Dictionary that maps from header to HXL hashtag or None
        """
        use_hxl = self.datasetinfo.get("use_hxl", False)
        if not use_hxl:
            return None
        header_to_hxltag = next(iterator)
        while not header_to_hxltag:
            header_to_hxltag = next(iterator)
        exclude_tags = self.datasetinfo.get("exclude_tags", list())
        find_tags = self.datasetinfo.get("find_tags")
        adm_cols = list()
        input_cols = list()
        columns = list()
        for header in headers:
            hxltag = header_to_hxltag[header]
            if not hxltag or hxltag in exclude_tags:
                continue
            if find_tags or (find_tags is None and self.datelevel is not None):
                if "#country" in hxltag:
                    if "code" in hxltag:
                        if len(adm_cols) == 0:
                            adm_cols.append(hxltag)
                        else:
                            adm_cols[0] = hxltag
                    continue
                if find_tags or self.datelevel != 0:
                    if "#adm1" in hxltag:
                        if "code" in hxltag:
                            if len(adm_cols) == 0:
                                adm_cols.append(None)
                            if len(adm_cols) == 1:
                                adm_cols.append(hxltag)
                        continue
            if (
                hxltag == self.datasetinfo.get("date_col")
                and self.datasetinfo.get("include_date", False) is False
            ):
                continue
            input_cols.append(hxltag)
            columns.append(header)
        self.datasetinfo["adm_cols"] = adm_cols
        self.headers = (list(), list())
        for subset in self.subsets:
            orig_input_cols = subset.get("input_cols", list())
            if not orig_input_cols:
                orig_input_cols.extend(input_cols)
            subset["input_cols"] = orig_input_cols
            orig_columns = subset.get("output_cols", list())
            if not orig_columns:
                orig_columns.extend(columns)
            subset["output_cols"] = orig_columns
            orig_hxltags = subset.get("output_hxltags", list())
            if not orig_hxltags:
                orig_hxltags.extend(input_cols)
            subset["output_hxltags"] = orig_hxltags
            self.headers[0].extend(orig_columns)
            self.headers[1].extend(orig_hxltags)
        return header_to_hxltag

    def run_scraper(
        self,
        iterator: Iterator[Dict],
    ) -> List[Dict]:
        """Run one mini scraper given an iterator over the rows

        Args:
            iterator (Iterator[Dict]): Iterator over the rows

        Returns:
            List[Dict]: Values
        """

        valuedicts = dict()
        for subset in self.subsets:
            for _ in subset["input_cols"]:
                dict_of_lists_add(valuedicts, subset["filter"], dict())

        def add_row(row):
            adm, should_process_subset = self.rowparser.parse(row)
            if not adm:
                return
            for i, subset in enumerate(self.subsets):
                if not should_process_subset[i]:
                    continue
                filter = subset["filter"]
                input_ignore_vals = subset.get("input_ignore_vals", list())
                input_transforms = subset.get("input_transforms", dict())
                sum_cols = subset.get("sum_cols")
                process_cols = subset.get("process_cols")
                input_append = subset.get("input_append", list())
                input_keep = subset.get("input_keep", list())
                for i, valcol in enumerate(subset["input_cols"]):
                    valuedict = valuedicts[filter][i]
                    val = get_rowval(row, valcol)
                    input_transform = input_transforms.get(valcol)
                    if input_transform and val not in input_ignore_vals:
                        val = eval(input_transform.replace(valcol, "val"))
                    if sum_cols or process_cols:
                        dict_of_lists_add(valuedict, adm, val)
                    else:
                        curval = valuedict.get(adm)
                        if valcol in input_append:
                            if curval:
                                val = curval + val
                        elif valcol in input_keep:
                            if curval:
                                val = curval
                        valuedict[adm] = val

        for row in self.rowparser.filter_sort_rows(iterator):
            add_row(row)

        retvalues = list()
        for subset in self.subsets:
            valdicts = valuedicts[subset["filter"]]
            process_cols = subset.get("process_cols")
            input_keep = subset.get("input_keep", list())
            sum_cols = subset.get("sum_cols")
            input_ignore_vals = subset.get("input_ignore_vals", list())
            valcols = subset["input_cols"]
            # Indices of list sorted by length
            sorted_len_indices = sorted(
                range(len(valcols)),
                key=lambda k: len(valcols[k]),
                reverse=True,
            )
            if process_cols:
                newvaldicts = [dict() for _ in process_cols]

                def text_replacement(string, adm):
                    # pzbgvjh is arbitrary! It is simply to prevent accidental replacement
                    # of all or parts of #population (if it is in the string).
                    arbitrary_string = "#pzbgvjh"
                    string = string.replace("#population", arbitrary_string)
                    hasvalues = False
                    for j in sorted_len_indices:
                        valcol = valcols[j]
                        if valcol not in string:
                            continue
                        if valcol in input_keep:
                            input_keep_index = 0
                        else:
                            input_keep_index = -1
                        val = valdicts[j][adm][input_keep_index]
                        if (
                            val is None
                            or val == ""
                            or val in input_ignore_vals
                        ):
                            val = 0
                        else:
                            hasvalues = True
                        string = string.replace(valcol, str(val))
                    string = string.replace(arbitrary_string, "#population")
                    return string, hasvalues

                for i, process_col in enumerate(process_cols):
                    valdict0 = valdicts[0]
                    for adm in valdict0:
                        hasvalues = True
                        matches = regex.search(
                            self.brackets, process_col, flags=regex.VERBOSE
                        )
                        if matches:
                            for bracketed_str in matches.captures("rec"):
                                if any(bracketed_str in x for x in valcols):
                                    continue
                                _, hasvalues_t = text_replacement(
                                    bracketed_str, adm
                                )
                                if not hasvalues_t:
                                    hasvalues = False
                                    break
                        if hasvalues:
                            formula, hasvalues_t = text_replacement(
                                process_col, adm
                            )
                            if hasvalues_t:
                                formula = formula.replace(
                                    "#population",
                                    "self.population_lookup[adm]",
                                )
                                newvaldicts[i][adm] = eval(formula)
                            else:
                                newvaldicts[i][adm] = ""
                        else:
                            newvaldicts[i][adm] = ""
                retvalues.extend(newvaldicts)
            elif sum_cols:
                for sum_col in sum_cols:
                    formula = sum_col["formula"]
                    mustbepopulated = sum_col.get("mustbepopulated", False)
                    newvaldicts = [dict() for _ in valdicts]
                    valdict0 = valdicts[0]
                    for adm in valdict0:
                        for i, val in enumerate(valdict0[adm]):
                            if not val or val in input_ignore_vals:
                                exists = False
                            else:
                                exists = True
                                for valdict in valdicts[1:]:
                                    val = valdict[adm][i]
                                    if (
                                        val is None
                                        or val == ""
                                        or val in input_ignore_vals
                                    ):
                                        exists = False
                                        break
                            if mustbepopulated and not exists:
                                continue
                            for j, valdict in enumerate(valdicts):
                                val = valdict[adm][i]
                                if (
                                    val is None
                                    or val == ""
                                    or val in input_ignore_vals
                                ):
                                    continue
                                newvaldicts[j][adm] = eval(
                                    f"newvaldicts[j].get(adm, 0.0) + {str(valdict[adm][i])}"
                                )
                    formula = formula.replace("#population", "#pzbgvjh")
                    for i in sorted_len_indices:
                        formula = formula.replace(
                            valcols[i], f"newvaldicts[{i}][adm]"
                        )
                    formula = formula.replace(
                        "#pzbgvjh", "self.population_lookup[adm]"
                    )
                    newvaldict = dict()
                    for adm in valdicts[0].keys():
                        try:
                            val = eval(formula)
                        except (ValueError, TypeError, KeyError):
                            val = ""
                        newvaldict[adm] = val
                    retvalues.append(newvaldict)
            else:
                retvalues.extend(valdicts)
        return retvalues

    def run(
        self,
        **kwargs: Any,
    ) -> Dict:
        """Runs one mini scraper described in dataset information and returns a
        dictionary containing the headers, values, source information and whether
        fallback data was used.

        Args:
            **kwargs: Variables to use when evaluating template arguments in urls

        Returns:
            Dict: Dictionary of output headers, values, source and whether fallback used
        """
        results = dict()
        name = self.datasetinfo["name"]
        try:
            headers, iterator = read(
                self.downloader, self.datasetinfo, today=self.today, **kwargs
            )
            if "source_url" not in self.datasetinfo:
                self.datasetinfo["source_url"] = self.datasetinfo["url"]
            date = self.datasetinfo.get("date")
            if date:
                if isinstance(date, str):
                    self.datasetinfo["date"] = parse_date(date)
            if not date or self.datasetinfo.get("force_date_today", False):
                self.datasetinfo["date"] = self.today
            header_to_hxltag = self.use_hxl(headers, iterator)
            self.rowparser = RowParser(
                self.countryiso3s,
                self.adminone,
                self.level,
                self.datelevel,
                self.datasetinfo,
                headers,
                header_to_hxltag,
                self.subsets,
            )
            values = self.run_scraper(iterator)
            sources = self.get_sources()
            fallback = False
            logger.info(f"Processed {name}")
        except Exception:
            if not self.fallbacks:
                raise
            if not self.headers[1]:
                raise
            values, sources = use_fallbacks(self.fallbacks, self.headers)
            fallback = True
            logger.exception(f"Used fallback data for {name}!")
        results["headers"] = self.headers
        results["values"] = values
        results["sources"] = sources
        results["fallbacks"] = fallback
        return results
