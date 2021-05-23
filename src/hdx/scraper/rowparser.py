# -*- coding: utf-8 -*-
import copy
import logging
from datetime import datetime
from operator import itemgetter
from typing import List, Dict, Tuple, Iterator, Union, Generator, Optional

import hxl
from hdx.location.adminone import AdminOne
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

from hdx.scraper import match_template

logger = logging.getLogger(__name__)


class RowParser(object):
    """RowParser class for parsing each row.

    Args:
        countryiso3s (List[str]): List of ISO3 country codes to process
        adminone (AdminOne): AdminOne object from HDX Python Country library that handles processing of admin level 1
        level (str): Can be global, national or subnational
        today (datetime): Value to use for today. Defaults to None (datetime.now()).
        datasetinfo (Dict): Dictionary of information about dataset
        headers (List[str]): Row headers
        subsets (List[Dict]): List of subset definitions
        maxdateonly (bool): Whether to only take the most recent date. Defaults to True.
    """

    def __init__(self, countryiso3s, adminone, level, today, datasetinfo, headers, subsets, maxdateonly=True):
        # type: (List[str], AdminOne, str, datetime, Dict, List[str], List[Dict], bool) -> None

        def get_level(lvl):
            if isinstance(lvl, str):
                if lvl == 'global':
                    return None
                elif lvl == 'national':
                    return 0
                else:
                    return 1
            return lvl

        self.level = get_level(level)
        self.today = today
        self.sort = datasetinfo.get('sort')
        self.datecol = datasetinfo.get('date_col')
        self.datetype = datasetinfo.get('date_type')
        if self.datetype:
            if self.datetype == 'date':
                date = parse_date('1900-01-01')
            else:
                date = 0
        else:
            date = 0
        self.maxdate = date
        datelevel = datasetinfo.get('date_level')
        if datelevel is None:
            self.datelevel = self.level
        else:
            self.datelevel = get_level(datelevel)
        date_condition = datasetinfo.get('date_condition')
        if date_condition is not None:
            for col in datasetinfo['input_cols']:
                date_condition = date_condition.replace(col, f"row['{col}']")
        self.date_condition = date_condition
        self.single_maxdate = datasetinfo.get('single_maxdate', False)
        self.ignore_future_date = datasetinfo.get('ignore_future_date', True)
        self.adminone = adminone
        self.admcols = datasetinfo.get('adm_cols', list())
        self.admexact = datasetinfo.get('adm_exact', False)
        self.subsets = subsets
        adms = datasetinfo.get('adm_vals')
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
                raise ValueError('No admin columns specified for required level!')
            self.maxdates = {i: {adm: date for adm in self.adms[self.datelevel]} for i, _ in enumerate(subsets)}

        self.maxdateonly = maxdateonly
        self.flatteninfo = datasetinfo.get('flatten')
        self.headers = headers
        self.filters = dict()
        self.read_external_filter(datasetinfo)

    def sort_rows(self, iterator, hxlrow):
        # type: (Iterator[Dict], Dict) -> Iterator[Dict]
        """Sort the input data before processing. If date_col is specified along with any of sum_cols, process_cols or
        append_cols, and sorting is not specified, then apply a sort by date to ensure correct results.

        Args:
            hxlrow (Dict): Mapping from column header to HXL hashtag
            iterator (Iterator[Dict]): Input data
        Returns:
            Iterator[Dict]: Input data sorted if specified or deemed necessary
        """
        if not self.sort:
            if self.datecol:
                for subset in self.subsets:
                    apply_sort = subset.get('sum_cols', subset.get('process_cols', subset.get('input_append')))
                    if apply_sort:
                        logger.warning('sum_cols, process_cols or input_append used without sorting. Applying sort by date to ensure correct results!')
                        self.sort = {'keys': [self.datecol], 'reverse': True}
                        break
        if self.sort:
            keys = self.sort['keys']
            reverse = self.sort.get('reverse', False)
            if hxlrow:
                headerrow = {v: k for k, v in hxlrow.items()}
                keys = [headerrow[key] for key in keys]
            iterator = sorted(list(iterator), key=itemgetter(*keys), reverse=reverse)
        return iterator

    def read_external_filter(self, datasetinfo):
        # type: (Dict) -> Tuple[List[str],Iterator[Union[List,Dict]]]
        """Read filter list from external url poitning to a HXLated file

        Args:
            datasetinfo (Dict): Dictionary of information about dataset

        Returns:
            None
        """
        external_filter = datasetinfo.get('external_filter')
        if not external_filter:
            return
        hxltags = external_filter['hxltags']
        data = hxl.data(external_filter['url'])
        use_hxl = datasetinfo.get('use_hxl', False)
        for row in data:
            for hxltag in data.columns:
                if hxltag.display_tag in hxltags:
                    if use_hxl:
                        header = hxltag.display_tag
                    else:
                        header = hxltag.header
                    dict_of_lists_add(self.filters, header, row.get('#country+code'))

    def flatten(self, row):
        # type: (Dict) -> Generator[Dict]
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
                colname = flatten['original']
                template_string, replace_string = match_template(colname)
                if not template_string:
                    raise ValueError('Column name for flattening lacks an incrementing number!')
                if counters[i] == -1:
                    counters[i] = int(replace_string)
                else:
                    replace_string = '%d' % counters[i]
                colname = colname.replace(template_string, replace_string)
                if colname not in row:
                    return
                newrow[flatten['new']] = row[colname]
                extracol = flatten.get('extracol')
                if extracol:
                    newrow[extracol] = colname
                counters[i] += 1
            yield newrow

    def get_maxdate(self):
        # type: () -> datetime
        """Get the most recent date of the rows so far

        Returns:
            datetime: Most recent date in processed rows
        """
        return self.maxdate

    def filtered(self, row):
        # type: (Dict) -> bool
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

    def parse(self, row, scrapername=None):
        # type: (Dict, str) -> Tuple[Optional[str], Optional[List[bool]]]
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
            if template_string:
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
                    adms[i], exact = self.adminone.get_pcode(adms[0], adm, scrapername)
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
            filter = subset['filter']
            process = True
            if filter:
                filters = filter.split('|')
                for filterstr in filters:
                    filter = filterstr.split('=')
                    if row[filter[0]] != filter[1]:
                        process = False
                        break
            should_process_subset.append(process)

        if self.datecol:
            if isinstance(self.datecol, list):
                dates = [str(row[x]) for x in self.datecol]
                date = ''.join(dates)
            else:
                date = row[self.datecol]
            if self.datetype == 'date':
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
                if date > self.today and self.ignore_future_date:
                   return None, None
            elif self.datetype == 'year':
                date = int(date)
                if date > self.today.year and self.ignore_future_date:
                   return None, None
            else:
                date = int(date)
            if self.date_condition:
                if eval(self.date_condition) is False:
                    return None, None
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
            return 'global', should_process_subset
        return adms[self.level], should_process_subset
