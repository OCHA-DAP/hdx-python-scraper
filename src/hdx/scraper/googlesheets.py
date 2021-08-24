# -*- coding: utf-8 -*-
import json
import logging
from typing import Dict, List, Union, Optional

import gspread

try:
    import numpy
    from pandas import DataFrame
except ImportError:
    pass


logger = logging.getLogger(__name__)


class GoogleSheets:
    """GoogleSheets class enabling writing to Google spreadsheets.

    Args:
        configuration (Dict): Configuration for Google Sheets
        gsheet_auth (str): Authorisation for Google Sheets/Drive
        updatesheets (List[str]): List of spreadsheets to update (eg. prod, test)
        tabs (Dict[str, str]): Dictionary of mappings from internal name to spreadsheet tab name
        updatetabs (List[str]): Tabs to update
    """

    def __init__(self, configuration, gsheet_auth, updatesheets, tabs, updatetabs):
        # type: (Dict, str, List[str], Dict[str, str], List[str]) -> None
        info = json.loads(gsheet_auth)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.gc = gspread.service_account_from_dict(info, scopes=scopes)
        self.googlesheets = configuration['googlesheets']
        if updatesheets is None:
            updatesheets = self.googlesheets.keys()
            logger.info('Updating all spreadsheets')
        else:
            logger.info('Updating only these spreadsheets: %s' % updatesheets)
        self.updatesheets = updatesheets
        self.tabs = tabs
        self.updatetabs = updatetabs

    def update_tab(self, tabname, values, hxltags=None, limit=None):
        # type: (str, Union[List, DataFrame], Optional[Dict], Optional[int]) -> None
        """Update tab with values

        Args:
            tabname (str): Tab to update
            values (Union[List, DataFrame]): Either values in a list of dicts or a DataFrame
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.
            limit (Optional[int]): Maximum number of rows to output

        Returns:
            None
        """
        if tabname not in self.updatetabs:
            return
        for sheet in self.googlesheets:
            if sheet not in self.updatesheets:
                continue
            url = self.googlesheets[sheet]
            spreadsheet = self.gc.open_by_url(url)

            tab = spreadsheet.worksheet(self.tabs[tabname])
            tab.clear()
            if not isinstance(values, list):
                headers = list(values.columns.values)
                rows = [headers]
                if hxltags:
                    rows.append([hxltags.get(header, '') for header in headers])
                if limit is not None:
                    values = values.head(limit)
                df = values.copy(deep=True)
                df.replace(numpy.inf, 'inf', inplace=True)
                df.replace(-numpy.inf, '-inf', inplace=True)
                df.fillna('NaN', inplace=True)
                rows.extend(df.values.tolist())
                values = rows
            tab.update('A1', values)
