import json
import logging
from typing import Dict, List, Optional, Union

import gspread

from .base import BaseOutput

try:
    import numpy
    from pandas import DataFrame
except ImportError:
    DataFrame = None


logger = logging.getLogger(__name__)


class GoogleSheets(BaseOutput):
    """GoogleSheets class enabling writing to Google spreadsheets.

    Args:
        configuration (Dict): Configuration for Google Sheets
        gsheet_auth (str): Authorisation for Google Sheets/Drive
        updatesheets (List[str]): List of spreadsheets to update (eg. prod, test)
        tabs (Dict[str, str]): Dictionary of mappings from internal name to spreadsheet tab name
        updatetabs (List[str]): Tabs to update
    """

    def __init__(
        self,
        configuration: Dict,
        gsheet_auth: str,
        updatesheets: List[str],
        tabs: Dict[str, str],
        updatetabs: List[str],
    ) -> None:
        super().__init__(updatetabs)
        info = json.loads(gsheet_auth)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.gc = gspread.service_account_from_dict(info, scopes=scopes)
        self.configuration = configuration
        if updatesheets is None:
            updatesheets = self.configuration.keys()
            logger.info("Updating all spreadsheets")
        else:
            logger.info(f"Updating only these spreadsheets: {updatesheets}")
        self.updatesheets = updatesheets
        self.tabs = tabs

    def update_tab(
        self,
        tabname: str,
        values: Union[List, DataFrame],
        hxltags: Optional[Dict] = None,
        limit: Optional[int] = None,
    ) -> None:
        """Update tab with values

        Args:
            tabname (str): Tab to update
            values (Union[List, DataFrame]): Values in a list of lists or a DataFrame
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.
            limit (Optional[int]): Maximum number of rows to output

        Returns:
            None
        """
        if tabname not in self.updatetabs:
            return
        for sheet in self.configuration:
            if sheet not in self.updatesheets:
                continue
            url = self.configuration[sheet]
            spreadsheet = self.gc.open_by_url(url)

            tab = spreadsheet.worksheet(self.tabs[tabname])
            tab.clear()
            if not isinstance(values, list):
                headers = list(values.columns.values)
                rows = [headers]
                if hxltags:
                    rows.append(
                        [hxltags.get(header, "") for header in headers]
                    )
                if limit is not None:
                    values = values.head(limit)
                df = values.copy(deep=True)
                df.replace(numpy.inf, "inf", inplace=True)
                df.replace(-numpy.inf, "-inf", inplace=True)
                df.fillna("NaN", inplace=True)
                rows.extend(df.values.tolist())
                values = rows
            tab.update("A1", values)
