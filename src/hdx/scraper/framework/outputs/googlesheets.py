import json
import logging

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
        configuration: Configuration for Google Sheets
        gsheet_auth: Authorisation for Google Sheets/Drive
        updatesheets: List of spreadsheets to update (eg. prod, test)
        tabs: Dictionary of mappings from internal name to spreadsheet tab name
        updatetabs: Tabs to update
    """

    def __init__(
        self,
        configuration: dict,
        gsheet_auth: str,
        updatesheets: list[str],
        tabs: dict[str, str],
        updatetabs: list[str],
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
        values: list | DataFrame,
        hxltags: dict | None = None,
        limit: int | None = None,
    ) -> None:
        """Update tab with values

        Args:
            tabname: Tab to update
            values: Values in a list of lists or a DataFrame
            hxltags: HXL tag mapping. Default is None.
            limit: Maximum number of rows to output

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
                    rows.append([hxltags.get(header, "") for header in headers])
                if limit is not None:
                    values = values.head(limit)
                df = values.copy(deep=True)
                df.replace(numpy.inf, "inf", inplace=True)
                df.replace(-numpy.inf, "-inf", inplace=True)
                df.fillna("NaN", inplace=True)
                rows.extend(df.values.tolist())
                values = rows
            tab.update(values, "A1")
