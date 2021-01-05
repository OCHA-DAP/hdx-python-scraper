# -*- coding: utf-8 -*-
from datetime import datetime
from typing import List, Union, Optional, Dict, Any

from hdx.utilities.downloader import Download
from pandas import DataFrame


class NoOutput:
    """NoOutput class for testing that mimics the format of other output classes but does nothing.

    Args:
        updatetabs (List[str]): Tabs to update
    """

    def __init__(self, updatetabs):
        # type: (List[str]) -> None
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
        return

    def add_data_row(self, key, row):
        # type: (str, Dict) -> None
        """Add row

        Args:
            key (str): Key to update
            rows (List[Dict]): List of dictionaries

        Returns:
            None
        """
        return

    def add_dataframe_rows(self, key, df, hxltags=None):
        # type: (str, DataFrame, Optional[Dict]) -> None
        """Add rows from dataframe under a key

        Args:
            key (str): Key in JSON to update
            df (DataFrame): Dataframe containing rows
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        return

    def add_data_rows_by_key(self, name, countryiso, rows, hxltags=None):
        # type: (str, str, List[Dict], Optional[Dict]) -> None
        """Add rows under both a key and an ISO 3 country code subkey

        Args:
            key (str): Key to update
            countryiso (str): Country to use as subkey
            rows (List[Dict]): List of dictionaries
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        return

    def add_additional_json(self, downloader, today=None):
        # type: (Download, Optional[datetime]) -> None
        """Download files and add them under keys defined in the configuration

        Args:
            downloader (Download): Download object for downloading
            today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).

        Returns:
            None
        """
        return

    def save(self, **kwargs):
        # type: (Any) -> None
        """Save file

        Args:
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            None
        """
        return
