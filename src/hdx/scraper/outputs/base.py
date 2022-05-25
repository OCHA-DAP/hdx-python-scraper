from typing import Any, Dict, List, Optional, Union

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class BaseOutput:
    """Base class for output that can also be used for testing as it does nothing.

    Args:
        updatetabs (List[str]): Tabs to update
    """

    def __init__(self, updatetabs: List[str]) -> None:
        self.updatetabs = updatetabs

    def update_tab(
        self,
        tabname: str,
        values: Union[List, DataFrame],
        hxltags: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        """Update tab with values. Classes that inherit from this one should
        implement this method.

        Args:
            tabname (str): Tab to update
            values (Union[List, DataFrame]): Values in a list of lists or a DataFrame
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.
            **kwargs

        Returns:
            None
        """
        return

    def add_data_row(self, key: str, row: Dict) -> None:
        """Add row

        Args:
            key (str): Key to update
            row (Dict): Row to add

        Returns:
            None
        """
        return

    def add_dataframe_rows(
        self, key: str, df: DataFrame, hxltags: Optional[Dict] = None
    ) -> None:
        """Add rows from dataframe under a key

        Args:
            key (str): Key in JSON to update
            df (DataFrame): Dataframe containing rows
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        return

    def add_data_rows_by_key(
        self,
        name: str,
        countryiso: str,
        rows: List[Dict],
        hxltags: Optional[Dict] = None,
    ) -> None:
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

    def add_additional(self) -> None:
        """Download files and add them under keys defined in the configuration

        Returns:
            None
        """
        return

    def save(self, **kwargs: Any) -> None:
        """Save file

        Args:
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            None
        """
        return
