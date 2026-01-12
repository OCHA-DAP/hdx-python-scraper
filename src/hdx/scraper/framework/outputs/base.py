from typing import Any

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class BaseOutput:
    """Base class for output that can also be used for testing as it does nothing.

    Args:
        updatetabs: Tabs to update
    """

    def __init__(self, updatetabs: list[str]) -> None:
        self.updatetabs = updatetabs

    def update_tab(
        self,
        tabname: str,
        values: list | DataFrame,
        hxltags: dict | None = None,
        **kwargs: Any,
    ) -> None:
        """Update tab with values. Classes that inherit from this one should
        implement this method.

        Args:
            tabname: Tab to update
            values: Values in a list of lists or a DataFrame
            hxltags: HXL tag mapping. Default is None.
            **kwargs (Any): Keyword arguments

        Returns:
            None
        """
        return

    def add_data_row(self, key: str, row: dict) -> None:
        """Add row

        Args:
            key: Key to update
            row: Row to add

        Returns:
            None
        """
        return

    def add_dataframe_rows(
        self, key: str, df: DataFrame, hxltags: dict | None = None
    ) -> None:
        """Add rows from dataframe under a key

        Args:
            key: Key in JSON to update
            df: Dataframe containing rows
            hxltags: HXL tag mapping. Default is None.

        Returns:
            None
        """
        return

    def add_data_rows_by_key(
        self,
        name: str,
        countryiso: str,
        rows: list[dict],
        hxltags: dict | None = None,
    ) -> None:
        """Add rows under both a key and an ISO 3 country code subkey

        Args:
            key: Key to update
            countryiso: Country to use as subkey
            rows: List of dictionaries
            hxltags: HXL tag mapping. Default is None.

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
