import logging
from os.path import join
from typing import Any, Dict, List, Optional, Union

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.saver import save_json

from .base import BaseOutput

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


from ..utilities import match_template
from ..utilities.reader import Read

logger = logging.getLogger(__name__)


class JsonFile(BaseOutput):
    """JsonFile class enabling writing to JSON files.

    Args:
        configuration (Dict): Configuration for Google Sheets
        updatetabs (List[str]): Tabs to update
        suffix (str): A suffix to add to keys. Default is _data.
    """

    def __init__(self, configuration, updatetabs, suffix="_data"):
        super().__init__(updatetabs)
        self.configuration = configuration
        self.json = dict()
        self.suffix = suffix

    def add_data_row(self, key: str, row: Dict) -> None:
        """Add row to JSON under a key

        Args:
            key (str): Key in JSON to update
            rows (List[Dict]): List of dictionaries

        Returns:
            None
        """
        dict_of_lists_add(self.json, f"{key}{self.suffix}", row)

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
        if hxltags:
            df = df.rename(columns=hxltags)
        self.json[f"{key}{self.suffix}"] = df.to_dict(orient="records")

    def add_data_rows_by_key(
        self,
        key: str,
        countryiso: str,
        rows: List[Dict],
        hxltags: Optional[Dict] = None,
    ) -> None:
        """Add rows under both a key and an ISO 3 country code subkey

        Args:
            key (str): Key in JSON to update
            countryiso (str): Country to use as subkey
            rows (List[Dict]): List of dictionaries
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        fullname = f"{key}{self.suffix}"
        jsondict = self.json.get(fullname, dict())
        jsondict[countryiso] = list()
        for row in rows:
            if hxltags:
                newrow = dict()
                for header, hxltag in hxltags.items():
                    newrow[hxltag] = row[header]
            else:
                newrow = row
            jsondict[countryiso].append(newrow)
        self.json[fullname] = jsondict

    def generate_json_from_list(self, key: str, rows: List[Dict]) -> None:
        """Generate JSON from key and rows list

        Args:
            key (str): Key in JSON to update
            rows (List[Dict]): List of dictionaries

        Returns:
            None
        """
        hxltags = rows[1]
        for row in rows[2:]:
            newrow = dict()
            for i, hxltag in enumerate(hxltags):
                value = row[i]
                if value in [None, ""]:
                    continue
                newrow[hxltag] = str(value)
            self.add_data_row(key, newrow)

    def generate_json_from_df(
        self, key: str, df: DataFrame, hxltags: Optional[Dict]
    ) -> None:
        """Generate JSON from key and dataframe

        Args:
            key (str): Key in JSON to update
            df (DataFrame): Dataframe containing rows
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        for i, row in df.iterrows():
            newrow = dict()
            row = row.to_dict()
            for i, hxltag in enumerate(hxltags):
                value = row.get(hxltag)
                if value in [None, ""]:
                    value = None
                newrow[hxltags.get(hxltag)] = str(value)
            self.add_data_row(key, newrow)

    def update_tab(
        self,
        tabname: str,
        values: Union[List, DataFrame],
        hxltags: Optional[Dict] = None,
    ) -> None:
        """Update tab with values

        Args:
            tabname (str): Tab to update
            values (Union[List, DataFrame]): Values in a list of lists or a DataFrame
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        if tabname not in self.updatetabs:
            return
        if isinstance(values, list):
            self.generate_json_from_list(tabname, values)
        else:
            # isinstance(values, DataFrame)
            self.generate_json_from_df(tabname, values, hxltags)

    def add_additional(self) -> None:
        """Download JSON files and add them under keys defined in the configuration

        Returns:
            None
        """
        reader = Read.get_reader()
        for datasetinfo in self.configuration.get("additional_inputs", list()):
            headers, iterator = reader.read(datasetinfo)
            hxl_row = next(iterator)
            if not isinstance(hxl_row, dict):
                hxl_row = hxl_row.value
            name = datasetinfo["name"]
            for row in iterator:
                newrow = dict()
                if not isinstance(row, dict):
                    row = row.value
                for key in row:
                    hxltag = hxl_row[key]
                    if hxltag != "":
                        newrow[hxl_row[key]] = row[key]
                self.add_data_row(name, newrow)

    def save(self, folder: Optional[str] = None, **kwargs: Any) -> List[str]:
        """Save JSON file and any addition subsets of that JSON defined in the additional configuration

        Args:
            folder (Optional[str]): Folder to save to. Defaults to None.
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            List[str]: List of file paths
        """
        filepaths = list()
        filepath = self.configuration["output"]
        if folder:
            filepath = join(folder, filepath)
        logger.info(f"Writing JSON to {filepath}")
        save_json(self.json, filepath)
        filepaths.append(filepath)
        for kwarg in kwargs:
            exec(f"{kwarg}={kwargs[kwarg]}")
        additional = self.configuration.get("additional_outputs", list())
        for filedetails in additional:
            json = dict()
            remove = filedetails.get("remove")
            if remove is None:
                tabs = filedetails["tabs"]
            else:
                tabs = list()
                for key in self.json.keys():
                    tab = key.replace(f"{self.suffix}", "")
                    if tab not in remove:
                        tabs.append({"tab": tab})
            for tabdetails in tabs:
                key = f'{tabdetails["tab"]}{self.suffix}'
                newjson = self.json.get(key)
                filters = tabdetails.get("filters", dict())
                hxltags = tabdetails.get("output")
                if (filters or hxltags or remove) and isinstance(
                    newjson, list
                ):
                    rows = list()
                    for row in newjson:
                        ignore_row = False
                        for filter, allowed_values in filters.items():
                            value = row.get(filter)
                            if value:
                                if isinstance(allowed_values, str):
                                    (
                                        template_string,
                                        match_string,
                                    ) = match_template(allowed_values)
                                    if template_string:
                                        allowed_values = eval(
                                            allowed_values.replace(
                                                template_string, match_string
                                            )
                                        )
                                if isinstance(allowed_values, list):
                                    if value not in allowed_values:
                                        ignore_row = True
                                        break
                                elif value != allowed_values:
                                    ignore_row = True
                                    break
                        if ignore_row:
                            continue
                        if hxltags is None:
                            newrow = row
                        else:
                            newrow = dict()
                            for hxltag in hxltags:
                                if hxltag in row:
                                    newrow[hxltag] = row[hxltag]
                        rows.append(newrow)
                    newjson = rows
                newkey = tabdetails.get("key", key)
                json[newkey] = newjson
            if not json:
                continue
            filedetailspath = filedetails["filepath"]
            if folder:
                filedetailspath = join(folder, filedetailspath)
            logger.info(f"Writing JSON to {filedetailspath}")
            save_json(json, filedetailspath)
            filepaths.append(filedetailspath)
        return filepaths
