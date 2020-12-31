# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from os.path import join
from typing import Union, List, Dict, Optional, Any

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.saver import save_json
from hdx.utilities.text import get_numeric_if_possible
from pandas import DataFrame

from hdx.scraper import match_template
from hdx.scraper.readers import read

logger = logging.getLogger(__name__)


class JsonOutput:
    """JsonOutput class enabling writing to JSON files.

    Args:
        configuration (Dict): Configuration for Google Sheets
        updatetabs (List[str]): Tabs to update
    """

    def __init__(self, configuration, updatetabs):
        self.json_configuration = configuration['json']
        self.updatetabs = updatetabs
        self.json = dict()

    def add_data_row(self, key, row):
        # type: (str, Dict) -> None
        """Add row to JSON under a key

        Args:
            key (str): Key in JSON to update
            rows (List[Dict]): List of dictionaries

        Returns:
            None
        """
        dict_of_lists_add(self.json, '%s_data' % key, row)

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
        if hxltags:
            df = df.rename(columns=hxltags)
        self.json['%s_data' % key] = df.to_dict(orient='records')

    def add_data_rows_by_key(self, key, countryiso, rows, hxltags=None):
        # type: (str, str, List[Dict], Optional[Dict]) -> None
        """Add rows under both a key and an ISO 3 country code subkey

        Args:
            key (str): Key in JSON to update
            countryiso (str): Country to use as subkey
            rows (List[Dict]): List of dictionaries
            hxltags (Optional[Dict]): HXL tag mapping. Defaults to None.

        Returns:
            None
        """
        fullname = '%s_data' % key
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

    def generate_json_from_list(self, key, rows):
        # type: (str, List[Dict]) -> None
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
                if value in [None, '']:
                    continue
                newrow[hxltag] = str(value)
            self.add_data_row(key, newrow)

    def generate_json_from_df(self, key, df, hxltags):
        # type: (str, DataFrame, Optional[Dict]) -> None
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
                if value in [None, '']:
                    value = None
                newrow[hxltags.get(hxltag)] = value
            self.add_data_row(key, newrow)

    def update_tab(self, tabname, values, hxltags=None):
        # type: (str, Union[List, DataFrame], Optional[Dict]) -> None
        """Update tab with values

        Args:
            tabname (str): Tab to update
            values (Union[List, DataFrame]): Either values in a list of dicts or a DataFrame
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

    def add_additional_json(self, downloader, today=None):
        # type: (Download, Optional[datetime]) -> None
        """Download JSON files and add them under keys defined in the configuration

        Args:
            downloader (Download): Download object for downloading JSON
            today (Optional[datetime]): Value to use for today. Defaults to None (datetime.now()).

        Returns:
            None
        """
        for datasetinfo in self.json_configuration.get('additional_json', list()):
            headers, iterator = read(downloader, datasetinfo, today=today)
            hxlrow = next(iterator)
            if not isinstance(hxlrow, dict):
                hxlrow = hxlrow.value
            name = datasetinfo['name']
            for row in iterator:
                newrow = dict()
                if not isinstance(row, dict):
                    row = row.value
                for key in row:
                    hxltag = hxlrow[key]
                    if hxltag != '':
                        newrow[hxlrow[key]] = row[key]
                self.add_data_row(name, newrow)

    def save(self, folder=None, **kwargs):
        # type: (str, Any) -> List[str]
        """Save JSON file and any addition subsets of that JSON defined in the additional configuration

        Args:
            folder (Optional[str]): Key in JSON to update
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            List[str]: List of file paths
        """
        filepaths = list()
        filepath = self.json_configuration['filepath']
        if folder:
            filepath = join(folder, filepath)
        logger.info('Writing JSON to %s' % filepath)
        save_json(self.json, filepath)
        filepaths.append(filepath)
        for kwarg in kwargs:
            exec('%s=%s' % (kwarg, kwargs[kwarg]))
        additional = self.json_configuration.get('additional', list())
        for filedetails in additional:
            json = dict()
            remove = filedetails.get('remove')
            if remove is None:
                tabs = filedetails['tabs']
            else:
                tabs = list()
                for key in self.json.keys():
                    tab = key.replace('_data', '')
                    if tab not in remove:
                        tabs.append({'tab': tab})
            for tabdetails in tabs:
                key = f'{tabdetails["tab"]}_data'
                newjson = self.json.get(key)
                filters = tabdetails.get('filters', dict())
                hxltags = tabdetails.get('hxltags')
                if (filters or hxltags or remove) and isinstance(newjson, list):
                    rows = list()
                    for row in newjson:
                        ignore_row = False
                        for filter, allowed_values in filters.items():
                            value = row.get(filter)
                            if value:
                                if isinstance(allowed_values, str):
                                    template_string, match_string = match_template(allowed_values)
                                    if template_string:
                                        allowed_values = eval(allowed_values.replace(template_string, match_string))
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
                newkey = tabdetails.get('key', key)
                json[newkey] = newjson
            if not json:
                continue
            filedetailspath = filedetails['filepath']
            if folder:
                filedetailspath = join(folder, filedetailspath)
            logger.info('Writing JSON to %s' % filedetailspath)
            save_json(json, filedetailspath)
            filepaths.append(filedetailspath)
        return filepaths
