import logging
from datetime import datetime
from typing import Dict

from hdx.utilities.dateparse import parse_date

from ..base_scraper import BaseScraper
from ..outputs.base import BaseOutput

logger = logging.getLogger(__name__)


class TimeSeries(BaseScraper):
    """Each time series scraper is configured from dataset information that can come
    from a YAML file for example. When run, it populates the given outputs with
    time series data. It also overrides add_sources where sources are compiled and
    returned.

    Args:
        name (str): Name of scraper
        datasetinfo (Dict): Information about dataset
        today (datetime): Value to use for today. Defaults to datetime.now().
        outputs (Dict[str, BaseOutput]): Mapping from names to output objects
    """

    def __init__(
        self,
        name: str,
        datasetinfo: Dict,
        today,
        outputs: Dict[str, BaseOutput],
    ):
        # Time series only outputs to separate tabs
        super().__init__(f"timeseries_{name}", datasetinfo, dict())
        self.today = today
        self.outputs = outputs

    def run(self) -> None:
        """Runs one time series scraper given dataset information and outputs to
        whatever outputs were specified in the constructor

        Returns:
            None
        """
        input = self.datasetinfo["input"]
        datecol = self.datasetinfo["date"]
        datetype = self.datasetinfo["date_type"]
        ignore_future_date = self.datasetinfo.get("ignore_future_date", True)
        headers = [datecol] + self.datasetinfo["output"]
        hxltags = [self.datasetinfo["date_hxl"]] + self.datasetinfo[
            "output_hxl"
        ]
        rows = [headers, hxltags]
        file_headers, iterator = self.get_reader().read(self.datasetinfo)
        for inrow in iterator:
            if isinstance(datecol, list):
                dates = [str(inrow[x]) for x in datecol]
                date = "".join(dates)
            else:
                date = inrow[datecol]
            if datetype == "date":
                if not isinstance(date, datetime):
                    date = parse_date(date)
                date = date.replace(tzinfo=None)
                if date > self.today and ignore_future_date:
                    continue
                date = date.strftime("%Y-%m-%d")
            elif datetype == "year":
                date = int(date)
                if date > self.today.year and ignore_future_date:
                    continue
                date = str(date)
            row = [date]
            for column in input:
                row.append(inrow[column])
            rows.append(row)
        for output in self.outputs.values():
            output.update_tab(self.name, rows)

    def add_sources(self) -> None:
        """Add source for each HXL hashtag

        Returns:
            None
        """
        for hxltag in self.datasetinfo["output_hxl"]:
            self.add_hxltag_source(self.name, hxltag)
