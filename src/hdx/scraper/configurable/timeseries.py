import logging
from datetime import datetime

from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class TimeSeries(BaseScraper):
    def __init__(self, name, datasetinfo, today, outputs):
        # Time series only outputs to separate tabs
        super().__init__(f"timeseries_{name}", datasetinfo, dict())
        self.today = today
        self.outputs = outputs

    def run(self):
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

    def add_sources(self):
        for hxltag in self.datasetinfo["output_hxl"]:
            self.add_hxltag_source(self.name, hxltag)

    @classmethod
    def get_scrapers(cls, configuration, today, outputs):
        scrapers = list()
        for name, datasetinfo in configuration.items():
            scrapers.append(cls(name, datasetinfo, today, outputs))
        return scrapers
