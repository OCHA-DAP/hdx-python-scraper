import logging
from os.path import join
from shutil import move

from slugify import slugify

from hdx.scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class FileCopier(BaseScraper):
    def __init__(self, datasetinfo, folder):
        # FileCopier only outputs to sources
        name = f"filecopier_{slugify(datasetinfo['hxltag'].lower(), separator='_')}"
        super().__init__(name, datasetinfo, dict())
        self.folder = folder

    def run(self):
        resource = self.get_reader().read_hdx_metadata(self.datasetinfo)
        url, path = resource.download()
        logger.info(f"Downloading {url} to {path}")
        move(path, join(self.folder, self.datasetinfo["filename"]))

    def add_sources(self):
        self.add_hxltag_source("FileCopier", self.datasetinfo["hxltag"])

    @classmethod
    def get_scrapers(cls, configuration, folder=""):
        scrapers = list()
        for datasetinfo in configuration:
            scrapers.append(cls(datasetinfo, folder))
        return scrapers
