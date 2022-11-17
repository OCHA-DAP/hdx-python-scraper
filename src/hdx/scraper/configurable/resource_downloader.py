import logging
from os.path import join
from shutil import copy2

from slugify import slugify

from ..base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ResourceDownloader(BaseScraper):
    """Each resource downloader is configured from dataset information that can come
    from a YAML file for example. When run it downloads the resource described in the
    dataset information from HDX and puts it in the given folder.

    Args:
        datasetinfo (Dict): Information about dataset
        folder (str): Folder to which to download. Defaults to "".
    """

    def __init__(self, datasetinfo, folder):
        # ResourceDownloader only outputs to sources
        name = f"resource_downloader_{slugify(datasetinfo['hxltag'].lower(), separator='_')}"
        super().__init__(name, datasetinfo, dict())
        self.folder = folder

    def run(self) -> None:
        """Runs one resource downloader given dataset information

        Returns:
            None
        """
        reader = self.get_reader("hdx")
        resource = reader.read_hdx_metadata(self.datasetinfo)
        url, path = reader.download_resource(self.name, resource)
        logger.info(f"Downloading {url} to {path}")
        copy2(path, join(self.folder, self.datasetinfo["filename"]))

    def add_sources(self) -> None:
        """Add source for resource download

        Returns:
            None
        """
        self.add_hxltag_source(
            self.datasetinfo["hxltag"],
            key="ResourceDownloader",
        )
