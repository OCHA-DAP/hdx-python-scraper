from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

from hdx.scraper.utilities.reader import Read


class BaseScraper(ABC):
    """Base scraper class for scrapers to inherit

    Args:
        name (str): Name of scraper
        datasetinfo (Dict): Information about dataset
        headers: Dict[str, Tuple]: Headers to be oytput at each level_name
    """

    population_lookup = dict()

    def __init__(
        self, name: str, datasetinfo: Dict, headers: Dict[str, Tuple]
    ):
        """
        Create values member variable for inheriting scrapers to populate. It is of
        form: {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...)}}
        It should be called last from the inheriting scraper's constructor.
        """
        self.setup(name, headers)
        self.datasetinfo = deepcopy(datasetinfo)
        self.errors_on_exit = None
        self.can_fallback = True

    def setup(self, name: str, headers: Dict[str, Tuple]):
        self.name = name
        self.headers = headers
        self.initialise_values_sources()
        self.has_run = False
        self.fallbacks_used = False
        self.source_urls = set()

    def get_reader(
        self, name: Optional[str] = None, prefix: Optional[str] = None
    ):
        if not name:
            name = self.name
        reader = Read.get_reader(name)
        if not prefix:
            prefix = name
        reader.prefix = prefix
        return reader

    def initialise_values_sources(self):
        self.values: Dict[str, Tuple] = {
            level: tuple(dict() for _ in value[0])
            for level, value in self.headers.items()
        }
        self.sources: Dict[str, List] = {
            level: list() for level in self.headers
        }

    def get_headers(self, level: str) -> Optional[Tuple[Tuple]]:
        """
        Get headers for a particular level_name like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Optional[Tuple[Tuple]]: Scraper headers or None
        """
        return self.headers.get(level)

    def get_values(self, level: str) -> Optional[Tuple]:
        """
        Get values for a particular level_name like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Optional[Tuple]: Scraper values or None
        """
        return self.values.get(level)

    def add_sources(self):
        """
        Adds sources for a particular level_name

        Returns:
            List[Tuple]: List of (hxltag, date, source, source url)

        """
        source = self.datasetinfo["source"]
        if isinstance(source, str):
            source = {"default_source": source}
        source_url = self.datasetinfo["source_url"]
        if isinstance(source_url, str):
            source_url = {"default_url": source_url}

        date = self.datasetinfo["source_date"].strftime("%Y-%m-%d")

        for level in self.headers:
            self.sources[level] = [
                (
                    hxltag,
                    date,
                    source.get(hxltag, source["default_source"]),
                    source_url.get(hxltag, source_url["default_url"]),
                )
                for hxltag in self.headers[level][1]
            ]

    def add_hxltag_source(self, tab: str, hxltag: str) -> None:
        self.sources[tab] = [
            (
                hxltag,
                self.datasetinfo["source_date"].strftime("%Y-%m-%d"),
                self.datasetinfo["source"],
                self.datasetinfo["source_url"],
            )
        ]

    def get_sources(self, level):
        return self.sources.get(level)

    def add_source_urls(self) -> None:
        source_url = self.datasetinfo.get("source_url")
        if source_url:
            if isinstance(source_url, str):
                self.source_urls.add(source_url)
            else:
                for url in source_url.values():
                    self.source_urls.add(url)

    def get_source_urls(self):
        return self.source_urls

    def add_population(self):
        for level in self.headers:
            try:
                population_index = self.headers[level][1].index("#population")
            except ValueError:
                population_index = None
            if population_index is not None:
                for key, value in self.values[level][population_index].items():
                    try:
                        valint = int(value)
                        self.population_lookup[key] = valint
                    except ValueError:
                        pass

    @abstractmethod
    def run(self) -> None:
        """
        Run scraper

        Returns:
            None
        """

    def run_after_fallbacks(self) -> None:
        """
        Executed after fallbacks are used

        Returns:
            None
        """
        pass

    def post_run(self) -> None:
        """
        Executed after running

        Returns:
            None
        """
        pass
