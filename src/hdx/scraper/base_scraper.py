from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Dict, List, Tuple


class BaseScraper(ABC):
    """Base scraper class for scrapers to inherit

    Args:
        name (str): Name of scraper
        datasetinfo (Dict): Information about dataset
        headers: Dict[str, Tuple]: Headers to be oytput at each level
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
        self.name = name
        self.datasetinfo = deepcopy(datasetinfo)
        self.headers = headers
        self.initialise_values_sources()
        self.has_run = False
        self.fallbacks_used = False

    def initialise_values_sources(self):
        self.values: Dict[str, Tuple] = {
            level: tuple(dict() for _ in value[0])
            for level, value in self.headers.items()
        }
        self.sources: Dict[str, List] = {
            level: list() for level in self.headers
        }

    def get_headers(self, level: str) -> Tuple[Tuple]:
        """
        Get headers for a particular level like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Tuple[Tuple]: Scraper headers
        """
        return self.headers[level]

    def get_values(self, level: str) -> Tuple:
        """
        Get values for a particular level like national or subnational

        Args:
            level (str): Level for which to get headers

        Returns:
            Tuple: Scraper values
        """
        return self.values[level]

    def add_sources(self):
        """
        Adds sources for a particular level

        Returns:
            List[Tuple]: List of (hxltag, date, source, source url)

        """
        source = self.datasetinfo["source"]
        if isinstance(source, str):
            source = {"default_source": source}
        source_url = self.datasetinfo["source_url"]
        if isinstance(source_url, str):
            source_url = {"default_url": source_url}

        date = self.datasetinfo["date"].strftime("%Y-%m-%d")

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

    def get_sources(self, level):
        return self.sources[level]

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
