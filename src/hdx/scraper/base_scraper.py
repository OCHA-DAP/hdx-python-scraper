from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Dict, List, Optional, Set, Tuple

from .utilities.reader import Read


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
    ) -> None:
        self.setup(name, headers)
        self.datasetinfo = deepcopy(datasetinfo)
        self.errors_on_exit = None
        self.can_fallback = True

    def setup(self, name: str, headers: Dict[str, Tuple]) -> None:
        """Initialise member variables including name and headers which is of form:
        {"national": (("School Closure",), ("#impact+type",)), ...},

        Args:
            name (str): Name of scraper
            headers (Dict[str, Tuple]): Headers to be output at each level_name

        Returns:
             None
        """
        self.name = name
        self.headers = headers
        self.initialise_values_sources()
        self.has_run = False
        self.fallbacks_used = False
        self.source_urls = set()
        self.population_key = None

    def initialise_values_sources(self) -> None:
        """
        Create values and sources member variables for inheriting scrapers to populate.
        values will be of form:
        {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}
        sources will be of form:
        {"national": [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

        Returns:
             None
        """
        self.values: Dict[str, Tuple] = {
            level: tuple(dict() for _ in value[0])
            for level, value in self.headers.items()
        }
        self.sources: Dict[str, List] = {
            level: list() for level in self.headers
        }

    def get_reader(
        self, name: Optional[str] = None, prefix: Optional[str] = None
    ):
        """Get reader given name if provided or using name member variable if not.
        Set reader prefix to given prefix or name if not provided.

        Args:
            name (str): Name of scraper
            prefix (Optional[str]): Prefix to use. Defaults to None (use scraper name).

        Returns:
             None
        """
        if not name:
            name = self.name
        reader = Read.get_reader(name)
        if not prefix:
            prefix = name
        reader.prefix = prefix
        return reader

    def get_headers(self, level: str) -> Optional[Tuple[Tuple]]:
        """
        Get headers for a particular level_name like national or subnational. Will be
        of form: (("School Closure",), ("#impact+type",))

        Args:
            level (str): Level to get like national, subnational or single

        Returns:
            Optional[Tuple[Tuple]]: Scraper headers or None
        """
        return self.headers.get(level)

    def get_values(self, level: str) -> Optional[Tuple]:
        """
        Get values for a particular level_name like national or subnational. Will be of
        form: ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}

        Args:
            level (str): Level for which to get headers

        Returns:
            Optional[Tuple]: Scraper values or None
        """
        return self.values.get(level)

    def add_sources(self) -> None:
        """
        Adds sources for a particular level_name

        Returns:
            None
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

    def add_hxltag_source(self, key: str, indicator: str) -> None:
        """
        Adds source under a particular key with a particular indicator expressed as a
        HXL hashtag.

        Args:
            key (str): Key under which to add source
            indicator (str): HXL hashtag to use for source

        Returns:
            None
        """
        self.sources[key] = [
            (
                indicator,
                self.datasetinfo["source_date"].strftime("%Y-%m-%d"),
                self.datasetinfo["source"],
                self.datasetinfo["source_url"],
            )
        ]

    def get_sources(self, level: str) -> Optional[List[Tuple]]:
        """
        Get values for a particular level_name like national or subnational. Will be of
        form:
        [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

        Args:
            level (str): Level to get like national, subnational or single

        Returns:
            Optional[List[Tuple]]: Scraper sources or None
        """
        return self.sources.get(level)

    def add_source_urls(self) -> None:
        """
        Add source urls from the datasetinfo member variable

        Returns:
            None
        """
        source_url = self.datasetinfo.get("source_url")
        if source_url:
            if isinstance(source_url, str):
                self.source_urls.add(source_url)
            else:
                for url in source_url.values():
                    self.source_urls.add(url)

    def get_source_urls(self) -> Set[str]:
        """
        Get source urls

        Returns:
            Set[str]: Source urls
        """
        return self.source_urls

    def add_population(self) -> None:
        """
        Add population data by looking for the #population HXL hashtag among the
        headers and pulling out the associated values

        Returns:
            None
        """
        for level in self.headers:
            try:
                population_index = self.headers[level][1].index("#population")
            except ValueError:
                population_index = None
            if population_index is not None:
                values = self.values[level][population_index]
                if len(values) == 1 and "value" in values:
                    values = (
                        (
                            self.datasetinfo.get("population_key") or level,
                            values["value"],
                        ),
                    )
                else:
                    values = values.items()
                for key, value in values:
                    try:
                        valint = int(value)
                        self.population_lookup[key] = valint
                    except (TypeError, ValueError):
                        pass

    @abstractmethod
    def run(self) -> None:
        """
        Run scraper. Must be overridden.

        Returns:
            None
        """

    def run_after_fallbacks(self) -> None:
        """
        Executed after fallbacks are used. Can be overridden if needed.

        Returns:
            None
        """
        pass

    def post_run(self) -> None:
        """
        Executed after running. Can be overridden if needed.

        Returns:
            None
        """
        pass
