from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Dict, List, Optional, Set, Tuple

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.typehint import ListTuple

from .utilities.reader import Read
from .utilities.sources import Sources


class BaseScraper(ABC):
    """Base scraper class for scrapers to inherit

    Args:
        name (str): Name of scraper
        datasetinfo (Dict): Information about dataset
        headers (Dict[str, Tuple]): Headers to be oytput at each level_name
        source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
    """

    population_lookup = dict()

    def __init__(
        self,
        name: str,
        datasetinfo: Dict,
        headers: Dict[str, Tuple],
        source_configuration: Dict = dict(),
    ) -> None:
        self.setup(name, headers, source_configuration)
        self.datasetinfo = deepcopy(datasetinfo)
        self.errors_on_exit = None
        self.can_fallback = True

    def setup(
        self,
        name: str,
        headers: Dict[str, Tuple],
        source_configuration: Dict = dict(),
    ) -> None:
        """Initialise member variables including name and headers which is of form:
        {"national": (("School Closure",), ("#impact+type",)), ...},

        Args:
            name (str): Name of scraper
            headers (Dict[str, Tuple]): Headers to be output at each level_name
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).

        Returns:
             None
        """
        self.name = name
        self.headers = headers
        self.initialise_values_sources(source_configuration)
        self.has_run = False
        self.fallbacks_used = False
        self.source_urls = set()
        self.population_key = None

    def initialise_values_sources(
        self,
        source_configuration: Dict = dict(),
    ) -> None:
        """
        Create values and sources member variables for inheriting scrapers to populate.
        values will be of form:
        {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}
        sources will be of form:
        {"national": [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

        Args:
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).

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
        self.source_configuration = deepcopy(source_configuration)

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
        if self.source_configuration.get("no_sources", False):
            return
        if self.datasetinfo.get("no_sources", False):
            return
        should_overwrite_sources = self.datasetinfo.get(
            "should_overwrite_sources"
        )
        if should_overwrite_sources is not None:
            self.source_configuration[
                "should_overwrite_sources"
            ] = should_overwrite_sources
        source = self.datasetinfo["source"]
        if isinstance(source, str):
            source = {"default_source": source}
        source_url = self.datasetinfo["source_url"]
        if isinstance(source_url, str):
            source_url = {"default_url": source_url}
        Sources.standardise_datasetinfo_source_date(self.datasetinfo)
        if not any(
            key in self.source_configuration
            for key in ("suffix_attribute", "admin_sources")
        ):
            for level in self.headers:
                self.sources[level] = [
                    (
                        hxltag,
                        Sources.get_hxltag_source_date(
                            self.datasetinfo, hxltag, fallback=True
                        ),
                        source.get(hxltag, source["default_source"]),
                        source_url.get(hxltag, source_url["default_url"]),
                    )
                    for hxltag in self.headers[level][1]
                ]
            return
        for level in self.headers:
            self.sources[level] = list()

            def add_source(hxltag, suffix_attribute):
                hxltag_suffix = f"{hxltag}+{suffix_attribute.lower()}"
                source_suffix = f"CUSTOM_{suffix_attribute}"
                out_date = Sources.get_hxltag_source_date(
                    self.datasetinfo, hxltag_suffix
                )
                if not out_date:
                    out_date = Sources.get_hxltag_source_date(
                        self.datasetinfo, source_suffix
                    )
                if not out_date:
                    out_date = Sources.get_hxltag_source_date(
                        self.datasetinfo, hxltag, fallback=True
                    )
                out_source = source.get(hxltag_suffix)
                if not out_source:
                    out_source = source.get(source_suffix)
                if not out_source:
                    out_source = source.get(hxltag)
                if not out_source:
                    out_source = source["default_source"]
                out_url = source_url.get(hxltag_suffix)
                if not out_url:
                    out_url = source_url.get(source_suffix)
                if not out_url:
                    out_url = source_url.get(hxltag)
                if not out_url:
                    out_url = source_url["default_url"]
                self.sources[level].append(
                    (
                        hxltag_suffix,
                        out_date,
                        out_source,
                        out_url,
                    )
                )

            for i, hxltag in enumerate(self.headers[level][1]):
                suffix_attribute = self.source_configuration.get(
                    "suffix_attribute"
                )
                if suffix_attribute:
                    add_source(hxltag, suffix_attribute)
                    continue
                values = self.get_values(level)[i]
                admin_sources = self.source_configuration.get(
                    "admin_sources", False
                )
                if not admin_sources:
                    raise ValueError("Invalid source configuration!")
                admin_mapping = self.source_configuration.get("admin_mapping")
                if len(values) == 1 and next(iter(values)) == "value":
                    if admin_mapping:
                        out_adm = admin_mapping.get(level)
                    else:
                        out_adm = level
                    if out_adm:
                        add_source(hxltag, out_adm)
                    continue
                out_adms = list()
                for adm in values.keys():
                    if admin_mapping:
                        out_adm = admin_mapping.get(adm)
                        if out_adm and out_adm not in out_adms:
                            out_adms.append(out_adm)
                    else:
                        out_adms.append(adm)
                for out_adm in out_adms:
                    add_source(hxltag, out_adm)

    def add_hxltag_source(
        self,
        hxltag: str,
        datasetinfo: Optional[Dict] = None,
        key: Optional[str] = None,
    ) -> None:
        """
        Adds source identified by HXL hashtag under a particular key.

        Args:
            hxltag (str): HXL hashtag to use for source
            datasetinfo (Optional[Dict]): Information about dataset. Defaults to None (use self.datasetinfo).
            key (Optional[str]): Key under which to add source. Defaults to None (use scraper name).

        Returns:
            None
        """
        if datasetinfo is None:
            datasetinfo = self.datasetinfo
        date = Sources.get_hxltag_source_date(
            datasetinfo, hxltag, fallback=True
        )
        if key is None:
            key = self.name
        dict_of_lists_add(
            self.sources,
            key,
            (
                hxltag,
                date,
                datasetinfo["source"],
                datasetinfo["source_url"],
            ),
        )

    def add_hxltag_sources(
        self,
        hxltags: ListTuple[str],
        datasetinfo: Optional[Dict] = None,
        key: Optional[str] = None,
        suffix_attributes: Optional[ListTuple] = None,
    ) -> None:
        """
        Adds sources identified by HXL hashtags under a particular key.

        Args:
            hxltags (ListTuple[str]): HXL hashtags to use for sources
            datasetinfo (Optional[Dict]): Information about dataset. Defaults to None (use self.datasetinfo).
            key (Optional[str]): Key under which to add source. Defaults to None (use scraper name).
            suffix_attributes (Optional[ListTuple]): List of suffix attributes to append to HXL hashtags eg. iso3 codes

        Returns:
            None
        """
        for hxltag in hxltags:
            if suffix_attributes is None:
                self.add_hxltag_source(hxltag, datasetinfo, key)
            else:
                for suffix_attribute in suffix_attributes:
                    self.add_hxltag_source(
                        f"{hxltag}+{suffix_attribute.lower()}",
                        datasetinfo,
                        key,
                    )

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

    def pre_run(self) -> None:
        """
        Executed before running. Can be overridden if needed.

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
