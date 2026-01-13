from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import deepcopy

from hdx.utilities.dictandlist import dict_of_lists_add

from .utilities.reader import Read
from .utilities.sources import Sources


class BaseScraper(ABC):
    """Base scraper class for scrapers to inherit

    Args:
        name: Name of scraper
        datasetinfo: Information about dataset
        headers: Headers to be oytput at each level_name
        source_configuration: Configuration for sources. Default is empty dict (use defaults).
        reader: Reader to use. Default is "" (datasetinfo reader falling back on name).
    """

    population_lookup = {}

    def __init__(
        self,
        name: str,
        datasetinfo: dict,
        headers: dict[str, tuple],
        source_configuration: dict = {},
        reader: str = "",
    ) -> None:
        self.name = name
        if reader:
            self.reader = reader
        else:
            self.reader = datasetinfo.get("reader", name)
        self.setup(headers, source_configuration)
        self.datasetinfo = deepcopy(datasetinfo)
        self.error_handler = None
        self.can_fallback = True

    def setup(
        self,
        headers: dict[str, tuple],
        source_configuration: dict = {},
    ) -> None:
        """Initialise member variables including name and headers which is of form:
        {"national": (("School Closure",), ("#impact+type",)), ...},

        Args:
            headers: Headers to be output at each level_name
            source_configuration: Configuration for sources. Default is empty dict (use defaults).

        Returns:
             None
        """
        self.headers = headers
        self.initialise_values_sources(source_configuration)
        self.has_run = False
        self.fallbacks_used = False
        self.source_urls = set()
        self.population_key = None

    def initialise_values_sources(
        self,
        source_configuration: dict = {},
    ) -> None:
        """
        Create values and sources member variables for inheriting scrapers to populate.
        values will be of form:
        {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}
        sources will be of form:
        {"national": [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

        Args:
            source_configuration: Configuration for sources. Default is empty dict (use defaults).

        Returns:
             None
        """
        self.values: dict[str, tuple] = {
            level: tuple({} for _ in value[0]) for level, value in self.headers.items()
        }
        self.sources: dict[str, list] = {level: [] for level in self.headers}
        self.source_configuration = deepcopy(source_configuration)

    def get_reader(self, name: str | None = None):
        """Get reader given name if provided or using name member variable if
        not.

        Args:
            name: Name of scraper

        Returns:
             None
        """
        if not name:
            name = self.reader
        reader = Read.get_reader(name)
        return reader

    def get_headers(self, level: str) -> tuple[tuple] | None:
        """
        Get headers for a particular level_name like national or subnational. Will be
        of form: (("School Closure",), ("#impact+type",))

        Args:
            level: Level to get like national, subnational or single

        Returns:
            Scraper headers or None
        """
        return self.headers.get(level)

    def get_values(self, level: str) -> tuple | None:
        """
        Get values for a particular level_name like national or subnational. Will be of
        form: ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}

        Args:
            level: Level for which to get headers

        Returns:
            Scraper values or None
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
        should_overwrite_sources = self.datasetinfo.get("should_overwrite_sources")
        if should_overwrite_sources is not None:
            self.source_configuration["should_overwrite_sources"] = (
                should_overwrite_sources
            )
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
            self.sources[level] = []

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
                suffix_attribute = self.source_configuration.get("suffix_attribute")
                if suffix_attribute:
                    add_source(hxltag, suffix_attribute)
                    continue
                values = self.get_values(level)[i]
                admin_sources = self.source_configuration.get("admin_sources", False)
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
                out_adms = []
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
        datasetinfo: dict | None = None,
        key: str | None = None,
    ) -> None:
        """
        Adds source identified by HXL hashtag under a particular key.

        Args:
            hxltag: HXL hashtag to use for source
            datasetinfo: Information about dataset. Default is None (use self.datasetinfo).
            key: Key under which to add source. Default is None (use scraper name).

        Returns:
            None
        """
        if datasetinfo is None:
            datasetinfo = self.datasetinfo
        date = Sources.get_hxltag_source_date(datasetinfo, hxltag, fallback=True)
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
        hxltags: Sequence[str],
        datasetinfo: dict | None = None,
        key: str | None = None,
        suffix_attributes: Sequence | None = None,
    ) -> None:
        """
        Adds sources identified by HXL hashtags under a particular key.

        Args:
            hxltags: HXL hashtags to use for sources
            datasetinfo: Information about dataset. Default is None (use self.datasetinfo).
            key: Key under which to add source. Default is None (use scraper name).
            suffix_attributes: List of suffix attributes to append to HXL hashtags eg. iso3 codes

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

    def get_sources(self, level: str) -> list[tuple] | None:
        """
        Get values for a particular level_name like national or subnational. Will be of
        form:
        [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

        Args:
            level: Level to get like national, subnational or single

        Returns:
            Scraper sources or None
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

    def get_source_urls(self) -> set[str]:
        """
        Get source urls

        Returns:
            Source urls
        """
        return self.source_urls

    def get_hapi_dataset_metadata(self) -> dict | None:
        """
        Get HAPI dataset metadata

        Returns:
            HAPI dataset metadata
        """
        return self.datasetinfo.get("hapi_dataset_metadata")

    def get_hapi_resource_metadata(self) -> dict | None:
        """
        Get HAPI resource metadata

        Returns:
            HAPI resource metadata
        """
        hapi_resource_metadata = self.datasetinfo.get("hapi_resource_metadata")
        if not hapi_resource_metadata:
            return None
        if "is_hxl" in hapi_resource_metadata:
            return hapi_resource_metadata
        reader = self.get_reader()
        filename = self.datasetinfo.get("filename")
        file_prefix = self.datasetinfo.get("file_prefix", self.name)
        if filename:
            kwargs = {"filename": filename}
        else:
            kwargs = {"file_prefix": file_prefix}
        hxl_info = reader.hxl_info_hapi_resource_metadata(
            hapi_resource_metadata,
            **kwargs,
        )
        is_hxl = False
        if hxl_info:
            for sheet in hxl_info.get("sheets", ()):
                if sheet["is_hxlated"]:
                    is_hxl = True
                    break
        hapi_resource_metadata["is_hxl"] = is_hxl

        return self.datasetinfo.get("hapi_resource_metadata")

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

    def pre_run(self) -> None:
        """
        Executed before running. Can be overridden if needed.

        Returns:
            None
        """

    def post_run(self) -> None:
        """
        Executed after running. Can be overridden if needed.

        Returns:
            None
        """
