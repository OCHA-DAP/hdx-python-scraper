import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import now_utc
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.typehint import ListTuple

from .base_scraper import BaseScraper
from .configurable.aggregator import Aggregator
from .configurable.resource_downloader import ResourceDownloader
from .configurable.scraper import ConfigurableScraper
from .configurable.timeseries import TimeSeries
from .outputs.base import BaseOutput
from .utilities import get_startend_dates_from_dataset_date
from .utilities.fallbacks import Fallbacks
from .utilities.reader import Read
from .utilities.sources import Sources

logger = logging.getLogger(__name__)


class Runner:
    """The Runner class is the means by which scrapers are set up and run.

    Args:
        countryiso3s (ListTuple[str]): List of ISO3 country codes to process
        today (datetime): Value to use for today. Defaults to now_utc().
        errors_on_exit (ErrorsOnExit): ErrorsOnExit object that logs errors on exit
        scrapers_to_run (Optional[ListTuple[str]]): Scrapers to run. Defaults to None.
    """

    def __init__(
        self,
        countryiso3s: ListTuple[str],
        today: datetime = now_utc(),
        errors_on_exit: Optional[ErrorsOnExit] = None,
        scrapers_to_run: Optional[ListTuple[str]] = None,
    ):
        self.countryiso3s = countryiso3s
        self.today = today
        self.errors_on_exit = errors_on_exit
        if isinstance(scrapers_to_run, tuple):
            scrapers_to_run = list(scrapers_to_run)
        self.scrapers_to_run: Optional[List[str]] = scrapers_to_run
        self.scrapers = dict()
        self.scraper_names = list()

    def add_custom(
        self, scraper: BaseScraper, force_add_to_run: bool = False
    ) -> str:
        """Add custom scrapers that inherit BaseScraper. If running specific scrapers
        rather than all, and you want to force the inclusion of the scraper in the run
        regardless of the specific scrapers given, the parameter force_add_to_run
        should be set to True.

        Args:
            scraper (BaseScraper): The scraper to add
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            str: scraper name
        """
        scraper_name = scraper.name
        self.scrapers[scraper_name] = scraper
        if scraper_name not in self.scraper_names:
            self.scraper_names.append(scraper_name)
        if (
            force_add_to_run
            and self.scrapers_to_run is not None
            and scraper_name not in self.scrapers_to_run
        ):
            self.scrapers_to_run.append(scraper_name)
        scraper.errors_on_exit = self.errors_on_exit
        return scraper_name

    def add_customs(
        self, scrapers: ListTuple[BaseScraper], force_add_to_run: bool = False
    ) -> List[str]:
        """Add multiple custom scrapers that inherit BaseScraper. If running specific
        scrapers rather than all, and you want to force the inclusion of the scraper in
        the run regardless of the specific scrapers given, the parameter
        force_add_to_run should be set to True.

        Args:
            scrapers (ListTuple[BaseScraper]): The scrapers to add
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            str: scraper name
        """
        scraper_names = list()
        for scraper in scrapers:
            scraper_names.append(self.add_custom(scraper, force_add_to_run))
        return scraper_names

    def add_configurable(
        self,
        name: str,
        datasetinfo: Dict,
        level: str,
        adminlevel: Optional[AdminLevel] = None,
        level_name: Optional[str] = None,
        source_configuration: Dict = dict(),
        suffix: Optional[str] = None,
        force_add_to_run: bool = False,
    ) -> str:
        """Add configurable scraper to the run. If running specific scrapers rather than
        all, and you want to force the inclusion of the scraper in the run regardless of
        the specific scrapers given, the parameter force_add_to_run should be set to
        True.

        Args:
            name (str): Name of scraper
            datasetinfo (Dict): Information about dataset
            level (str): Can be national, subnational or single
            adminlevel (Optional[AdminLevel]): AdminLevel object from HDX Python Country. Defaults to None.
            level_name (Optional[str]): Customised level_name name. Defaults to None (level_name).
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
            suffix (Optional[str]): Suffix to add to the scraper name
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            str: scraper name (including suffix if set)
        """
        if suffix:
            scraper_name = f"{name}{suffix}"
        else:
            scraper_name = name
        self.scrapers[scraper_name] = ConfigurableScraper(
            name,
            datasetinfo,
            level,
            self.countryiso3s,
            adminlevel,
            level_name,
            source_configuration,
            self.today,
            self.errors_on_exit,
        )
        if scraper_name not in self.scraper_names:
            self.scraper_names.append(scraper_name)
        if (
            force_add_to_run
            and self.scrapers_to_run is not None
            and scraper_name not in self.scrapers_to_run
        ):
            self.scrapers_to_run.append(scraper_name)
        return scraper_name

    def add_configurables(
        self,
        configuration: Dict,
        level: str,
        adminlevel: Optional[AdminLevel] = None,
        level_name: Optional[str] = None,
        source_configuration: Dict = dict(),
        suffix: Optional[str] = None,
        force_add_to_run: bool = False,
    ) -> List[str]:
        """Add multiple configurable scrapers to the run. If running specific scrapers
        rather than all, and you want to force the inclusion of the scraper in the run
        regardless of the specific scrapers given, the parameter force_add_to_run
        should be set to True.

        Args:
            configuration (Dict): Mapping from scraper name to information about datasets
            level (str): Can be national, subnational or single
            adminlevel (Optional[AdminLevel]): AdminLevel object from HDX Python Country. Defaults to None.
            level_name (Optional[str]): Customised level_name name. Defaults to None (level_name).
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
            suffix (Optional[str]): Suffix to add to the scraper name
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            List[str]: scraper names (including suffix if set)
        """
        keys = list()
        for name in configuration:
            datasetinfo = configuration[name]
            keys.append(
                self.add_configurable(
                    name,
                    datasetinfo,
                    level,
                    adminlevel,
                    level_name,
                    source_configuration,
                    suffix,
                    force_add_to_run,
                )
            )
        return keys

    def add_timeseries_scraper(
        self,
        name: str,
        datasetinfo: Dict,
        outputs: Dict[str, BaseOutput],
        force_add_to_run: bool = False,
    ) -> str:
        """Add time series scraper to the run. If running specific scrapers rather than
        all, and you want to force the inclusion of the scraper in the run regardless of
        the specific scrapers given, the parameter force_add_to_run should be set to
        True.

        Args:
            name (str): Name of scraper
            datasetinfo (Dict): Information about dataset
            outputs (Dict[str, BaseOutput]): Mapping from names to output objects
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            str: scraper name (including suffix if set)
        """
        return self.add_custom(
            TimeSeries(name, datasetinfo, outputs, self.today),
            force_add_to_run,
        )

    def add_timeseries_scrapers(
        self,
        configuration: Dict,
        outputs: Dict[str, BaseOutput],
        force_add_to_run: bool = False,
    ) -> List[str]:
        """Add multiple time series scrapers to the run. If running specific scrapers
        rather than all, and you want to force the inclusion of the scraper in the run
        regardless of the specific scrapers given, the parameter force_add_to_run should
        be set to True.

        Args:
            configuration (Dict): Mapping from scraper name to information about datasets
            outputs (Dict[str, BaseOutput]): Mapping from names to output objects
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            List[str]: scraper names (including suffix if set)
        """
        keys = list()
        for name, datasetinfo in configuration.items():
            keys.append(
                self.add_timeseries_scraper(
                    name, datasetinfo, outputs, force_add_to_run
                )
            )
        return keys

    def create_aggregator(
        self,
        use_hxl: bool,
        header_or_hxltag: str,
        datasetinfo: Dict,
        input_level: str,
        output_level: str,
        adm_aggregation: Union[Dict, List],
        source_configuration: Dict = dict(),
        names: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
        aggregation_scrapers: List["Aggregator"] = list(),
    ) -> Optional["Aggregator"]:
        """Create aggregator

        Args:
            use_hxl (bool): Whether keys should be HXL hashtags or column headers
            header_or_hxltag (str): Column header or HXL hashtag depending on use_hxl
            datasetinfo (Dict): Information about dataset
            input_level (str): Input level to aggregate like national or subnational
            output_level (str): Output level of aggregated data like regional
            adm_aggregation (Union[Dict, List]): Mapping from input admins to aggregated output admins
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            aggregation_scrapers (List["Aggregator"]): Other aggregations needed. Defaults to list().

        Returns:
            Optional["Aggregator"]: scraper or None
        """
        input_headers = self.get_headers(
            names, [input_level], overrides=overrides
        )
        input_headers = input_headers.get(input_level)
        if not input_headers:
            return None
        scraper = Aggregator.get_scraper(
            use_hxl,
            header_or_hxltag,
            datasetinfo,
            input_level,
            output_level,
            adm_aggregation,
            input_headers,
            source_configuration,
            aggregation_scrapers,
        )
        if not scraper:
            return None

        def get_values_sourcesinfo_by_header(scraper_self):
            (
                input_values,
                input_sourcesinfo,
            ) = self.get_values_sourcesinfo_by_header(
                input_level, names, overrides, True, use_hxl
            )
            scraper_self.set_input_values_sources(
                input_values, input_sourcesinfo
            )

        scraper.pre_run = lambda: get_values_sourcesinfo_by_header(scraper)
        return scraper

    def add_aggregator(
        self,
        use_hxl: bool,
        header_or_hxltag: str,
        datasetinfo: Dict,
        input_level: str,
        output_level: str,
        adm_aggregation: Union[Dict, List],
        source_configuration: Dict = dict(),
        names: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
        aggregation_scrapers: List["Aggregator"] = list(),
        force_add_to_run: bool = False,
    ) -> Optional[str]:
        """Add aggregator to the run. The mapping from input admins to aggregated output
        admins adm_aggregation is of form: {"AFG": ("ROAP",), "MMR": ("ROAP",)}. If the
        mapping is to the top level, then it is a list of input admins like:
        ("AFG", "MMR"). If running specific scrapers rather than all, and you want to
        force the inclusion of the scraper in the run regardless of the specific
        scrapers given, the parameter force_add_to_run should be set to True.

        Args:
            use_hxl (bool): Whether keys should be HXL hashtags or column headers
            header_or_hxltag (str): Column header or HXL hashtag depending on use_hxl
            datasetinfo (Dict): Information about dataset
            input_level (str): Input level to aggregate like national or subnational
            output_level (str): Output level of aggregated data like regional
            adm_aggregation (Union[Dict, List]): Mapping from input admins to aggregated output admins
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None.
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            aggregation_scrapers (List["Aggregator"]): Other aggregations needed. Defaults to list().
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            Optional[str]: scraper name (including suffix if set) or None
        """
        scraper = self.create_aggregator(
            use_hxl,
            header_or_hxltag,
            datasetinfo,
            input_level,
            output_level,
            adm_aggregation,
            source_configuration,
            names,
            overrides,
            aggregation_scrapers,
        )
        if not scraper:
            return None
        return self.add_custom(scraper, force_add_to_run)

    def add_aggregators(
        self,
        use_hxl: bool,
        configuration: Dict,
        input_level: str,
        output_level: str,
        adm_aggregation: Union[Dict, ListTuple],
        source_configuration: Dict = dict(),
        names: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
        force_add_to_run: bool = False,
    ) -> List[str]:
        """Add multiple aggregators to the run. The mapping from input admins to
        aggregated output admins adm_aggregation is of form:
        {"AFG": ("ROAP",), "MMR": ("ROAP",)}. If the mapping is to the top level, then
        it is a list of input admins like: ("AFG", "MMR"). If running specific scrapers
        rather than all, and you want to force the inclusion of the scraper in the run
        regardless of the specific scrapers given, the parameter force_add_to_run should
        be set to True.

        Args:
            use_hxl (bool): Whether keys should be HXL hashtags or column headers
            configuration (Dict): Mapping from scraper name to information about datasets
            input_level (str): Input level to aggregate like national or subnational
            output_level (str): Output level of aggregated data like regional
            adm_aggregation (Union[Dict, ListTuple]): Mapping from input admins to aggregated output admins
            source_configuration (Dict): Configuration for sources. Defaults to empty dict (use defaults).
            names (Optional[ListTuple[str]]): Names of scrapers
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            List[str]: scraper names (including suffix if set)
        """
        scrapers = list()
        for header_or_hxltag, datasetinfo in configuration.items():
            scraper = self.create_aggregator(
                use_hxl,
                header_or_hxltag,
                datasetinfo,
                input_level,
                output_level,
                adm_aggregation,
                source_configuration,
                names,
                overrides,
                scrapers,
            )
            if scraper:
                scrapers.append(scraper)
        return self.add_customs(scrapers, force_add_to_run)

    def add_resource_downloader(
        self,
        datasetinfo: Dict,
        folder: str = "",
        force_add_to_run: bool = False,
    ) -> str:
        """Add resource downloader to the run. If running specific scrapers rather than
        all, and you want to force the inclusion of the scraper in the run regardless of
        the specific scrapers given, the parameter force_add_to_run should be set to
        True.

        Args:
            datasetinfo (Dict): Information about dataset
            folder (str): Folder to which to download. Defaults to "".
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            str: scraper name (including suffix if set)
        """
        return self.add_custom(
            ResourceDownloader(datasetinfo, folder), force_add_to_run
        )

    def add_resource_downloaders(
        self,
        configuration: Dict,
        folder: str = "",
        force_add_to_run: bool = False,
    ) -> List[str]:
        """Add multiple resource downloaders to the run. If running specific scrapers
        rather than all, and you want to force the inclusion of the scraper in the run
        regardless of the specific scrapers given, the parameter force_add_to_run should
        be set to True.

        Args:
            configuration (Dict): Mapping from scraper name to information about datasets
            folder (str): Folder to which to download. Defaults to "".
            force_add_to_run (bool): Whether to force include the scraper in the next run

        Returns:
            List[str]: scraper names (including suffix if set)
        """
        keys = list()
        for datasetinfo in configuration:
            keys.append(
                self.add_resource_downloader(
                    datasetinfo, folder, force_add_to_run
                )
            )
        return keys

    def prioritise_scrapers(self, scraper_names: ListTuple[str]) -> None:
        """Set certain scrapers to run first

        Args:
            scraper_names (ListTuple[str]): Names of scrapers to run first

        Returns:
            None
        """
        for scraper_name in reversed(scraper_names):
            if scraper_name in self.scraper_names:
                self.scraper_names.remove(scraper_name)
                self.scraper_names.insert(0, scraper_name)

    def get_scraper_names(self) -> List[str]:
        """Get names of scrapers

        Returns:
            List[str]: Names of scrapers
        """
        return self.scraper_names

    def get_scraper(self, name: str) -> BaseScraper:
        """Get scraper given name

        Args:
            name (str): Name of scraper

        Returns:
            Optional[BaseScraper]: Scraper or None if there is no scraper with given name
        """
        return self.scrapers.get(name)

    def get_scraper_exception(self, name: str) -> BaseScraper:
        """Get scraper given name. Throws exception if there is no scraper with the
        given name.

        Args:
            name (str): Name of scraper

        Returns:
            BaseScraper: Scraper
        """
        scraper = self.get_scraper(name)
        if not scraper:
            raise ValueError(f"No such scraper {name}!")
        return scraper

    def add_instance_variables(self, name: str, **kwargs: Any) -> None:
        """Add instance variables to scraper instance given scraper name

        Args:
            name (str): Name of scraper
            **kwargs: Instance name value pairs to add to scraper instance

        Returns:
            None
        """
        scraper = self.get_scraper_exception(name)
        for key, value in kwargs.items():
            setattr(scraper, key, value)

    def add_pre_run(
        self, name: str, fn: Callable[[BaseScraper], None]
    ) -> None:
        """Add pre run instance method to scraper instance given scraper name. The
        function should have one parameter. Since it is being added as an instance
        method to the scraper instance, that parameter will be self and hence is of
        type BaseScraper. The function does not need to return anything.

        Args:
            name (str): Name of scraper
            fn (Callable[[BaseScraper], None]): Function to call pre run

        Returns:
            None
        """
        scraper = self.get_scraper_exception(name)
        scraper.pre_run = lambda: fn(scraper)

    def add_post_run(
        self, name: str, fn: Callable[[BaseScraper], None]
    ) -> None:
        """Add post run instance method to scraper instance given scraper name. The
        function should have one parameter. Since it is being added as an instance
        method to the scraper instance, that parameter will be self and hence is of
        type BaseScraper. The function does not need to return anything.

        Args:
            name (str): Name of scraper
            fn (Callable[[BaseScraper], None]): Function to call post run

        Returns:
            None
        """
        scraper = self.get_scraper_exception(name)
        scraper.post_run = lambda: fn(scraper)

    def run_one(self, name: str, force_run: bool = False) -> bool:
        """Run scraper with given name, adding sources and population to global
        dictionary. If scraper run fails and fallbacks have been set up, use them.

        Args:
            name (str): Name of scraper
            force_run (bool): Force run even if scraper marked as already run

        Returns:
            bool: Return True if scraper was run, False if not
        """
        scraper = self.get_scraper_exception(name)
        if scraper.has_run is False or force_run:
            try:
                scraper.pre_run()
                scraper.run()
                scraper.add_sources()
                scraper.add_source_urls()
                scraper.add_population()
                logger.info(f"Processed {scraper.name}")
            except Exception as ex:
                if not Fallbacks.exist() or scraper.can_fallback is False:
                    raise
                logger.exception(f"Using fallbacks for {scraper.name}!")
                if self.errors_on_exit:
                    self.errors_on_exit.add(
                        f"Using fallbacks for {scraper.name}! Error: {ex}"
                    )
                for level in scraper.headers.keys():
                    values, sources = Fallbacks.get(
                        level, scraper.headers[level]
                    )
                    scraper.values[level] = values
                    scraper.sources[level] = sources
                scraper.add_population()
                scraper.fallbacks_used = True
                scraper.run_after_fallbacks()
            scraper.has_run = True
            scraper.post_run()
            return True
        return False

    def run_scraper(self, name: str, force_run: bool = False) -> bool:
        """Check scraper with given name is in the list of scrapers to run. If it isn't,
         return False, otherwise run it (including force running scrapers that have
         already run if force_run is True), adding sources and population to global
         dictionary. If scraper run fails and fallbacks have been set up, use them.

        Args:
            name (str): Name of scraper
            force_run (bool): Force run even if scraper marked as already run

        Returns:
            bool: Return True if scraper was run, False if not
        """
        if self.scrapers_to_run and not any(
            x in name for x in self.scrapers_to_run
        ):
            return False
        logger.info(f"Running {name}")
        return self.run_one(name, force_run)

    def run(
        self,
        what_to_run: Optional[ListTuple[str]] = None,
        force_run: bool = False,
        prioritise_scrapers: Optional[ListTuple[str]] = None,
    ) -> None:
        """Run scrapers limiting to those in what_to_run if given (including force
        running scrapers that have already run if force_run is True), adding sources
        and population to global dictionary. Scrapers given by prioritise_scrapers
        are run first. If scraper run fails and fallbacks have been set up, use them.

        Args:
            what_to_run (Optional[ListTuple[str]]): Run only these scrapers. Defaults to None (run all).
            force_run (bool): Force run even if any scraper marked as already run
            prioritise_scrapers (Optional[ListTuple[str]]): Scrapers to run first. Defaults to None.

        Returns:
            None
        """
        if prioritise_scrapers:
            self.prioritise_scrapers(prioritise_scrapers)
        for name in self.scraper_names:
            if what_to_run and name not in what_to_run:
                continue
            self.run_scraper(name, force_run)

    def set_not_run(self, name: str) -> None:
        """Set scraper given by name as not run

        Args:
            name (str): Name of scraper

        Returns:
            None
        """
        self.get_scraper(name).has_run = False

    def set_not_run_many(self, names: ListTuple[str]) -> None:
        """Set scrapers given by names as not run

        Args:
            names (ListTuple[str]): Names of scraper

        Returns:
            None
        """
        for name in names:
            self.get_scraper(name).has_run = False

    def get_headers(
        self,
        names: Optional[ListTuple[str]] = None,
        levels: Optional[ListTuple[str]] = None,
        headers: Optional[ListTuple[str]] = None,
        hxltags: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
    ) -> Dict[str, Tuple]:
        """Get the headers for scrapers limiting to those in names if given and
        limiting further to those that have been set in the constructor if previously
        given. All levels will be obtained unless the levels parameter (which can
        contain levels like national, subnational or single) is passed. The dictionary
        returned can be limited to given headers or hxltags.

        Args:
            names (Optional[ListTuple[str]]): Names of scraper
            levels (Optional[ListTuple[str]]): Levels to get like national, subnational or single
            headers (Optional[ListTuple[str]]): Headers to get
            hxltags (Optional[ListTuple[str]]): HXL hashtags to get
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().


        Returns:
            Dict[str, Tuple]: Dictionary that maps each level to (list of headers, list of hxltags)
        """
        if not names:
            names = self.scrapers.keys()
        results = dict()

        def add_level_results(
            scraper_level, override_level, scrap, levels_used
        ):
            nonlocal results

            if scraper_level in levels_used:
                return
            if levels is not None and override_level not in levels:
                return
            level_results = results.get(override_level)
            if level_results is None:
                level_results = (list(), list())
                results[override_level] = level_results
            scrp_headers = scrap.headers.get(scraper_level)
            for i, header in enumerate(scrp_headers[0]):
                if headers is not None and header not in headers:
                    continue
                hxltag = scrp_headers[1][i]
                if hxltags is not None and hxltag not in hxltags:
                    continue
                level_results[0].append(header)
                level_results[1].append(hxltag)
            levels_used.add(scraper_level)
            levels_used.add(override_level)

        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            override = overrides.get(name, dict())
            lvls_used = set()
            for scrap_level, over_level in override.items():
                add_level_results(scrap_level, over_level, scraper, lvls_used)
            for scrap_level in scraper.headers:
                add_level_results(scrap_level, scrap_level, scraper, lvls_used)
        return results

    def get_results(
        self,
        names: Optional[ListTuple[str]] = None,
        levels: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
        has_run: bool = True,
        should_overwrite_sources: Optional[bool] = None,
    ) -> Dict[str, Dict]:
        """Get the results (headers, values and sources) for scrapers limiting to those
        in names if given and limiting further to those that have been set in the
        constructor if previously given. All levels will be obtained unless the levels
        parameter (which can contain levels like national, subnational or single) is
        passed. Sometimes it may be necessary to map alternative level names to levels
        and this can be done using overrides. It is a dictionary with keys being scraper
        names and values being dictionaries which map level names to output levels. By
        default only scrapers marked as having run are returned unless has_run is set to
        False. The results dictionary has keys for each output level and values which
        are dictionaries with keys headers, values, sources and fallbacks. Headers is
        a tuple of (column headers, hxl hashtags). Values, sources and fallbacks are all
        lists.

        Args:
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None (all scrapers).
            levels (Optional[ListTuple[str]]): Levels to get like national, subnational or single
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            has_run (bool): Only get results for scrapers marked as having run. Defaults to True.
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (class default).

        Returns:
            Dict[str, Dict]: Results dictionary that maps each level to headers, values, sources, fallbacks.
        """
        if not names:
            names = self.scrapers.keys()
        results = dict()

        def add_level_results(
            scraper_level, override_level, scrap, levels_used
        ):
            nonlocal results

            if scraper_level in levels_used:
                return
            if levels is not None and override_level not in levels:
                return
            headers = scrap.headers.get(scraper_level)
            if headers is None:
                return
            level_results = results.get(override_level)
            if level_results is None:
                level_results = {
                    "headers": (list(), list()),
                    "values": list(),
                    "sources": list(),
                    "source_hxltags": list(),
                    "fallbacks": list(),
                }
                results[override_level] = level_results
            headings = headers[0]
            hxltags = headers[1]
            values = scrap.get_values(scraper_level)
            lev_headings = level_results["headers"][0]
            lev_hxltags = level_results["headers"][1]
            lev_values = level_results["values"]
            scraper_should_overwrite_sources = (
                scraper.source_configuration.get(
                    "should_overwrite_sources", should_overwrite_sources
                )
            )
            for i, hxltag in enumerate(hxltags):
                if hxltag in lev_hxltags:
                    index = lev_hxltags.index(hxltag)
                    lev_values[index].update(values[i])
                else:
                    lev_headings.append(headings[i])
                    lev_hxltags.append(hxltag)
                    lev_values.append(values[i])
            lev_source_hxltags = level_results["source_hxltags"]
            lev_sources = level_results["sources"]
            Sources.add_sources_overwrite(
                lev_source_hxltags,
                lev_sources,
                scrap.get_sources(scraper_level),
                logger,
                scraper_should_overwrite_sources,
            )
            levels_used.add(scraper_level)
            levels_used.add(override_level)

        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            if has_run and not scraper.has_run:
                continue
            override = overrides.get(name, dict())
            lvls_used = set()
            for scrap_level, over_level in override.items():
                add_level_results(scrap_level, over_level, scraper, lvls_used)
            for scrap_level in scraper.headers:
                add_level_results(scrap_level, scrap_level, scraper, lvls_used)

        for level in results:
            del results[level]["source_hxltags"]
        return results

    def get_rows(
        self,
        level: str,
        adms: ListTuple[str],
        headers: ListTuple[ListTuple] = (tuple(), tuple()),
        row_fns: ListTuple[Callable[[str], str]] = tuple(),
        names: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
    ) -> List[List]:
        """Get rows for a given level for scrapers limiting to those in names if given.
        Rows include header row, HXL hashtag row and value rows, one for each admin unit
        specified in the adms parameter. Additional columns can be included by specifying
        headers and row_fns. Headers are of the form (list of headers, list of HXL
        hashtags). row_fns are functions that accept an admin unit and return a string.
        Sometimes it may be necessary to map alternative level names to levels and this
        can be done using overrides. It is a dictionary with keys being scraper names
        and values being dictionaries which map level names to output levels.

        Args:
            level (str): Level to get like national, subnational or single
            adms (ListTuple[str]): Admin units
            headers (ListTuple[ListTuple]): Additional headers in the form (list of headers, list of HXL hashtags)
            row_fns (ListTuple[Callable[[str], str]]): Functions to populate additional columns
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None (all scrapers).
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().

        Returns:
            List[List]: Rows for a given level
        """
        results = self.get_results(names, [level], overrides=overrides).get(
            level
        )
        rows = list()
        if results:
            all_headers = results["headers"]
            rows.append(list(headers[0]) + all_headers[0])
            rows.append(list(headers[1]) + all_headers[1])
            all_values = results["values"]
            for adm in adms:
                row = list()
                for fn in row_fns:
                    row.append(fn(adm))
                for values in all_values:
                    row.append(values.get(adm))
                rows.append(row)
        return rows

    def get_values_sourcesinfo_by_header(
        self,
        level: str,
        names: Optional[ListTuple[str]] = None,
        overrides: Dict[str, Dict] = dict(),
        has_run: bool = True,
        use_hxl: bool = True,
    ) -> Tuple[Dict, Dict]:
        """Get mapping from headers to values and headers to sources information for a
        given level for scrapers limiting to those in names if given. Keys will be
        headers if use_hxl is False or HXL hashtags if use_hxl is True. Sometimes it may
        be necessary to map alternative level names to levels and this can be done using
        overrides. It is a dictionary with keys being scraper names and values being
        dictionaries which map level names to output levels.

        Args:
            level (str): Level to get like national, subnational or single
            names (Optional[ListTuple[str]]): Names of scrapers. Defaults to None (all scrapers).
            overrides (Dict[str, Dict]): Dictionary mapping scrapers to level mappings. Defaults to dict().
            has_run (bool): Only get results for scrapers marked as having run. Defaults to True.
            use_hxl (bool): Whether keys should be HXL hashtags or column headers. Defaults to True.

        Returns:
            Tuple[Dict, Dict]: Tuple of (headers to values, headers to sources)
        """
        if not names:
            names = self.scrapers.keys()
        values = dict()
        sourcesinfo = dict()

        if use_hxl:
            main_index = 1
        else:
            main_index = 0

        def add_level_results(
            scraper_level, override_level, scrap, levels_used
        ):
            nonlocal values, sourcesinfo

            if scraper_level in levels_used:
                return
            if override_level != level:
                return
            scrp_headers = scrap.headers.get(scraper_level)
            scrp_values = scrap.get_values(scraper_level)
            scrp_datasetinfo = scrap.datasetinfo
            for i, header in enumerate(scrp_headers[0]):
                header_or_hxltag = scrp_headers[main_index][i]
                if header_or_hxltag in values:
                    values[header_or_hxltag].update(scrp_values[i])
                else:
                    values[header_or_hxltag] = scrp_values[i]
                hxltag = scrp_headers[1][i]

                sourceinfo = sourcesinfo.get(header_or_hxltag, dict())
                scrp_source = scrp_datasetinfo.get("source")
                if scrp_source:
                    source = sourceinfo.get("source", list())
                    if not isinstance(scrp_source, str):
                        scrp_source = scrp_source.get(
                            hxltag, scrp_source["default_source"]
                        )
                    if scrp_source not in source:
                        source.append(scrp_source)
                    sourceinfo["source"] = source
                scrp_source_url = scrp_datasetinfo.get("source_url")
                if scrp_source_url:
                    source_url = sourceinfo.get("source_url", list())
                    if not isinstance(scrp_source_url, str):
                        scrp_source_url = scrp_source_url.get(
                            hxltag, scrp_source_url["default_url"]
                        )
                    if scrp_source_url not in source_url:
                        source_url.append(scrp_source_url)
                    sourceinfo["source_url"] = source_url

                if "source_date" not in scrp_datasetinfo:
                    continue
                scrp_date = Sources.get_hxltag_source_datetime(
                    scrp_datasetinfo, hxltag, True
                )
                scrp_startdate = scrp_date.get("start")
                scrp_enddate = scrp_date["end"]
                date = sourceinfo.get("source_date", dict())
                if date:
                    startdate = date["start"]
                    if scrp_startdate:
                        if startdate:
                            if scrp_startdate < startdate:
                                date["start"] = scrp_startdate
                        else:
                            date["start"] = scrp_startdate
                    if scrp_enddate > date["end"]:
                        date["end"] = scrp_enddate
                else:
                    date["start"] = scrp_startdate
                    date["end"] = scrp_enddate
                sourceinfo["source_date"] = date
                sourcesinfo[header_or_hxltag] = sourceinfo

            levels_used.add(scraper_level)
            levels_used.add(override_level)

        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            if has_run and not scraper.has_run:
                continue
            override = overrides.get(name, dict())
            lvls_used = set()
            for scrap_level, over_level in override.items():
                add_level_results(scrap_level, over_level, scraper, lvls_used)
            for scrap_level in scraper.headers:
                add_level_results(scrap_level, scrap_level, scraper, lvls_used)

        return values, sourcesinfo

    def get_sources(
        self,
        names: Optional[ListTuple[str]] = None,
        levels: Optional[ListTuple[str]] = None,
        additional_sources: ListTuple[Dict] = tuple(),
        should_overwrite_sources: Optional[bool] = None,
    ) -> List[Tuple]:
        """Get sources for scrapers limiting to those in names if given. All levels will
        be obtained unless the levels parameter (which can contain levels like national,
        subnational or single) is passed. Additional sources can be added. Each is a
        dictionary with indicator (specified with HXL hash tag), dataset or source and
        source_url as well as the source_date or whether to force_date_today.

        Args:
            names (Optional[ListTuple[str]]): Names of scrapers
            levels (Optional[ListTuple[str]]): Levels to get like national, subnational or single
            additional_sources (ListTuple[Dict]): Additional sources to add
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (class default).

        Returns:
            List[Tuple]: Sources in form (indicator, date, source, source_url)
        """
        if not names:
            names = self.scrapers.keys()
        sources = list()
        hxltags = list()

        reader = Read.get_reader()

        def add_additional_sources():
            for sourceinfo in additional_sources:
                hxltag = sourceinfo["indicator"]
                date = sourceinfo.get("source_date")
                if sourceinfo.get("force_date_today", False):
                    date = self.today
                source_name = sourceinfo.get("source")
                source_url = sourceinfo.get("source_url")
                dataset_name = sourceinfo.get("dataset")
                if dataset_name:
                    dataset = reader.read_dataset(dataset_name)
                    if date is None:
                        date = get_startend_dates_from_dataset_date(
                            dataset, today=self.today
                        )
                    if source_name is None:
                        source_name = dataset["dataset_source"]
                    if source_url is None:
                        source_url = dataset.get_hdx_url()
                if date:
                    sourceinfo["source_date"] = date
                    date = Sources.get_hxltag_source_date(
                        sourceinfo, hxltag, fallback=True
                    )
                copy = sourceinfo.get("copy")
                if copy:
                    try:
                        index = hxltags.index(copy)
                        orig_source = sources[index]
                        if not date:
                            date = orig_source[1]
                        if not source_name:
                            source_name = orig_source[2]
                        if not source_url:
                            source_url = orig_source[3]
                    except ValueError:
                        continue
                source = (hxltag, date, source_name, source_url)
                should_overwrite_source = sourceinfo.get(
                    "should_overwrite_source", should_overwrite_sources
                )
                Sources.add_source_overwrite(
                    hxltags, sources, source, logger, should_overwrite_source
                )

        add_additional_sources()
        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
            if levels is not None:
                levels_to_check = levels
            else:
                levels_to_check = scraper.sources.keys()
            scraper_should_overwrite_sources = (
                scraper.source_configuration.get(
                    "should_overwrite_sources", should_overwrite_sources
                )
            )
            for level in levels_to_check:
                Sources.add_sources_overwrite(
                    hxltags,
                    sources,
                    scraper.get_sources(level),
                    logger,
                    scraper_should_overwrite_sources,
                )
        add_additional_sources()
        return sources

    def get_source_urls(
        self, names: Optional[ListTuple[str]] = None
    ) -> List[str]:
        """Get source urls for scrapers limiting to those in names if given.

        Args:
            names (Optional[ListTuple[str]]): Names of scrapers

        Returns:
            List[str]: List of source urls
        """
        source_urls = set()
        if not names:
            names = self.scrapers.keys()
        for name in names:
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
            source_urls.update(scraper.get_source_urls())
        return sorted(source_urls)
