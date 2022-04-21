import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl

from hdx.data.dataset import Dataset
from hdx.location.adminone import AdminOne
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve
from hdx.utilities.typehint import ListTuple

from .base_scraper import BaseScraper
from .configurable.scraper import ConfigurableScraper
from .utilities import get_isodate_from_dataset_date
from .utilities.fallbacks import Fallbacks

logger = logging.getLogger(__name__)


class Runner:
    """Runner class

    Args:
        countryiso3s (ListTuple[str]): List of ISO3 country codes to process
        adminone (AdminOne): AdminOne object from HDX Python Country library
        today (datetime): Value to use for today. Defaults to datetime.now().
        errors_on_exit (ErrorsOnExit): ErrorsOnExit object that logs errors on exit
        scrapers_to_run (Optional[ListTuple[str]]): Scrapers to run. Defaults to None.
    """

    def __init__(
        self,
        countryiso3s: ListTuple[str],
        adminone: AdminOne,
        today: datetime = datetime.now(),
        errors_on_exit: Optional[ErrorsOnExit] = None,
        scrapers_to_run: Optional[ListTuple[str]] = None,
    ):
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.today = today
        self.errors_on_exit = errors_on_exit
        if isinstance(scrapers_to_run, tuple):
            scrapers_to_run = list(scrapers_to_run)
        self.scrapers_to_run: Optional[List[str]] = scrapers_to_run
        self.scrapers = dict()
        self.scraper_names = list()

    @staticmethod
    def create_retrievers(
        fallback_dir: str,
        saved_dir: str,
        temp_dir: str,
        save: bool = False,
        use_saved: bool = False,
        ignore: ListTuple[str] = tuple(),
        rate_limit: Optional[Dict] = {"calls": 1, "period": 0.1},
        **kwargs: Any,
    ):
        """Generate a default retriever. Additional retrievers are generated if any of
        header_auths, basic_auths or extra_params are populated. header_auths and
        basic_auths are dictionaries of form {"scraper name": "auth", ...}. extra_params
        is of form {"scraper name": {"key": "auth", ...}, ...}.

        Args:
            fallback_dir (str): Directory containing static fallback data
            saved_dir (str): Directory to save or load downloaded data
            temp_dir (str): Temporary directory for when data is not needed after downloading
            save (bool): Whether to save downloaded data. Defaults to False.
            use_saved (bool): Whether to use saved data. Defaults to False.
            ignore (ListTuple[str]): Don't generate retrievers for these downloaders
            rate_limit (Optional[Dict]): Rate limiting per host. Defaults to {"calls": 1, "period": 0.1}
            **kwargs: See below and parameters of Download class in HDX Python Utilities
            header_auths (Mapping[str, str]): Header authorisations
            basic_auths (Mapping[str, str]): Basic authorisations
            extra_params (Mapping[str, str]): Extra parameter authorisations

        Returns:
            None
        """
        if rate_limit:
            kwargs["rate_limit"] = rate_limit
        custom_configs = dict()
        header_auths = kwargs.get("header_auths")
        if header_auths is not None:
            for name in header_auths:
                custom_configs[name] = {
                    "headers": {"Authorization": header_auths[name]}
                }
            del kwargs["header_auths"]
        basic_auths = kwargs.get("basic_auths")
        if basic_auths is not None:
            for name in basic_auths:
                custom_configs[name] = {"basic_auth": basic_auths[name]}
            del kwargs["basic_auths"]
        extra_params = kwargs.get("extra_params")
        if extra_params is not None:
            for name in extra_params:
                custom_configs[name] = {
                    "extra_params_dict": dict(parse_qsl(extra_params[name]))
                }
            del kwargs["extra_params"]
        Download.generate_downloaders(custom_configs, **kwargs)
        Retrieve.generate_retrievers(
            fallback_dir,
            saved_dir,
            temp_dir,
            save,
            use_saved,
            ignore,
        )

    def add_custom(
        self, scraper: BaseScraper, add_to_run: bool = False
    ) -> str:
        scraper_name = scraper.name
        self.scrapers[scraper_name] = scraper
        if scraper_name not in self.scraper_names:
            self.scraper_names.append(scraper_name)
        if (
            add_to_run
            and self.scrapers_to_run is not None
            and scraper_name not in self.scrapers_to_run
        ):
            self.scrapers_to_run.append(scraper_name)
        scraper.errors_on_exit = self.errors_on_exit
        return scraper_name

    def add_customs(
        self, scrapers: Iterable[BaseScraper], add_to_run: bool = False
    ) -> List[str]:
        scraper_names = list()
        for scraper in scrapers:
            scraper_names.append(self.add_custom(scraper, add_to_run))
        return scraper_names

    def add_configurable(
        self, name, datasetinfo, level, level_name=None, suffix=None
    ) -> str:
        if suffix:
            key = f"{name}{suffix}"
        else:
            key = name
        if key not in self.scraper_names:
            self.scraper_names.append(key)
        self.scrapers[key] = ConfigurableScraper(
            name,
            datasetinfo,
            level,
            self.countryiso3s,
            self.adminone,
            level_name,
            self.today,
            self.errors_on_exit,
        )
        return key

    def add_configurables(
        self, configuration, level, level_name=None, suffix=None
    ) -> List[str]:
        keys = list()
        for name in configuration:
            datasetinfo = configuration[name]
            keys.append(
                self.add_configurable(
                    name, datasetinfo, level, level_name, suffix
                )
            )
        return keys

    def prioritise_scrapers(self, scraper_names):
        for scraper_name in reversed(scraper_names):
            if scraper_name in self.scraper_names:
                self.scraper_names.remove(scraper_name)
                self.scraper_names.insert(0, scraper_name)

    def get_scraper_names(self):
        return self.scraper_names

    def get_scraper(self, name):
        return self.scrapers.get(name)

    def get_scraper_exception(self, name):
        scraper = self.get_scraper(name)
        if not scraper:
            raise ValueError(f"No such scraper {name}!")
        return scraper

    def add_instance_variables(self, name, **kwargs):
        scraper = self.get_scraper_exception(name)
        for key, value in kwargs.items():
            setattr(scraper, key, value)

    def add_post_run(self, name, fn):
        scraper = self.get_scraper_exception(name)
        scraper.post_run = lambda: fn(scraper)

    def run_one(self, name, run_again=False):
        scraper = self.get_scraper_exception(name)
        if scraper.has_run is False or run_again:
            try:
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

    def run_scraper(self, name, run_again=False):
        if self.scrapers_to_run and not any(
            x in name for x in self.scrapers_to_run
        ):
            return False
        logger.info(f"Running {name}")
        self.run_one(name, run_again)
        return True

    def run(
        self, what_to_run=None, run_again=False, prioritise_scrapers=tuple()
    ):
        self.prioritise_scrapers(prioritise_scrapers)
        for name in self.scraper_names:
            if what_to_run and name not in what_to_run:
                continue
            self.run_scraper(name, run_again)

    def set_not_run(self, name):
        self.get_scraper(name).has_run = False

    def set_not_run_many(self, names):
        for name in names:
            self.get_scraper(name).has_run = False

    def get_headers(self, names=None, levels=None, headers=None, hxltags=None):
        if not names:
            names = self.scrapers.keys()
        results = dict()
        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            for level, scraper_headers in scraper.headers.items():
                if levels is not None and level not in levels:
                    continue
                level_results = results.get(level)
                if level_results is None:
                    level_results = (list(), list())
                    results[level] = level_results
                for i, header in enumerate(scraper_headers[0]):
                    if headers is not None and header not in headers:
                        continue
                    hxltag = scraper_headers[1][i]
                    if hxltags is not None and hxltag not in hxltags:
                        continue
                    level_results[0].append(header)
                    level_results[1].append(hxltag)
        return results

    def get_results(
        self, names=None, levels=None, overrides=dict(), has_run=True
    ):
        if not names:
            names = self.scrapers.keys()
        results = dict()
        for name in names:
            if self.scrapers_to_run and not any(
                x in name for x in self.scrapers_to_run
            ):
                continue
            scraper = self.get_scraper(name)
            if has_run and not scraper.has_run:
                continue
            override = overrides.get(name, dict())
            for level, headers in scraper.headers.items():
                level_override = override.get(level)
                if level_override:
                    output_level = level_override
                else:
                    output_level = level
                if levels is not None and output_level not in levels:
                    continue
                level_results = results.get(level)
                if level_results is None:
                    level_results = {
                        "headers": (list(), list()),
                        "values": list(),
                        "sources": list(),
                        "fallbacks": list(),
                    }
                    results[output_level] = level_results
                level_results["headers"][0].extend(headers[0])
                level_results["headers"][1].extend(headers[1])
                level_results["values"].extend(scraper.get_values(level))
                level_results["sources"].extend(scraper.get_sources(level))
        return results

    def get_rows(
        self,
        level,
        adms,
        headers=(tuple(), tuple()),
        row_fns=tuple(),
        names=None,
        overrides=dict(),
    ):
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

    def get_sources(self, names=None, levels=None, additional_sources=tuple()):
        if not names:
            names = self.scrapers.keys()
        hxltags = set()
        sources = list()
        for sourceinfo in additional_sources:
            date = sourceinfo.get("source_date")
            if date is None:
                if sourceinfo.get("force_date_today", False):
                    date = self.today.strftime("%Y-%m-%d")
            source = sourceinfo.get("source")
            source_url = sourceinfo.get("source_url")
            dataset_name = sourceinfo.get("dataset")
            if dataset_name:
                dataset = Dataset.read_from_hdx(dataset_name)
                if date is None:
                    date = get_isodate_from_dataset_date(
                        dataset, today=self.today
                    )
                if source is None:
                    source = dataset["dataset_source"]
                if source_url is None:
                    source_url = dataset.get_hdx_url()
            sources.append((sourceinfo["indicator"], date, source, source_url))
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
            for level in levels_to_check:
                for source in scraper.get_sources(level):
                    hxltag = source[0]
                    if hxltag in hxltags:
                        continue
                    hxltags.add(hxltag)
                    sources.append(source)
        return sources

    def get_source_urls(self, names=None):
        source_urls = set()
        if not names:
            names = self.scrapers.keys()
        for name in names:
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
            source_urls.update(scraper.get_source_urls())
        return sorted(source_urls)
