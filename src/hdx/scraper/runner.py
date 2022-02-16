import logging
from typing import Iterable, Type

from hdx.utilities.downloader import Download

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.configurable.scraper import ConfigurableScraper
from hdx.scraper.utilities.fallbacks import Fallbacks

logger = logging.getLogger(__name__)


class Runner:
    def __init__(
        self,
        countryiso3s,
        adminone,
        downloader,
        basic_auths,
        today,
        errors_on_exit=None,
        scrapers_to_run=None,
    ):
        self.countryiso3s = countryiso3s
        self.adminone = adminone
        self.downloader = downloader
        self.basic_auths = basic_auths
        self.today = today
        self.errors_on_exit = errors_on_exit
        self.scrapers_to_run = scrapers_to_run
        self.scrapers = dict()

    def add_custom(self, scraper: Type[BaseScraper]):
        self.scrapers[scraper.name] = scraper
        scraper.errors_on_exit = self.errors_on_exit

    def add_customs(self, scrapers: Iterable[Type[BaseScraper]]):
        for scraper in scrapers:
            self.add_custom(scraper)

    def add_configurable(self, name, datasetinfo, level, suffix=None):
        basic_auth = self.basic_auths.get(name)
        if basic_auth is None:
            int_downloader = self.downloader
        else:
            int_downloader = Download(
                basic_auth=basic_auth,
                rate_limit={"calls": 1, "period": 0.1},
            )
        if suffix:
            key = f"{name}{suffix}"
        else:
            key = name
        self.scrapers[key] = ConfigurableScraper(
            name,
            datasetinfo,
            level,
            self.countryiso3s,
            self.adminone,
            int_downloader,
            self.today,
            self.errors_on_exit,
        )
        return key

    def add_configurables(self, configuration, level, suffix=None):
        keys = list()
        for name in configuration:
            datasetinfo = configuration[name]
            keys.append(
                self.add_configurable(name, datasetinfo, level, suffix)
            )
        return keys

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
        self.run_one(name, run_again)
        return True

    def run(self, what_to_run=None, run_again=False):
        for name in self.scrapers:
            if what_to_run and name not in what_to_run:
                continue
            logger.info(f"Running {name}")
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
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
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

    def get_results(self, names=None, levels=None):
        if not names:
            names = self.scrapers.keys()
        results = dict()
        for name in names:
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
            for level, headers in scraper.headers.items():
                if levels is not None and level not in levels:
                    continue
                level_results = results.get(level)
                if level_results is None:
                    level_results = {
                        "headers": (list(), list()),
                        "values": list(),
                        "sources": list(),
                        "fallbacks": list(),
                    }
                    results[level] = level_results
                level_results["headers"][0].extend(headers[0])
                level_results["headers"][1].extend(headers[1])
                level_results["values"].extend(scraper.get_values(level))
                level_results["sources"].extend(scraper.get_sources(level))
        return results

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
