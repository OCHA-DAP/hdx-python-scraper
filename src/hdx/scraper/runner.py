import logging

from hdx.utilities.downloader import Download

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

    def add_custom(self, scraper):
        self.scrapers[scraper.name] = scraper

    def add_configurable(self, name, datasetinfo, level):
        basic_auth = self.basic_auths.get(name)
        if basic_auth is None:
            int_downloader = self.downloader
        else:
            int_downloader = Download(
                basic_auth=basic_auth,
                rate_limit={"calls": 1, "period": 0.1},
            )
        self.scrapers[name] = ConfigurableScraper(
            name,
            datasetinfo,
            level,
            self.countryiso3s,
            self.adminone,
            int_downloader,
            self.today,
        )

    def add_configurables(self, configuration, level):
        for name in configuration:
            datasetinfo = configuration[name]
            self.add_configurable(name, datasetinfo, level)

    def get_scraper(self, name):
        return self.scrapers.get(name)

    def run_one(self, name):
        scraper = self.get_scraper(name)
        if not scraper:
            raise ValueError(f"No such scraper {name}!")
        try:
            scraper.run()
            scraper.add_sources()
            scraper.add_population()
            logger.info(f"Processed {scraper.name}")
        except Exception as ex:
            logger.exception(f"Using fallbacks for {scraper.name}!")
            if self.errors_on_exit:
                self.errors_on_exit.append(
                    f"Using fallbacks for {scraper.name}! Error: {ex}"
                )
            for level in scraper.headers.keys():
                values, sources = Fallbacks.get(level, scraper.headers[level])
                scraper.values[level] = values
                scraper.sources[level] = sources
            scraper.fallbacks_used = True
        scraper.has_run = True

    def run_scraper(self, name):
        if self.scrapers_to_run and not any(
            x in name for x in self.scrapers_to_run
        ):
            return False
        self.run_one(name)
        return True

    def run(self, what_to_run=None):
        for name in self.scrapers:
            if what_to_run and name not in what_to_run:
                continue
            logger.info(f"Running {name}")
            self.run_scraper(name)

    def set_not_run(self, name):
        self.get_scraper(name).has_run = False

    def set_not_run_many(self, names):
        for name in names:
            self.get_scraper(name).has_run = False

    def get_results(self, names=None):
        if not names:
            names = self.scrapers.keys()
        results = dict()
        for name in names:
            scraper = self.get_scraper(name)
            if not scraper.has_run:
                continue
            for level, headers in scraper.headers.items():
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
