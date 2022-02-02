import logging
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional

from hdx.location.adminone import AdminOne
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download

from hdx.scraper.scraper import Scraper
from hdx.scraper.utils import add_population, get_level

logger = logging.getLogger(__name__)


def run_scrapers(
    configuration: Dict,
    level: str,
    countryiso3s: List[str],
    adminone: AdminOne,
    downloader: Download,
    basic_auths: Dict[str, str] = dict(),
    today: datetime = datetime.now(),
    population_lookup: Optional[Dict[str, int]] = None,
    fallbacks: Optional[Dict] = None,
    scrapers: Optional[List[str]] = None,
    **kwargs: Any,
) -> Dict:
    """Runs all mini scrapers given in configuration and returns headers, values and sources.

    Args:
        configuration (Dict): Configuration for mini scrapers
        level (str): Can be global, national or subnational
        countryiso3s (List[str]): List of ISO3 country codes to process
        adminone (AdminOne): AdminOne object from HDX Python Country library that handles processing of admin level 1
        downloader (Download): Download object for downloading files
        basic_auths (Dict[str,str]): Dictionary of basic authentication information
        today (datetime): Value to use for today. Defaults to datetime.now().
        population_lookup (Optional[Dict[str,int]]): Admin code to population dict. Defaults to None.
        fallbacks (Optional[Dict]): Fallback data to use. Defaults to None.
        scrapers (Optional[List[str]])): List of mini scraper names to process
        **kwargs: Variables to use when evaluating template arguments in urls

    Returns:
        Dict: Dictionary of output containing output headers, values and sources
    """
    results = {
        "headers": (list(), list()),
        "values": list(),
        "sources": list(),
        "fallbacks": list(),
    }

    for name in configuration:
        if scrapers:
            if not any(scraper in name for scraper in scrapers):
                continue
        else:
            if name == "population":
                continue
        logger.info(f"Processing {name}")
        basic_auth = basic_auths.get(name)
        if basic_auth is None:
            int_downloader = downloader
        else:
            int_downloader = Download(
                basic_auth=basic_auth,
                rate_limit={"calls": 1, "period": 0.1},
            )
        datasetinfo = deepcopy(configuration[name])
        datasetinfo["name"] = name
        scraper = Scraper(
            datasetinfo,
            get_level(level),
            countryiso3s,
            adminone,
            int_downloader,
            today,
            population_lookup,
            fallbacks,
        )
        scraper_results = scraper.run(**kwargs)
        if basic_auth:
            int_downloader.close()
        if population_lookup is not None:
            add_population(population_lookup, scraper_results)
        headers = scraper_results["headers"]
        results["headers"][0].extend(headers[0])
        results["headers"][1].extend(headers[1])
        results["values"].extend(scraper_results["values"])
        results["sources"].extend(scraper_results["sources"])
        if scraper_results["fallbacks"]:
            dict_of_lists_add(results, "fallbacks", name)
    return results
