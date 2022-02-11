"""Global fixtures"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country
from hdx.utilities.loader import load_json

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.fallbacks import Fallbacks


@pytest.fixture(scope="session")
def configuration():
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        user_agent="test",
        project_config_yaml=join(
            "tests", "config", "project_configuration.yml"
        ),
    )
    Locations.set_validlocations(
        [
            {"name": "afg", "title": "Afghanistan"},
            {"name": "phl", "title": "Philippines"},
            {"name": "pse", "title": "State of Palestine"},
        ]
    )
    Country.countriesdata(use_live=False)
    return Configuration.read()


@pytest.fixture(scope="session")
def fixtures():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def fallbacks(fixtures):
    Fallbacks.add(join(fixtures, "fallbacks.json"))


def check_helper(runner, level, headers, values, sources, population_lookup):
    results = runner.get_results()[level]
    assert results["headers"] == headers
    assert results["values"] == values
    assert results["sources"] == sources
    if population_lookup:
        assert BaseScraper.population_lookup == population_lookup


def check_scraper(
    name,
    runner,
    level,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
):
    runner.run_one(name)
    check_helper(runner, level, headers, values, sources, population_lookup)
    scraper = runner.get_scraper(name)
    assert scraper.has_run is True
    assert scraper.fallbacks_used is fallbacks_used
    runner.set_not_run(name)


def check_scrapers(
    names,
    runner,
    level,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
):
    runner.run(names)
    check_helper(runner, level, headers, values, sources, population_lookup)
    for name in names:
        scraper = runner.get_scraper(name)
        assert scraper.has_run is True
        assert scraper.fallbacks_used is fallbacks_used
    runner.set_not_run_many(names)
