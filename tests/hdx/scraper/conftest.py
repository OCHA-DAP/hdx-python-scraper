"""Global fixtures"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities.fallbacks import Fallbacks

from . import bool_assert


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
    Fallbacks.add(join(fixtures, "fallbacks.json"), sources_key="sources")


def check_scrapers(
    names,
    runner,
    level_name,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
    source_urls=None,
):
    for name in names:
        scraper = runner.get_scraper(name)
        bool_assert(scraper.has_run, True, "Scraper has not run!")
        bool_assert(
            scraper.fallbacks_used,
            fallbacks_used,
            f"Fallbacks used {scraper.fallbacks_used} is not as expected!",
        )
    results = runner.get_results(names)[level_name]
    assert results["headers"] == headers
    assert results["values"] == values
    assert results["sources"] == sources
    if population_lookup is not None:
        assert BaseScraper.population_lookup == population_lookup
    if source_urls is not None:
        assert runner.get_source_urls() == source_urls


def check_scraper(
    name,
    runner,
    level_name,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
    source_urls=None,
):
    check_scrapers(
        (name,),
        runner,
        level_name,
        headers,
        values,
        sources,
        population_lookup,
        fallbacks_used,
        source_urls,
    )


def run_check_scraper(
    name,
    runner,
    level_name,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
    source_urls=None,
):
    runner.run_one(name)
    check_scraper(
        name,
        runner,
        level_name,
        headers,
        values,
        sources,
        population_lookup,
        fallbacks_used,
        source_urls,
    )
    runner.set_not_run(name)


def run_check_scrapers(
    names,
    runner,
    level_name,
    headers,
    values,
    sources,
    population_lookup=None,
    fallbacks_used=False,
    source_urls=None,
):
    runner.run(names)
    check_scrapers(
        names,
        runner,
        level_name,
        headers,
        values,
        sources,
        population_lookup,
        fallbacks_used,
        source_urls,
    )
    runner.set_not_run_many(names)
