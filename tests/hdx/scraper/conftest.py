"""Global fixtures"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from hdx.scraper.base_scraper import BaseScraper
from hdx.scraper.utilities import string_params_to_dict
from hdx.scraper.utilities.fallbacks import Fallbacks
from hdx.scraper.utilities.reader import Read

from . import bool_assert


@pytest.fixture(scope="session")
def fixtures():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def configuration(fixtures):
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

    header_auths = "population:pop_12345,who_national:who_abc"
    basic_auths = "access:YWNjXzEyMzQ1OmFjY19hYmM=,who_national2:d2hvX2RlZjp3aG9fMTIzNDU="
    param_auths = (
        "sadd:user=sadd_123&pass=sadd_abc,ourworldindata:auth=owid_abc"
    )

    header_auths = string_params_to_dict(header_auths)
    basic_auths = string_params_to_dict(basic_auths)
    param_auths = string_params_to_dict(param_auths)
    today = parse_date("2020-10-01")
    save = False
    if save:
        Read.create_readers(
            "",
            join(fixtures, "tmp"),
            "",
            save=True,
            use_saved=False,
            user_agent="test",
            header_auths=header_auths,
            basic_auths=basic_auths,
            param_auths=param_auths,
            today=today,
        )
    else:
        Read.create_readers(
            "",
            fixtures,
            "",
            save=False,
            use_saved=True,
            user_agent="test",
            header_auths=header_auths,
            basic_auths=basic_auths,
            param_auths=param_auths,
            today=today,
        )

    return Configuration.read()


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
    set_not_run=True,
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
    if set_not_run:
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
    set_not_run=True,
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
    if set_not_run:
        runner.set_not_run_many(names)
