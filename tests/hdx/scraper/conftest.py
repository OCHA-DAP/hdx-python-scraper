"""Global fixtures"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country
from hdx.utilities.loader import load_json


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
def fallback_data():
    return load_json(join("tests", "fixtures", "fallbacks.json"))
