import logging
from copy import copy
from typing import Dict, Optional, Type

from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.loader import load_yaml
from hdx.utilities.matching import get_code_from_name
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.text import normalise

logger = logging.getLogger(__name__)


class Lookup:
    """Lookup class. YAML input is in this form:
    https://github.com/OCHA-DAP/hdx-python-scraper/blob/main/src/hdx/scraper/framework/utilities/sector_configuration.yaml

    Args:
        yaml_config_path (str): YAML configuration file
        classobject (Type): Child class
    """

    def __init__(self, yaml_config_path: str, classobject: Type):
        configuration = load_yaml(script_dir_plus_file(yaml_config_path, classobject))
        self._configuration = configuration
        initial_lookup = configuration.get("initial_lookup", {})
        self._code_lookup = copy(initial_lookup)
        self._code_to_name = {}
        self._unmatched = []
        self.setup()

    def add_to_lookup(self, code: str, name: str) -> None:
        """Add code and name to lookup

        Args:
            code (str): Code to add to lookup
            name (str): Name to add to lookup

        Returns:
            None
        """

        self._code_lookup[name] = code
        self._code_lookup[code] = code
        self._code_lookup[normalise(name)] = code
        self._code_lookup[normalise(code)] = code
        self._code_to_name[code] = name

    def setup(self) -> None:
        """Setup lookup from YAML configuration

        Returns:
            None
        """

        log_message = self._configuration["log_message"]
        logger.info(f"Populating {log_message}")

        reader = Read.get_reader()
        datasetinfo = self._configuration["datasetinfo"]
        headers, iterator = reader.read(
            datasetinfo, file_prefix=self._configuration["file_prefix"]
        )
        code_key = self._configuration["code_key"]
        name_key = self._configuration["name_key"]
        for row in iterator:
            self.add_to_lookup(
                code=row[code_key],
                name=row[name_key],
            )

        extra_entries = self._configuration.get("extra_entries", {})
        for code, name in extra_entries.items():
            self.add_to_lookup(code=code, name=name)

    def get_code(self, code: str) -> Optional[str]:
        """Get code from lookup using fuzzy matching if needed

        Args:
            code (str): Code to get from lookup

        Returns:
            Optional[str]: Code obtained from lookup or None if no code is found
        """

        return get_code_from_name(
            name=code,
            code_lookup=self._code_lookup,
            unmatched=self._unmatched,
        )

    def get_name(self, code: str, default: Optional[str] = None) -> Optional[str]:
        """Get name from code

        Args:
            code (str): Code to lookup
            default (Optional[str]): Default name to return if code is not found

        Returns:
            Optional[str]: Name obtained from lookup or default if no name is found
        """

        return self._code_to_name.get(code, default)

    def get_code_to_name(self) -> Dict[str, str]:
        """Get the code to name lookup dictionary

        Returns:
            Dict[str, str]: Code to name lookup dictionary
        """

        return self._code_to_name
