"""Populate the sector mapping."""

import logging
from copy import copy
from typing import Dict, Optional

from .reader import Read
from hdx.utilities.loader import load_yaml
from hdx.utilities.matching import get_code_from_name
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.text import normalise

logger = logging.getLogger(__name__)


class Sector:
    def __init__(
        self,
        configuration: Optional[Dict] = None,
    ):
        if configuration is None:
            configuration = load_yaml(
                script_dir_plus_file("sector_configuration.yaml", Sector)
            )
        self._datasetinfo = configuration["sector"]
        self.data = copy(configuration["sector_map"])
        self.unmatched = []
        self.populate()

    def populate(self) -> None:
        logger.info("Populating sector mapping")

        def parse_sector_values(code: str, name: str):
            self.data[name] = code
            self.data[code] = code
            self.data[normalise(name)] = code
            self.data[normalise(code)] = code

        reader = Read.get_reader()
        headers, iterator = reader.read(
            self._datasetinfo, file_prefix="sector"
        )
        for row in iterator:
            parse_sector_values(
                code=row["#sector +code +acronym"],
                name=row["#sector +name +preferred +i_en"],
            )

        extra_entries = {
            "Cash": "Cash programming",
            "Hum": "Humanitarian assistance (unspecified)",
            "Multi": "Multi-sector (unspecified)",
            "Intersectoral": "Intersectoral",
        }
        for code, name in extra_entries.items():
            parse_sector_values(code=code, name=name)

    def get_sector_code(self, sector: str) -> str | None:
        return get_code_from_name(
            name=sector,
            code_lookup=self.data,
            unmatched=self.unmatched,
        )
