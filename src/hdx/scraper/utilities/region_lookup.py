import logging
from typing import Dict

from hdx.utilities.dictandlist import dict_of_sets_add
from hdx.utilities.typehint import ListTuple

from .reader import Read

logger = logging.getLogger(__name__)


class RegionLookup:
    iso3_to_region = dict()
    iso3_to_regions = dict()
    regions = None

    @classmethod
    def load(
        cls,
        region_config: Dict,
        countryiso3s: ListTuple[str],
        additional_regions: Dict[str, ListTuple],
    ):
        _, iterator = Read.get_reader().read_hdx(
            region_config,
            file_prefix="regions",
        )
        ignore_regions = region_config.get("ignore", tuple())
        for region in additional_regions:
            if region in ignore_regions:
                continue
            cls.iso3_to_regions[region] = dict()
        regions = set()
        toplevel_region = region_config.get("toplevel_region")
        if toplevel_region:
            cls.iso3_to_regions[toplevel_region] = dict()
        for row in iterator:
            countryiso3 = row[region_config["iso3_header"]]
            if countryiso3 and countryiso3 in countryiso3s:
                region = row[region_config["region_header"]]
                if region in ignore_regions:
                    continue
                regions.add(region)
                cls.iso3_to_region[countryiso3] = region
                if toplevel_region:
                    dict_of_sets_add(
                        cls.iso3_to_regions[toplevel_region],
                        countryiso3,
                        region,
                    )
                for additional_region in additional_regions:
                    if additional_region in ignore_regions:
                        continue
                    if countryiso3 in additional_regions[additional_region]:
                        dict_of_sets_add(
                            cls.iso3_to_regions[additional_region],
                            countryiso3,
                            region,
                        )
        cls.regions = sorted(list(regions))
        for additional_region in additional_regions:
            if additional_region in ignore_regions:
                continue
            cls.regions.insert(0, additional_region)
            for countryiso3 in additional_regions[additional_region]:
                for region in cls.iso3_to_regions:
                    dict_of_sets_add(
                        cls.iso3_to_regions[region],
                        countryiso3,
                        additional_region,
                    )
        cls.regions.insert(0, toplevel_region)
        for countryiso3 in countryiso3s:
            for region in cls.iso3_to_regions:
                dict_of_sets_add(
                    cls.iso3_to_regions[region], countryiso3, toplevel_region
                )
