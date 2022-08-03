import logging
from typing import Dict

from hdx.utilities.dictandlist import dict_of_sets_add
from hdx.utilities.typehint import ListTuple

from .reader import Read

logger = logging.getLogger(__name__)


class RegionLookup:
    """Provide list of regions and mappings from country ISO3 code to region names."""

    iso3_to_region = dict()
    iso3_to_regions = dict()
    regions = None

    @classmethod
    def load(
        cls,
        region_config: Dict,
        countryiso3s: ListTuple[str],
        additional_regions: Dict[str, ListTuple] = dict(),
    ) -> None:
        """Read in region information and provide regions (list of regions) and
        iso3_to_region (one-to-one mapping from country ISO3 code to region name). It
        also provides iso3_to_regions which is a one-to-many mapping from country ISO3
        code to multiple region names. The possibility of multiple region names arises
        due to the addition of toplevel_region which is defined in the region_config
        and additional_regions which are of the form: {"HRPs": hrp_countries, ...}.
        Hence, a country can map to not only what is specified in the dataset given in
        region_config but also to toplevel_region (eg. GHO) which covers all countries
        given by countryiso3s and to one or more additional_regions.

        Args:
            region_config (Dict): Region configuration
            countryiso3s (ListTuple[str]): List of country ISO3 codes
            additional_regions (Dict[str, ListTuple]): Region to ISO3s. Defaults to dict().
        """
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
