from logging import Logger
from typing import Dict, List, Optional, Union

from hdx.location.adminlevel import AdminLevel
from hdx.utilities.typehint import ListTuple


def add_source_overwrite(
    hxltags: List[str],
    sources: List[ListTuple],
    source: ListTuple[str],
    logger: Logger,
):
    """Add source to sources preventing duplication

    Args:
        hxltags (List[str]): List of HXL hashtags, one for each source name
        sources (List[ListTuple]): List of sources
        source (ListTuple[str]): Source information
        logger (Logger): Logegr to log warnings to

    Returns:
        None
    """
    hxltag = source[0]
    if hxltag in hxltags:
        logger.warning(f"Overwriting source information for {hxltag}!")
        index = hxltags.index(hxltag)
        sources[index] = source
    else:
        hxltags.append(hxltag)
        sources.append(source)


def add_sources_overwrite(
    hxltags: List[str],
    sources: List[ListTuple],
    sources_to_add: List[ListTuple],
    logger: Logger,
):
    """Add source to sources preventing duplication

    Args:
        hxltags (List[str]): List of HXL hashtags, one for each source name
        sources (List[ListTuple]): List of sources
        sources_to_add (List[ListTuple]): List of sources to add
        logger (Logger): Logegr to log warnings to

    Returns:
        None
    """
    for source in sources_to_add:
        add_source_overwrite(hxltags, sources, source, logger)


def create_source_configuration(
    suffix_attribute: Optional[str] = None,
    admin_sources: bool = False,
    adminlevel: Union[AdminLevel, ListTuple[AdminLevel], None] = None,
    admin_mapping_dict: Optional[Dict] = None,
) -> Optional[Dict]:
    """Create source configuration. If none of the arguments are suppled, source
    configuration is None. suffix_attribute is an attribute to add to the end of
    source HXL hashtags. admin_sources defines whether the admin unit is added as an
    attribute (eg. a country iso3 code like +AFG). admin_level defines one or more
    AdminLevel objects that will be used to map admin pcodes to country iso3 codes. If
    admin_level is defined, admin_sources is assumed to be True. Alternatively,
    admin_mapping_dict can be supplied to define mapping from amin names to attribute
    suffixes.

    Args:
        suffix_attribute (Optional[str]): Suffix to add. Defaults to None.
        admin_sources (bool): Whether source information is per admin unit. Defaults to False.
        adminlevel (Union[AdminLevel, ListTuple[AdminLevel], None]): Admin level(s) mapping. Defaults to None.
        admin_mapping_dict (Optional[Dict]): Admin unit mapping to use. Defaults to None.

    Returns:
         Optional[Dict]: Source configuration dictionary
    """
    source_configuration = dict()
    if suffix_attribute:
        source_configuration["suffix_attribute"] = suffix_attribute
        return source_configuration
    admin_mapping = None
    if adminlevel:
        if isinstance(adminlevel, AdminLevel):
            admin_mapping = adminlevel.pcode_to_iso3
        else:
            admin_mapping = dict()
            for admlevel in adminlevel:
                admin_mapping.update(admlevel.pcode_to_iso3)
    elif admin_mapping_dict:
        admin_mapping = admin_mapping_dict
    if not admin_sources and not admin_mapping:
        return None
    source_configuration["admin_sources"] = True
    if admin_mapping:
        source_configuration["admin_mapping"] = admin_mapping
    return source_configuration
