import logging
from typing import Dict, List, Tuple

from hdx.utilities.loader import LoadError, load_json

logger = logging.getLogger(__name__)


class Fallbacks:
    """Provide fallbacks if data download fails"""

    fallbacks = None
    default_levels_mapping = {
        "global": "global_data",
        "regional": "regional_data",
        "national": "national_data",
        "subnational": "subnational_data",
    }
    default_admin_name_mapping = {
        "global": "value",
        "regional": "#region+name",
        "national": "#country+code",
        "subnational": "#adm1+code",
    }

    @classmethod
    def add(
        cls,
        fallbacks_path: str,
        levels_mapping: Dict[str, str] = default_levels_mapping,
        sources_key: str = "sources",
        admin_name_mapping: Dict[str, str] = default_admin_name_mapping,
    ) -> None:
        """
        Add fallbacks from a given JSON fallbacks_path (which will usually be the
        previous run). Map the keys in the file to levels, use the given sources_key
        for sources and use the given HXL hashtags for different admin levels. The
        defaults are:
            default_levels_mapping = {
                "global": "global_data",
                "regional": "regional_data",
                "national": "national_data",
                "subnational": "subnational_data",
            }
            default_admin_name_mapping = {
                "global": "value",
                "regional": "#region+name",
                "national": "#country+code",
                "subnational": "#adm1+code",
            }

        Args:
            fallbacks_path (str): Path to JSON fallbacks file
            levels_mapping (Dict[str,str]): Map keys from file to levels. Defaults in description.
            sources_key (str): Key to use for sources. Defaults to "sources".
            admin_name_mapping: HXL hashtags for different admin levels. Defaults in description.

        Returns:
            None
        """
        try:
            fallback_data = load_json(fallbacks_path)
            fallback_sources = fallback_data[sources_key]
            sources_hxltags = [
                "#indicator+name",
                "#date",
                "#meta+source",
                "#meta+url",
            ]
            cls.fallbacks = dict()
            for level, output_key in levels_mapping.items():
                cls.fallbacks[level] = {
                    "data": fallback_data[output_key],
                    "admin name": admin_name_mapping[level],
                    "sources": fallback_sources,
                    "sources hxltags": sources_hxltags,
                }
        except (IOError, LoadError):
            cls.fallbacks = None

    @classmethod
    def exist(cls) -> None:
        """
        Returns fallbacks if they exist

        Args:
            fallbacks_path (str): Path to JSON fallbacks file
            levels_mapping (Dict[str,str]): Map keys from file to levels. Defaults in description.
            sources_key (str): Key to use for sources. Defaults to "sources".
            admin_name_mapping: HXL hashtags for different admin levels. Defaults in description.

        Returns:
            None
        """
        return cls.fallbacks is not None

    @classmethod
    def get(
        cls,
        level,
        headers: Tuple[List, List],
    ) -> Tuple[List, List]:
        """Use provided fallbacks when there is a problem obtaining the latest data. The
        fallbacks dictionary should have the following keys: "data" containing a list of
        dictionaries from HXL hashtag to value, "admin name" to specify a particular admin
        name to use or "admin hxltag" specifying the HXL hashtag of the admin unit,
        "sources" containing a list of dictionaries with source information and
        "sources hxltags" containing a list of HXL hashtags with the name one first. eg.

        {"data": [{"#country+code": "AFG", "": "#value+wb+total": "572000000", ...}, ...],
        "admin hxltag": "#country+code",
        "sources": [{"#date": "2020-07-29", "#indicator+name": "#value+wb+total",
        "#meta+source": "OCHA, Center for Disaster Protection",
        "#meta+url": "https://data.humdata.org/dataset/compilation..."}, ...],
        "sources hxltags": ["#indicator+name", "#date", "#meta+source", "#meta+url"]}

        Args:
            headers (Tuple[List, List]): Headers

        Returns:
            Tuple[List, List]: Tuple of (Output values, output sources)
        """
        values = list()
        sources = list()

        if cls.fallbacks:
            fallbacks = cls.fallbacks[level]
            fb_data = fallbacks["data"]
            fb_adm_name = fallbacks.get("admin name", None)
            fb_adm_hxltag = fallbacks.get("admin hxltag", None)

            output_hxltags = headers[1]
            valdicts = [dict() for _ in output_hxltags]
            for row in fb_data:
                if fb_adm_name:
                    adm_key = fb_adm_name
                elif fb_adm_hxltag:
                    adm_key = fb_adm_hxltag
                else:
                    raise ValueError(
                        "Either admin name or admin hxltag must be specified!"
                    )
                if adm_key != "value":
                    adm_key = row[adm_key]
                for i, hxltag in enumerate(output_hxltags):
                    val = row.get(hxltag)
                    if val is not None:
                        valdicts[i][adm_key] = val
            values.extend(valdicts)
            fb_sources_hxltags = fallbacks["sources hxltags"]
            for row in fallbacks["sources"]:
                hxltag = row[fb_sources_hxltags[0]]
                if hxltag in output_hxltags:
                    sources.append(
                        (
                            hxltag,
                            row[fb_sources_hxltags[1]],
                            row[fb_sources_hxltags[2]],
                            row[fb_sources_hxltags[3]],
                        )
                    )
        return values, sources
