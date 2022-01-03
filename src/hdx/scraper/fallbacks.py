import logging
from typing import Dict, List

from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


def use_fallbacks(
    name: str,
    fallbacks: Dict,
    output_cols: List[str],
    output_hxltags: List[str],
    results: Dict,
) -> None:
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
        name (str): Name of mini scraper
        fallbacks (Dict): Fallbacks dictionary
        output_cols (List[str]): Names of output columns
        output_hxltags (List[str]): HXL hashtags of output columns
        results (Dict): Dictionary of output containing output headers, values and sources

    Returns:
        None
    """
    retheaders = results["headers"]
    retvalues = results["values"]

    fb_data = fallbacks["data"]
    fb_adm_name = fallbacks.get("admin name", None)
    fb_adm_hxltag = fallbacks.get("admin hxltag", None)

    retheaders[0].extend(output_cols)
    retheaders[1].extend(output_hxltags)

    valdicts = [dict() for _ in output_hxltags]
    for row in fb_data:
        if fb_adm_name:
            adm_key = fb_adm_name
        elif fb_adm_hxltag:
            adm_key = row[fb_adm_hxltag]
        else:
            raise ValueError(
                "Either admin name or admin hxltag must be specified!"
            )
        for i, hxltag in enumerate(output_hxltags):
            val = row.get(hxltag)
            if val is not None:
                valdicts[i][adm_key] = val
    retvalues.extend(valdicts)
    fb_sources_hxltags = fallbacks["sources hxltags"]
    for row in fallbacks["sources"]:
        hxltag = row[fb_sources_hxltags[0]]
        if hxltag in output_hxltags:
            results["sources"].append(
                (
                    hxltag,
                    row[fb_sources_hxltags[1]],
                    row[fb_sources_hxltags[2]],
                    row[fb_sources_hxltags[3]],
                )
            )
    logger.error(f"Used fallback data for {name}!")
    dict_of_lists_add(results, "fallbacks", name)
