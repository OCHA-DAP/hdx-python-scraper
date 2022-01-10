import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def use_fallbacks(
    fallbacks: Dict,
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
        fallbacks (Dict): Fallbacks dictionary
        output_cols (List[str]): Names of output columns
        output_hxltags (List[str]): HXL hashtags of output columns

    Returns:
        Tuple[List, List]: Tuple of (Output values, output sources)
    """
    values = list()
    sources = list()

    fb_data = fallbacks["data"]
    fb_adm_name = fallbacks.get("admin name", None)
    fb_adm_hxltag = fallbacks.get("admin hxltag", None)

    output_hxltags = headers[1]
    valdicts = [dict() for _ in output_hxltags]
    for row in fb_data:
        if fb_adm_name:
            adm_key = fb_adm_name
        elif fb_adm_hxltag:
            if fb_adm_hxltag == "global":
                adm_key = "global"
            else:
                adm_key = row[fb_adm_hxltag]
        else:
            raise ValueError(
                "Either admin name or admin hxltag must be specified!"
            )
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
