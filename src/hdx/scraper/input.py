from typing import Any, Dict, Optional
from urllib.parse import parse_qsl

from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve
from hdx.utilities.typehint import ListTuple


def create_retrievers(
    fallback_dir: str,
    saved_dir: str,
    temp_dir: str,
    save: bool = False,
    use_saved: bool = False,
    ignore: ListTuple[str] = tuple(),
    rate_limit: Optional[Dict] = {"calls": 1, "period": 0.1},
    **kwargs: Any,
):
    """Generate a default retriever. Additional retrievers are generated if any of
    header_auths, basic_auths or extra_params are populated. header_auths and
    basic_auths are dictionaries of form {"scraper name": "auth", ...}. extra_params
    is of form {"scraper name": {"key": "auth", ...}, ...}.

    Args:
        fallback_dir (str): Directory containing static fallback data
        saved_dir (str): Directory to save or load downloaded data
        temp_dir (str): Temporary directory for when data is not needed after downloading
        save (bool): Whether to save downloaded data. Defaults to False.
        use_saved (bool): Whether to use saved data. Defaults to False.
        ignore (ListTuple[str]): Don't generate retrievers for these downloaders
        rate_limit (Optional[Dict]): Rate limiting per host. Defaults to {"calls": 1, "period": 0.1}
        **kwargs: See below and parameters of Download class in HDX Python Utilities
        header_auths (Mapping[str, str]): Header authorisations
        basic_auths (Mapping[str, str]): Basic authorisations
        param_auths (Mapping[str, str]): Extra parameter authorisations

    Returns:
        None
    """
    if rate_limit:
        kwargs["rate_limit"] = rate_limit
    custom_configs = dict()
    header_auths = kwargs.get("header_auths")
    if header_auths is not None:
        for name in header_auths:
            custom_configs[name] = {
                "headers": {"Authorization": header_auths[name]}
            }
        del kwargs["header_auths"]
    basic_auths = kwargs.get("basic_auths")
    if basic_auths is not None:
        for name in basic_auths:
            custom_configs[name] = {"basic_auth": basic_auths[name]}
        del kwargs["basic_auths"]
    param_auths = kwargs.get("param_auths")
    if param_auths is not None:
        for name in param_auths:
            custom_configs[name] = {
                "extra_params_dict": dict(parse_qsl(param_auths[name]))
            }
        del kwargs["param_auths"]
    Download.generate_downloaders(custom_configs, **kwargs)
    Retrieve.generate_retrievers(
        fallback_dir,
        saved_dir,
        temp_dir,
        save,
        use_saved,
        ignore,
    )
