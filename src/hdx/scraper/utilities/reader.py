import logging
from datetime import datetime
from os.path import join
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qsl

import hxl
from hxl.input import InputOptions, munge_url
from slugify import slugify

from . import get_startend_dates_from_reference_period, match_template
from .sources import Sources
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_json
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


class Read(Retrieve):
    """Read data from tabular source eg. csv, xls, xlsx

    Args:
        downloader (Download): Download object
        fallback_dir (str): Directory containing static fallback data
        saved_dir (str): Directory to save or load downloaded data
        temp_dir (str): Temporary directory for when data is not needed after downloading
        save (bool): Whether to save downloaded data. Defaults to False.
        use_saved (bool): Whether to use saved data. Defaults to False.
        prefix (str): Prefix to add to filenames. Defaults to "".
        delete (bool): Whether to delete saved_dir if save is True. Defaults to True.
        today (Optional[datetime]): Value to use for today. Defaults to None (datetime.utcnow).
    """

    def __init__(
        self,
        downloader: Download,
        fallback_dir: str,
        saved_dir: str,
        temp_dir: str,
        save: bool = False,
        use_saved: bool = False,
        prefix: str = "",
        delete: bool = True,
        today: Optional[datetime] = None,
    ):
        super().__init__(
            downloader,
            fallback_dir,
            saved_dir,
            temp_dir,
            save,
            use_saved,
            prefix,
            delete,
        )
        self.today: Optional[datetime] = today

    @classmethod
    def create_readers(
        cls,
        fallback_dir: str,
        saved_dir: str,
        temp_dir: str,
        save: bool = False,
        use_saved: bool = False,
        ignore: ListTuple[str] = tuple(),
        rate_limit: Optional[Dict] = {"calls": 1, "period": 0.1},
        today: Optional[datetime] = None,
        **kwargs: Any,
    ):
        """Generate a default reader and an HDX reader. Additional readers are generated
        if any of header_auths, basic_auths or extra_params are populated. header_auths
        and basic_auths are dictionaries of form {"scraper name": "auth", ...}.
        extra_params is of form {"scraper name": {"key": "auth", ...}, ...}.

        Args:
            fallback_dir (str): Directory containing static fallback data
            saved_dir (str): Directory to save or load downloaded data
            temp_dir (str): Temporary directory for when data is not needed after downloading
            save (bool): Whether to save downloaded data. Defaults to False.
            use_saved (bool): Whether to use saved data. Defaults to False.
            ignore (ListTuple[str]): Don't generate retrievers for these downloaders
            rate_limit (Optional[Dict]): Rate limiting per host. Defaults to {"calls": 1, "period": 0.1}
            today (Optional[datetime]): Value to use for today. Defaults to None (datetime.utcnow).
            **kwargs: See below and parameters of Download class in HDX Python Utilities
            hdx_auth (str): HDX API key
            header_auths (Mapping[str, str]): Header authorisations
            basic_auths (Mapping[str, str]): Basic authorisations
            param_auths (Mapping[str, str]): Extra parameter authorisations

        Returns:
            None
        """
        if rate_limit:
            kwargs["rate_limit"] = rate_limit
        custom_configs = {}
        hdx_auth = kwargs.get("hdx_auth")
        if hdx_auth:
            custom_configs["hdx"] = {"headers": {"Authorization": hdx_auth}}
            del kwargs["hdx_auth"]
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
        cls.generate_retrievers(
            fallback_dir,
            saved_dir,
            temp_dir,
            save,
            use_saved,
            ignore,
            today=today,
        )

    @classmethod
    def get_reader(cls, name: Optional[str] = None) -> "Read":
        """Get a generated reader given a name. If name is not supplied, the default
        one will be returned.

        Args:
            name (Optional[str]): Name of reader. Defaults to None (get default).

        Returns:
            Retriever: Reader object
        """
        return cls.get_retriever(name)

    @staticmethod
    def get_url(url: str, **kwargs: Any) -> str:
        """Get url from a string replacing any template arguments

        Args:
            url (str): Url to read
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            str: Url with any template arguments replaced
        """
        for kwarg in kwargs:
            exec(f'{kwarg}="{kwargs[kwarg]}"')
        template_string, match_string = match_template(url)
        if template_string:
            replace_string = eval(match_string)
            url = url.replace(template_string, replace_string)
        return url

    def clone(self, downloader: Download) -> "Read":
        """Clone a given reader but use the given downloader

        Args:
            downloader (Download): Downloader to use

        Returns:
            Read: Cloned reader

        """
        return Read(
            downloader,
            fallback_dir=self.fallback_dir,
            saved_dir=self.saved_dir,
            temp_dir=self.temp_dir,
            save=self.save,
            use_saved=self.use_saved,
            prefix=self.prefix,
            delete=False,
            today=self.today,
        )

    def read_tabular(
        self, datasetinfo: Dict, **kwargs: Any
    ) -> Tuple[List[str], Iterator[Dict]]:
        """Read data from tabular source eg. csv, xls, xlsx

        Args:
            datasetinfo (Dict): Dictionary of information about dataset
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
        """
        sheet = datasetinfo.get("sheet")
        headers = datasetinfo.get("headers")
        if headers is None:
            headers = 1
            datasetinfo["headers"] = 1
        kwargs["headers"] = headers
        if isinstance(headers, list):
            kwargs["fill_merged_cells"] = True
        format = datasetinfo["format"]
        kwargs["format"] = format
        if not sheet and format in ("xls", "xlsx"):
            sheet = 1
        if sheet:
            kwargs["sheet"] = sheet
        compression = datasetinfo.get("compression")
        if compression:
            kwargs["compression"] = compression
        url = datasetinfo["url"]
        if isinstance(url, list):
            url = [self.get_url(x, **kwargs) for x in url]
        return self.get_tabular_rows(
            url,
            dict_form=True,
            has_hxl=datasetinfo.get("use_hxl", False),
            **kwargs,
        )

    def read_dataset(self, dataset_name: str) -> Optional[Dataset]:
        """Read HDX dataset

        Args:
            dataset_name (str): Dataset name

        Returns:
            Optional[Dataset]: The dataset that was read or None
        """
        saved_path = join(self.saved_dir, f"{dataset_name}.json")
        if self.use_saved:
            logger.info(f"Using saved dataset {dataset_name} in {saved_path}")
            dataset = Dataset.load_from_json(saved_path)
        else:
            dataset = Dataset.read_from_hdx(dataset_name)
            if self.save:
                logger.info(f"Saving dataset {dataset_name} in {saved_path}")
                if dataset is None:
                    save_json(None, saved_path)
                else:
                    dataset.save_to_json(saved_path, follow_urls=True)
        return dataset

    def download_resource(
        self, identifier: str, resource: Resource
    ) -> Tuple[str, str]:
        """Download HDX resource os a file and return the url downloaded and the path
        of the file. The identifier is information to identify what called
        this function and is used to prefix the filename of the file.

        Args:
            identifier (str): Information to identify caller
            resource (Resource): HDX resource

        Returns:
            Tuple[str, str]: (URL that was downloaded, path to downloaded file)
        """
        filename = f"{identifier}_{resource['name'].lower()}"
        file_type = f".{resource.get_file_type()}"
        if filename.endswith(file_type):
            filename = filename[: -len(file_type)]
        filename = f"{slugify(filename, separator='_')}{file_type}"
        url = munge_url(resource["url"], InputOptions())
        path = self.download_file(url, filename=filename)
        return url, path

    def read_hxl_resource(
        self, identifier: str, resource: Resource, data_type: str
    ) -> Optional[hxl.Dataset]:
        """Read HDX resource as a HXL dataset. The identifier is information to identify
        what called this function and is used to prefix the filename of the file and for
        logging.

        Args:
            identifier (str): Information to identify caller
            resource (Resource): HDX resource
            data_type (str): Description of the type of data for logging

        Returns:
            Optional[hxl.Dataset]: HXL dataset or None
        """
        try:
            _, path = self.download_resource(identifier, resource)
            data = hxl.data(path, InputOptions(allow_local=True)).cache()
            data.display_tags
            return data
        except hxl.HXLException:
            logger.warning(
                f"Could not process {data_type} for {identifier}. Maybe there are no HXL tags?"
            )
            return None
        except Exception:
            logger.exception(f"Error reading {data_type} for {identifier}!")
            raise

    def get_hapi_dataset_metadata(self, dataset: Dataset) -> Dict:
        """Get HAPI dataset metadata from HDX dataset

        Args:
            dataset (Dataset): HDX dataset

        Returns:
            Dict: HAPI dataset metadata
        """
        return {
            "hdx_id": dataset["id"],
            "hdx_stub": dataset["name"],
            "title": dataset["title"],
            "hdx_provider_stub": dataset["organization"]["name"],
            "hdx_provider_name": dataset["organization"]["title"],
            "reference_period": dataset.get_reference_period(today=self.today),
        }

    @staticmethod
    def get_hapi_resource_metadata(resource: Resource) -> Dict:
        """Get HAPI resource metadata from HDX resource

        Args:
            resource (Resource): HDX dataset

        Returns:
            Dict: HAPI resource metadata
        """
        return {
            "hdx_id": resource["id"],
            "name": resource["name"],
            "format": resource["format"],
            "update_date": parse_date(resource["last_modified"]),
            "download_url": resource["url"],
        }

    def read_hdx_metadata(
        self, datasetinfo: Dict, do_resource_check: bool = True
    ) -> Optional[Resource]:
        """Read metadata from HDX dataset and add to input dictionary. If url
        is not supplied, will look through resources for one that matches
        specified format and use its url unless do_resource_check is False.
        The dataset key of the parameter datasetinfo will usually point to a
        string (single dataset) but where sources vary across HXL tags can be
        a dictionary that maps from HXL tags to datasets with the key
        default_dataset setting a default for HXL tags. For a single dataset,
        the keys hapi_dataset_metadata and hapi_resource_metadata will be
        populated with more detailed dataset and resource information required
        by HAPI.

        Args:
            datasetinfo (Dict): Dictionary of information about dataset
            do_resource_check (bool): Whether to check resources. Defaults to False.

        Returns:
            Optional[Resource]: The resource if a url was not given
        """
        dataset_nameinfo = datasetinfo["dataset"]
        if isinstance(dataset_nameinfo, str):
            dataset = self.read_dataset(dataset_nameinfo)
            datasetinfo[
                "hapi_dataset_metadata"
            ] = self.get_hapi_dataset_metadata(dataset)
            resource = None
            url = datasetinfo.get("url")
            if do_resource_check and not url:
                resource_name = datasetinfo.get("resource")
                format = datasetinfo["format"].lower()
                for resource in dataset.get_resources():
                    if resource["format"].lower() == format:
                        if resource_name and resource["name"] != resource_name:
                            continue
                        url = resource["url"]
                        datasetinfo[
                            "hapi_resource_metadata"
                        ] = self.get_hapi_resource_metadata(resource)
                        break
                if not url:
                    raise ValueError(
                        f"Cannot find {format} resource in {dataset_nameinfo}!"
                    )
                datasetinfo["url"] = url
            if "source_date" not in datasetinfo:
                datasetinfo[
                    "source_date"
                ] = get_startend_dates_from_reference_period(
                    dataset, today=self.today
                )
            if "source" not in datasetinfo:
                datasetinfo["source"] = dataset["dataset_source"]
            if "source_url" not in datasetinfo:
                datasetinfo["source_url"] = dataset.get_hdx_url()
            Sources.standardise_datasetinfo_source_date(datasetinfo)
            return resource

        if "source_date" not in datasetinfo:
            source_date = {}
        else:
            source_date = None
        if "source" not in datasetinfo:
            source = {}
        else:
            source = None
        if "source_url" not in datasetinfo:
            source_url = {}
        else:
            source_url = None
        datasets = {}
        for hxltag, dataset_name in dataset_nameinfo.items():
            dataset = datasets.get(dataset_name)
            if not dataset:
                dataset = self.read_dataset(dataset_name)
                datasets[dataset_name] = dataset
            if source_date is not None:
                if hxltag == "default_dataset":
                    key = "default_date"
                else:
                    key = hxltag
                source_date[key] = get_startend_dates_from_reference_period(
                    dataset, today=self.today
                )
            if source is not None:
                if hxltag == "default_dataset":
                    key = "default_source"
                else:
                    key = hxltag
                source[key] = dataset["dataset_source"]
            if source_url is not None:
                if hxltag == "default_dataset":
                    key = "default_url"
                else:
                    key = hxltag
                source_url[key] = dataset.get_hdx_url()
        if source_date is not None:
            datasetinfo["source_date"] = source_date
        if source is not None:
            datasetinfo["source"] = source
        if source_url is not None:
            datasetinfo["source_url"] = source_url
        Sources.standardise_datasetinfo_source_date(datasetinfo)
        return None

    def read_hdx(
        self,
        datasetinfo: Dict,
        **kwargs: Any,
    ) -> Tuple[List[str], Iterator[Dict]]:
        """Read data and metadata from HDX dataset

        Args:
            datasetinfo (Dict): Dictionary of information about dataset
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
        """
        self.read_hdx_metadata(datasetinfo)
        return self.read_tabular(datasetinfo, **kwargs)

    def read(
        self,
        datasetinfo: Dict,
        **kwargs: Any,
    ) -> Tuple[List[str], Iterator[Dict]]:
        """Read data and metadata from HDX dataset

        Args:
            datasetinfo (Dict): Dictionary of information about dataset
            **kwargs: Variables to use when evaluating template arguments in urls

        Returns:
            Tuple[List[str],Iterator[Dict]]: Tuple (headers, iterator where each row is a dictionary)
        """
        format = datasetinfo["format"]
        if format in ["json", "csv", "xls", "xlsx"]:
            if "dataset" in datasetinfo:
                headers, iterator = self.read_hdx(datasetinfo, **kwargs)
            else:
                headers, iterator = self.read_tabular(datasetinfo, **kwargs)
        else:
            raise ValueError(
                f"Invalid format {format} for {datasetinfo['name']}!"
            )
        return headers, iterator
