import glob
import logging
from collections.abc import Iterator, Sequence
from datetime import datetime
from os.path import join
from typing import Any
from urllib.parse import parse_qsl

import hxl
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_json
from hxl.input import InputOptions, munge_url
from slugify import slugify

from . import get_startend_dates_from_time_period, match_template
from .sources import Sources

logger = logging.getLogger(__name__)


class Read(Retrieve):
    """Read data from tabular source eg. csv, xls, xlsx

    Args:
        downloader: Download object
        fallback_dir: Directory containing static fallback data
        saved_dir: Directory to save or load downloaded data
        temp_dir: Temporary directory for when data is not needed after downloading
        save: Whether to save downloaded data. Default is False.
        use_saved: Whether to use saved data. Default is False.
        prefix: Prefix to add to filenames. Default is "".
        delete: Whether to delete saved_dir if save is True. Default is True.
        today: Value to use for today. Default is None (datetime.utcnow).
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
        today: datetime | None = None,
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
        self.today: datetime | None = today

    @classmethod
    def create_readers(
        cls,
        fallback_dir: str,
        saved_dir: str,
        temp_dir: str,
        save: bool = False,
        use_saved: bool = False,
        ignore: Sequence[str] = tuple(),
        rate_limit: dict | None = {"calls": 1, "period": 0.1},
        today: datetime | None = None,
        **kwargs: Any,
    ):
        """Generate a default reader and an HDX reader. Additional readers are generated
        if any of header_auths, basic_auths or extra_params are populated. header_auths
        and basic_auths are dictionaries of form {"scraper name": "auth", ...}.
        extra_params is of form {"scraper name": {"key": "auth", ...}, ...}.

        Args:
            fallback_dir: Directory containing static fallback data
            saved_dir: Directory to save or load downloaded data
            temp_dir: Temporary directory for when data is not needed after downloading
            save: Whether to save downloaded data. Default is False.
            use_saved: Whether to use saved data. Default is False.
            ignore: Don't generate retrievers for these downloaders
            rate_limit: Rate limiting per host. Default is {"calls": 1, "period": 0.1}
            today: Value to use for today. Default is None (datetime.utcnow).
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
        bearer_tokens = kwargs.get("bearer_tokens")
        if bearer_tokens is not None:
            for name in bearer_tokens:
                custom_configs[name] = {"bearer_token": bearer_tokens[name]}
            del kwargs["bearer_tokens"]
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
    def get_reader(cls, name: str | None = None) -> "Read":
        """Get a generated reader given a name. If name is not supplied, the default
        one will be returned.

        Args:
            name: Name of reader. Default is None (get default).

        Returns:
            Reader object
        """
        return cls.get_retriever(name)

    @staticmethod
    def get_url(url: str, **kwargs: Any) -> str:
        """Get url from a string replacing any template arguments

        Args:
            url: Url to read
            **kwargs: Variables to use when evaluating template arguments

        Returns:
            Url with any template arguments replaced
        """
        for kwarg in kwargs:
            globals()[kwarg] = kwargs[kwarg]
        template_string, match_string = match_template(url)
        if template_string:
            replace_string = eval(match_string)
            url = url.replace(template_string, replace_string)
        return url

    def clone(self, downloader: Download) -> "Read":
        """Clone a given reader but use the given downloader

        Args:
            downloader: Downloader to use

        Returns:
            Cloned reader

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

    def setup_tabular(self, datasetinfo: dict, kwargs: dict) -> str | list:
        """Setup kwargs for tabular source eg. csv, xls, xlsx from
        datasetinfo and return url.

        Args:
            datasetinfo: Dictionary of information about dataset
            kwargs: Parameters to pass to download_file call

        Returns:
            url or list of urls
        """
        sheet = datasetinfo.get("sheet")
        headers = datasetinfo.get("headers")
        if headers is None:
            headers = 1
            datasetinfo["headers"] = 1
        format = datasetinfo["format"]
        kwargs["format"] = format
        if format in ("xls", "xlsx"):
            if not sheet:
                sheet = 1
            if isinstance(headers, list):
                kwargs["fill_merged_cells"] = True
            elif "fill_merged_cells" not in kwargs:
                kwargs["fill_merged_cells"] = False
            kwargs["xlsx2csv"] = datasetinfo.get("xlsx2csv", False)
        if sheet:
            kwargs["sheet"] = sheet
        kwargs["headers"] = headers
        compression = datasetinfo.get("compression")
        if compression:
            kwargs["compression"] = compression
        url = datasetinfo["url"]
        if isinstance(url, list):
            url = [self.get_url(x, **kwargs) for x in url]
        filename = kwargs.get("filename")
        if not filename:
            filename = datasetinfo.get("filename")
            if filename:
                kwargs["filename"] = filename
        if filename:
            # remove file_prefix if filename provided
            kwargs.pop("file_prefix", None)
        elif "file_prefix" not in kwargs:
            file_prefix = datasetinfo.get("file_prefix")
            if file_prefix:
                kwargs["file_prefix"] = file_prefix
        return url

    def read_tabular(
        self, datasetinfo: dict, **kwargs: Any
    ) -> tuple[list[str], Iterator[dict]]:
        """Read data from tabular source eg. csv, xls, xlsx

        Args:
            datasetinfo: Dictionary of information about dataset
            **kwargs: Parameters to pass to download_file call

        Returns:
            Tuple (headers, iterator where each row is a dictionary)
        """
        url = self.setup_tabular(datasetinfo, kwargs)
        return self.get_tabular_rows(
            url,
            dict_form=True,
            has_hxl=datasetinfo.get("use_hxl", False),
            **kwargs,
        )

    def read_dataset(
        self, dataset_name: str, configuration: Configuration | None = None
    ) -> Dataset | None:
        """Read HDX dataset

        Args:
            dataset_name: Dataset name
            configuration: HDX configuration. Default is global configuration.

        Returns:
            The dataset that was read or None
        """
        saved_path = join(self.saved_dir, f"{dataset_name}.json")
        if self.use_saved:
            logger.info(f"Using saved dataset {dataset_name} in {saved_path}")
            dataset = Dataset.load_from_json(saved_path)
        else:
            dataset = Dataset.read_from_hdx(dataset_name, configuration)
            if self.save:
                logger.info(f"Saving dataset {dataset_name} in {saved_path}")
                if dataset is None:
                    save_json(None, saved_path)
                else:
                    dataset.save_to_json(saved_path, follow_urls=True)
        return dataset

    def search_datasets(
        self,
        filename: str,
        query: str | None = "*:*",
        configuration: Configuration | None = None,
        page_size: int = 1000,
        **kwargs: Any,
    ) -> list[Dataset]:
        """Read HDX dataset

        Args:
            filename: Filename for saved files. Will be prefixed by underscore and a number.
            query: Query (in Solr format). Default is '*:*'.
            configuration: HDX configuration. Default is global configuration.
            page_size: Size of page to return. Default is 1000.
            **kwargs: See below
            fq (string): Any filter queries to apply
            rows (int): Number of matching rows to return. Default is all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            sort (string): Sorting of results. Default is 'relevance asc, metadata_modified desc' if rows<=page_size or 'metadata_modified asc' if rows>page_size.
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Default is 50.
            facet.field (list[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Default is False.

        Returns:
            list of datasets resulting from query
        """

        saved_path = join(self.saved_dir, filename)
        if self.use_saved:
            logger.info(
                f"Using saved datasets in {filename}_n.json in {self.saved_dir}"
            )
            datasets = []
            for file_path in sorted(glob.glob(f"{saved_path}_*.json")):
                datasets.append(Dataset.load_from_json(file_path))
        else:
            datasets = Dataset.search_in_hdx(query, configuration, page_size, **kwargs)
            if self.save:
                for i, dataset in enumerate(datasets):
                    file_path = f"{saved_path}_{i}.json"
                    name = dataset["name"]
                    logger.info(f"Saving dataset {name} in {file_path}")
                    dataset.save_to_json(file_path, follow_urls=True)
        return datasets

    @staticmethod
    def construct_filename(name: str, format: str):
        """Construct filename from name and format. The filename of the file
        comes from the name and format.

        Args:
            name: Name for the download
            format: Format of download

        Returns:
            Filename of file
        """
        filename = name.lower()
        file_type = f".{format}"
        if filename.endswith(file_type):
            filename = filename[: -len(file_type)]
        return f"{slugify(filename, separator='_')}{file_type}"

    def construct_filename_and_download(
        self, name: str, format: str, url: str, **kwargs: Any
    ) -> tuple[str, str]:
        """Construct filename, download file and return the url downloaded and
        the path of the file. The filename of the file comes from the name and
        format.

        Args:
            name: Name for the download
            format: Format of download
            url: URL of download
            **kwargs: Parameters to pass to download_file call

        Returns:
            (URL that was downloaded, path to downloaded file)
        """
        filename = kwargs.get("filename")
        if not filename:
            kwargs["filename"] = self.construct_filename(name, format)
        url = munge_url(url, InputOptions())
        path = self.download_file(url, **kwargs)
        return url, path

    def download_resource(self, resource: Resource, **kwargs: Any) -> tuple[str, str]:
        """Download HDX resource os a file and return the url downloaded and
        the path of the file. The filename of the file comes from the name and
        format.

        Args:
            resource: HDX resource
            **kwargs: Parameters to pass to download_file call

        Returns:
            (URL that was downloaded, path to downloaded file)
        """
        return self.construct_filename_and_download(
            resource["name"],
            resource.get_format(),
            resource["url"],
            **kwargs,
        )

    @staticmethod
    def get_hapi_dataset_metadata(dataset: Dataset, datasetinfo: dict) -> dict:
        """Get HAPI dataset metadata from HDX dataset

        Args:
            dataset: HDX dataset
            datasetinfo: Dictionary of information about dataset

        Returns:
            HAPI dataset metadata
        """
        license_id = dataset["license_id"]
        if license_id == "hdx-other":
            license = dataset["license_other"]
        else:
            license_title = dataset["license_title"]
            license_url = dataset.get("license_url")
            if license_url:
                license = f"[{license_title}]({license_url})"
            else:
                license = license_title
        return {
            "hdx_id": dataset["id"],
            "hdx_stub": dataset["name"],
            "title": dataset["title"],
            "hdx_provider_stub": dataset["organization"]["name"],
            "hdx_provider_name": dataset["organization"]["title"],
            "license": license,
            "time_period": datasetinfo["time_period"],
        }

    @staticmethod
    def get_hapi_resource_metadata(resource: Resource) -> dict:
        """Get HAPI resource metadata from HDX resource

        Args:
            resource: HDX dataset

        Returns:
            HAPI resource metadata
        """
        return {
            "hdx_id": resource["id"],
            "name": resource["name"],
            "format": resource.get_format(),
            "update_date": parse_date(resource["last_modified"]),
            "download_url": resource["url"],
        }

    def read_hxl_resource(
        self, resource: Resource, **kwargs: Any
    ) -> hxl.Dataset | None:
        """Read HDX resource as a HXL dataset.

        Args:
            resource: HDX resource
            **kwargs: Parameters to pass to download_file call

        Returns:
            HXL dataset or None
        """
        url = resource["url"]
        try:
            _, path = self.download_resource(resource, **kwargs)
            data = hxl.data(path, InputOptions(allow_local=True)).cache()
            data.display_tags
            return data
        except hxl.HXLException:
            logger.warning(f"Could not process {url}. Maybe there are no HXL tags?")
            return None
        except Exception:
            logger.exception(f"Error reading {url}!")
            raise

    def hxl_info_file(
        self, name: str, format: str, url: str, **kwargs: Any
    ) -> dict | None:
        """Get HXL info on file. The filename comes from the name and
        format.

        Args:
            name: Name for the download
            format: Format of download
            url: URL of download
            **kwargs (Any): Parameters to pass to download_file call

        Returns:
            Information about file or None
        """
        try:
            _, path = self.construct_filename_and_download(name, format, url, **kwargs)
            return hxl.info(path, InputOptions(allow_local=True))
        except hxl.HXLException:
            logger.warning(f"Could not process {url}. Maybe there are no HXL tags?")
            return None
        except Exception:
            logger.exception(f"Error reading {url}!")
            raise

    def hxl_info_hapi_resource_metadata(
        self,
        hapi_resource_metadata: dict,
        **kwargs: Any,
    ) -> dict | None:
        """Get HXL info on HAPI resource. The filename comes from the name and
        format.

        Args:
            hapi_resource_metadata: HAPI resource metadata
            **kwargs (Any): Parameters to pass to download_file call

        Returns:
            Information about file or None
        """
        name = hapi_resource_metadata["name"]
        format = hapi_resource_metadata["format"]
        url = hapi_resource_metadata["download_url"]
        return self.hxl_info_file(name, format, url, **kwargs)

    def read_hdx_metadata(
        self,
        datasetinfo: dict,
        do_resource_check: bool = True,
        configuration: Configuration | None = None,
    ) -> Resource | None:
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
            datasetinfo: Dictionary of information about dataset
            do_resource_check: Whether to check resources. Default is False.
            configuration: HDX configuration. Default is global configuration.

        Returns:
            The resource if a url was not given
        """
        dataset_nameinfo = datasetinfo["dataset"]
        if isinstance(dataset_nameinfo, str):
            dataset = self.read_dataset(dataset_nameinfo, configuration)
            resource = None
            url = datasetinfo.get("url")
            resource_name = datasetinfo.get("resource")
            # Only loop through resources if do_resource_check is True and
            # either there is no url in the datasetinfo dictionary or a
            # resource name has been specified in there
            if do_resource_check and (not url or resource_name):
                format = datasetinfo["format"].lower()
                found = False
                for resource in dataset.get_resources():
                    if resource["format"].lower() == format:
                        if resource_name:  # if resource is specified,
                            if resource["name"] == resource_name:  # match it
                                found = True
                                break
                            continue
                        else:  # if resource is not specified, use first one
                            found = True
                            break
                if not found:
                    error = [f"Cannot find {format} resource"]
                    if resource_name:
                        error.append(f"with name {resource_name}")
                    error.append(f"in {dataset_nameinfo}!")
                    raise ValueError(" ".join(error))
                if url:  # if there is a url in the datasetinfo dictionary,
                    resource["url"] = url  # set the resource url to it
                else:
                    url = resource["url"]  # otherwise set the url key in
                    # datasetinfo to the resource url (by setting url here)
                datasetinfo["hapi_resource_metadata"] = self.get_hapi_resource_metadata(
                    resource
                )
                datasetinfo["url"] = url
            if "source_date" not in datasetinfo:
                datasetinfo["source_date"] = get_startend_dates_from_time_period(
                    dataset, today=self.today
                )
            if "source" not in datasetinfo:
                datasetinfo["source"] = dataset["dataset_source"]
            if "source_url" not in datasetinfo:
                datasetinfo["source_url"] = dataset.get_hdx_url()
            Sources.standardise_datasetinfo_source_date(datasetinfo)
            datasetinfo["hapi_dataset_metadata"] = self.get_hapi_dataset_metadata(
                dataset, datasetinfo
            )
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
                dataset = self.read_dataset(dataset_name, configuration)
                datasets[dataset_name] = dataset
            if source_date is not None:
                if hxltag == "default_dataset":
                    key = "default_date"
                else:
                    key = hxltag
                source_date[key] = get_startend_dates_from_time_period(
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
        datasetinfo: dict,
        configuration: Configuration | None = None,
        **kwargs: Any,
    ) -> tuple[list[str], Iterator[dict]]:
        """Read data and metadata from HDX dataset

        Args:
            datasetinfo: Dictionary of information about dataset
            configuration: HDX configuration. Default is global configuration.
            **kwargs: Parameters to pass to download_file call

        Returns:
            Tuple (headers, iterator where each row is a dictionary)
        """
        resource = self.read_hdx_metadata(datasetinfo, configuration=configuration)
        filename = kwargs.get("filename")
        if filename:
            del kwargs["filename"]
            datasetinfo["filename"] = filename
        filename = datasetinfo.get("filename")
        if resource and not filename:
            filename = self.construct_filename(resource["name"], resource.get_format())
            file_prefix = kwargs.get("file_prefix")
            if not file_prefix:
                file_prefix = datasetinfo.get("file_prefix")
            if file_prefix:
                filename = f"{file_prefix}_{filename}"
            datasetinfo["filename"] = filename
        return self.read_tabular(datasetinfo, **kwargs)

    def read(
        self,
        datasetinfo: dict,
        configuration: Configuration | None = None,
        **kwargs: Any,
    ) -> tuple[list[str], Iterator[dict]]:
        """Read data and metadata from HDX dataset

        Args:
            datasetinfo: Dictionary of information about dataset
            configuration: HDX configuration. Default is global configuration.
            **kwargs: Parameters to pass to download_file call

        Returns:
            Tuple (headers, iterator where each row is a dictionary)
        """
        format = datasetinfo["format"]
        if format in ["json", "csv", "xls", "xlsx"]:
            if "dataset" in datasetinfo:
                headers, iterator = self.read_hdx(datasetinfo, configuration, **kwargs)
            else:
                headers, iterator = self.read_tabular(datasetinfo, **kwargs)
        else:
            raise ValueError(f"Invalid format {format} for {datasetinfo['name']}!")
        return headers, iterator
