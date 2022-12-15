from logging import Logger
from typing import Dict, List, Optional, Union

from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date
from hdx.utilities.typehint import ListTuple


class Sources:
    default_source_date_format = "%b %-d, %Y"
    default_date_range_separator = "-"
    should_overwrite_sources = False

    @classmethod
    def set_default_source_date_format(cls, format):
        cls.default_source_date_format = format

    @classmethod
    def set_default_date_range_separator(cls, separator):
        cls.default_date_range_separator = separator

    @classmethod
    def set_should_overwrite_sources(cls, overwrite):
        cls.should_overwrite_sources = overwrite

    @staticmethod
    def standardise_datasetinfo_source_date(datasetinfo):
        source_date = datasetinfo.get("source_date")
        if not source_date:
            datasetinfo["source_date"] = None
            return None

        output_source_date = dict()

        def set_source_date(date, hxltag="default_date", startend="end"):
            if isinstance(date, str):
                date = parse_date(date)
            if hxltag not in output_source_date:
                output_source_date[hxltag] = dict()
            output_source_date[hxltag][startend] = date

        if isinstance(source_date, dict):
            for key, value in source_date.items():
                if key in ("start", "end"):
                    set_source_date(value, startend=key)
                else:
                    if isinstance(value, dict):
                        for startend, date in value.items():
                            set_source_date(
                                date, hxltag=key, startend=startend
                            )
                    else:
                        set_source_date(value, hxltag=key)
        else:
            set_source_date(source_date)
        default_date = output_source_date.get("default_date")
        if default_date:
            default_end_date = default_date.get("end")
            if default_end_date:
                datasetinfo["source_date"] = output_source_date
                return default_end_date
        datasetinfo["source_date"] = None
        return None

    @classmethod
    def get_hxltag_source_datetime(cls, datasetinfo, hxltag, fallback=False):
        cls.standardise_datasetinfo_source_date(datasetinfo)
        source_date = datasetinfo["source_date"]
        date = source_date.get(hxltag)
        if not date:
            if not fallback:
                return None
            date = source_date["default_date"]
        return date

    @classmethod
    def format_hxltag_source_date(cls, datasetinfo, date):
        source_date_format = datasetinfo.get(
            "source_date_format", cls.default_source_date_format
        )
        if isinstance(source_date_format, str):
            start_source_date_format = None
            end_source_date_format = source_date_format
            date_range_separator = None
        else:
            start_source_date_format = source_date_format.get("start")
            end_source_date_format = source_date_format.get("end")
            if not end_source_date_format:
                end_source_date_format = source_date_format["date"]
            date_range_separator = source_date_format.get(
                "separator", cls.default_date_range_separator
            )
        enddate = date["end"].strftime(end_source_date_format)
        startdate = date.get("start")
        if start_source_date_format and startdate:
            startdate = startdate.strftime(start_source_date_format)
            return f"{startdate}{date_range_separator}{enddate}"
        return enddate

    @classmethod
    def get_hxltag_source_date(cls, datasetinfo, hxltag, fallback=False):
        date = cls.get_hxltag_source_datetime(datasetinfo, hxltag, fallback)
        if not date:
            return
        return cls.format_hxltag_source_date(datasetinfo, date)

    @classmethod
    def add_source_overwrite(
        cls,
        hxltags: List[str],
        sources: List[ListTuple],
        source: ListTuple[str],
        logger: Logger,
        should_overwrite_sources: Optional[bool] = None,
    ):
        """Add source to sources preventing duplication.

        Args:
            hxltags (List[str]): List of HXL hashtags, one for each source name
            sources (List[ListTuple]): List of sources
            source (ListTuple[str]): Source information
            logger (Logger): Logger to log warnings to
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (class default).

        Returns:
            None
        """
        hxltag = source[0]
        if should_overwrite_sources is None:
            should_overwrite_sources = cls.should_overwrite_sources
        if hxltag in hxltags:
            if should_overwrite_sources:
                logger.warning(f"Overwriting source information for {hxltag}!")
                index = hxltags.index(hxltag)
                sources[index] = source
            else:
                logger.warning(
                    f"Keeping existing source information for {hxltag}!"
                )
        else:
            hxltags.append(hxltag)
            sources.append(source)

    @classmethod
    def add_sources_overwrite(
        cls,
        hxltags: List[str],
        sources: List[ListTuple],
        sources_to_add: List[ListTuple],
        logger: Logger,
        should_overwrite_sources: Optional[bool] = None,
    ):
        """Add source to sources preventing duplication

        Args:
            hxltags (List[str]): List of HXL hashtags, one for each source name
            sources (List[ListTuple]): List of sources
            sources_to_add (List[ListTuple]): List of sources to add
            logger (Logger): Logegr to log warnings to
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (class default).

        Returns:
            None
        """
        for source in sources_to_add:
            cls.add_source_overwrite(
                hxltags, sources, source, logger, should_overwrite_sources
            )

    @staticmethod
    def create_source_configuration(
        suffix_attribute: Optional[str] = None,
        admin_sources: bool = False,
        adminlevel: Union[AdminLevel, ListTuple[AdminLevel], None] = None,
        admin_mapping_dict: Optional[Dict] = None,
        no_sources: bool = False,
        should_overwrite_sources: Optional[bool] = None,
    ) -> Optional[Dict]:
        """Create source configuration. If none of the arguments are suppled, source
        configuration is None. suffix_attribute is an attribute to add to the end of
        source HXL hashtags. admin_sources defines whether the admin unit is added as an
        attribute (eg. a country iso3 code like +AFG). admin_level defines one or more
        AdminLevel objects that will be used to map admin pcodes to country iso3 codes. If
        admin_level is defined, admin_sources is assumed to be True. Alternatively,
        admin_mapping_dict can be supplied to define mapping from amin names to attribute
        suffixes. If no sources should be outputted no_sources should be set to True.

        Args:
            suffix_attribute (Optional[str]): Suffix to add. Defaults to None.
            admin_sources (bool): Whether source information is per admin unit. Defaults to False.
            adminlevel (Union[AdminLevel, ListTuple[AdminLevel], None]): Admin level(s) mapping. Defaults to None.
            admin_mapping_dict (Optional[Dict]): Admin unit mapping to use. Defaults to None.
            no_sources (bool): Don't create sources. Defaults to False.
            should_overwrite_sources (Optional[bool]): Whether to overwrite sources. Defaults to None (use default).

        Returns:
             Optional[Dict]: Source configuration dictionary
        """
        source_configuration = dict()
        if no_sources:
            source_configuration["no_sources"] = True
            return source_configuration
        source_configuration[
            "should_overwrite_sources"
        ] = should_overwrite_sources
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
