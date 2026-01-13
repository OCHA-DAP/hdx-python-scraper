from collections.abc import Sequence
from datetime import datetime
from logging import Logger

from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date


class Sources:
    default_source_date_format = "%b %-d, %Y"
    default_date_range_separator = "-"
    should_overwrite_sources = False

    @classmethod
    def set_default_source_date_format(cls, format: str) -> None:
        """Set new default source date format. (Default is "%b %-d, %Y".)

        Args:
            format: Date format

        Returns:
            None
        """
        cls.default_source_date_format = format

    @classmethod
    def set_default_date_range_separator(cls, separator: str) -> None:
        """Set new default source date range separator. (Default is "-".)

        Args:
            separator: Separator

        Returns:
            None
        """
        cls.default_date_range_separator = separator

    @classmethod
    def set_should_overwrite_sources(cls, overwrite: bool) -> None:
        """Set whether a source should overwrite a previous one with the same
        HXL hashtag. (Default is to keep the previous source.)

        Args:
            overwrite: Whether to overwrite previous source

        Returns:
            None
        """
        cls.should_overwrite_sources = overwrite

    @staticmethod
    def standardise_datasetinfo_source_date(
        datasetinfo: dict,
    ) -> datetime | None:
        """Standardise source date format in datasetinfo dictionary. Returns
        default end date or None.

        Args:
            datasetinfo: Information about dataset

        Returns:
            Default end date or None
        """
        source_date = datasetinfo.get("source_date")
        if not source_date:
            datasetinfo["source_date"] = None
            return None

        output_source_date = {}

        def set_source_date(date, hxltag="default_date", startend="end"):
            if isinstance(date, str):
                date = parse_date(date)
                if startend == "end":
                    date = date.replace(
                        hour=23,
                        minute=59,
                        second=59,
                        microsecond=999999,
                    )

            if hxltag not in output_source_date:
                output_source_date[hxltag] = {}
            output_source_date[hxltag][startend] = date

        if isinstance(source_date, dict):
            for key, value in source_date.items():
                if key in ("start", "end"):
                    set_source_date(value, startend=key)
                else:
                    if isinstance(value, dict):
                        for startend, date in value.items():
                            set_source_date(date, hxltag=key, startend=startend)
                    else:
                        set_source_date(value, hxltag=key)
        else:
            set_source_date(source_date)
        default_date = output_source_date.get("default_date")
        if default_date:
            default_end_date = default_date.get("end")
            if default_end_date:
                datasetinfo["source_date"] = output_source_date
                datasetinfo["time_period"] = default_date
                return default_end_date
        datasetinfo["source_date"] = None
        return None

    @classmethod
    def get_hxltag_source_datetime(
        cls, datasetinfo: dict, hxltag: str, fallback: bool = False
    ) -> datetime:
        """Get standardised source date for HXL hashtag as datetime

        Args:
            datasetinfo: Information about dataset
            hxltag: HXL hashtag to check
            fallback: Whether to fall back to default_date. Default is False.

        Returns:
            Standardised source date for HXL hashtag
        """
        cls.standardise_datasetinfo_source_date(datasetinfo)
        source_date = datasetinfo["source_date"]
        date = source_date.get(hxltag)
        if not date:
            if not fallback:
                return None
            date = source_date["default_date"]
        return date

    @classmethod
    def format_hxltag_source_date(cls, datasetinfo: dict, date: dict) -> str:
        """Get formatted date from source date

        Args:
            datasetinfo: Information about dataset
            date: Source date dictionary

        Returns:
            Formatted source date string
        """
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
    def get_hxltag_source_date(
        cls, datasetinfo: dict, hxltag: str, fallback: bool = False
    ) -> str:
        """Get standardised and formatted source date for HXL hashtag as
        string

        Args:
            datasetinfo: Information about dataset
            hxltag: HXL hashtag to check
            fallback: Whether to fall back to default_date. Default is False.

        Returns:
            Standardised and formatted source date for HXL hashtag
        """
        date = cls.get_hxltag_source_datetime(datasetinfo, hxltag, fallback)
        if not date:
            return
        return cls.format_hxltag_source_date(datasetinfo, date)

    @classmethod
    def add_source_overwrite(
        cls,
        hxltags: list[str],
        sources: list[Sequence],
        source: Sequence[str],
        logger: Logger,
        should_overwrite_sources: bool | None = None,
    ) -> None:
        """Add source to sources preventing duplication.

        Args:
            hxltags: List of HXL hashtags, one for each source name
            sources: List of sources
            source: Source information
            logger: Logger to log warnings to
            should_overwrite_sources: Whether to overwrite sources. Default is None (class default).

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
                logger.warning(f"Keeping existing source information for {hxltag}!")
        else:
            hxltags.append(hxltag)
            sources.append(source)

    @classmethod
    def add_sources_overwrite(
        cls,
        hxltags: list[str],
        sources: list[Sequence],
        sources_to_add: list[Sequence],
        logger: Logger,
        should_overwrite_sources: bool | None = None,
    ) -> None:
        """Add source to sources preventing duplication

        Args:
            hxltags: List of HXL hashtags, one for each source name
            sources: List of sources
            sources_to_add: List of sources to add
            logger: Logegr to log warnings to
            should_overwrite_sources: Whether to overwrite sources. Default is None (class default).

        Returns:
            None
        """
        for source in sources_to_add:
            cls.add_source_overwrite(
                hxltags, sources, source, logger, should_overwrite_sources
            )

    @staticmethod
    def create_source_configuration(
        suffix_attribute: str | None = None,
        admin_sources: bool = False,
        adminlevel: AdminLevel | Sequence[AdminLevel] | None = None,
        admin_mapping_dict: dict | None = None,
        no_sources: bool = False,
        should_overwrite_sources: bool | None = None,
    ) -> dict | None:
        """Create source configuration. If none of the arguments are supplied,
        source configuration is None. suffix_attribute is an attribute to add
        to the end of source HXL hashtags. admin_sources defines whether the
        admin unit is added as an attribute (eg. a country iso3 code like
        +AFG). admin_level defines one or more AdminLevel objects that will be
        used to map admin pcodes to country iso3 codes. If admin_level is
        defined, admin_sources is assumed to be True. Alternatively,
        admin_mapping_dict can be supplied to define mapping from amin names to
        attribute suffixes. If no sources should be outputted no_sources should
        be set to True.

        Args:
            suffix_attribute: Suffix to add. Default is None.
            admin_sources: Whether source information is per admin unit. Default is False.
            adminlevel: Admin level(s) mapping. Default is None.
            admin_mapping_dict: Admin unit mapping to use. Default is None.
            no_sources: Don't create sources. Default is False.
            should_overwrite_sources: Whether to overwrite sources. Default is None (use default).

        Returns:
             Source configuration dictionary
        """
        source_configuration = {}
        if no_sources:
            source_configuration["no_sources"] = True
            return source_configuration
        source_configuration["should_overwrite_sources"] = should_overwrite_sources
        if suffix_attribute:
            source_configuration["suffix_attribute"] = suffix_attribute
            return source_configuration
        admin_mapping = None
        if adminlevel:
            if isinstance(adminlevel, AdminLevel):
                admin_mapping = adminlevel.pcode_to_iso3
            else:
                admin_mapping = {}
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
