import logging
import sys
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import (  # noqa: F401
    get_fraction_str,
    get_numeric_if_possible,
    number_format,
)
from hdx.utilities.typehint import ListTuple
from slugify import slugify

from ..base_scraper import BaseScraper
from ..utilities.reader import Read

logger = logging.getLogger(__name__)


class Aggregator(BaseScraper):
    """Each aggregator is configured from dataset information that can come
    from a YAML file for example. When run, it works out headers and aggregated values.
    The mapping from input admins to aggregated output admins adm_aggregation
    is of form: {"AFG": ("ROAP",), "MMR": ("ROAP",)}. If the mapping is to the top
    level, then it is a list of input admins like: ("AFG", "MMR"). The input_values
    are a mapping from headers (if use_hxl is False) or HXL tags (if use_hxl is True)
    to column values expressed as a dictionary mapping from input admin to value. If
    any formulae require the result of other aggregators, these can be passed in using
    the aggregation_scrapers parameter.


    Args:
        name (str): Name of aggregator
        datasetinfo (Dict): Information about dataset
        adm_aggregation (Union[Dict, ListTuple]): Mapping from input admins to aggregated output admins
        headers (Dict[str, Tuple]): Column headers and HXL hashtags
        use_hxl (bool): Whether to map from headers or from HXL tags
        source_configuration (Optional[Dict]): Configuration for sources. Defaults to None (use defaults).
        aggregation_scrapers (List["Aggregator"]): Other aggregations needed. Defaults to list().
    """

    def __init__(
        self,
        name: str,
        datasetinfo: Dict,
        headers: Dict[str, Tuple],
        adm_aggregation: Union[Dict, ListTuple],
        use_hxl: bool,
        source_configuration: Optional[Dict] = None,
        aggregation_scrapers: List["Aggregator"] = list(),
    ):
        super().__init__(
            name,
            datasetinfo,
            headers,
            source_configuration=source_configuration,
        )
        if isinstance(adm_aggregation, dict):
            self.adm_aggregation: Dict[str, Tuple] = adm_aggregation
        else:
            self.adm_aggregation: Dict[str, Tuple] = {
                x: ("value",) for x in adm_aggregation
            }

        self.use_hxl = use_hxl
        self.aggregation_scrapers = aggregation_scrapers

    @classmethod
    def get_scraper(
        cls,
        use_hxl: bool,
        header_or_hxltag: str,
        datasetinfo: Dict,
        input_level: str,
        output_level: str,
        adm_aggregation: Union[Dict, ListTuple],
        input_headers: Tuple[ListTuple, ListTuple],
        source_configuration: Optional[Dict] = None,
        aggregation_scrapers: List["Aggregator"] = list(),
    ) -> Optional["Aggregator"]:
        """Gets one aggregator given dataset information and returns headers, values and
        sources. The mapping from input admins to aggregated output admins
        adm_aggregation is of form: {"AFG": ("ROAP",), "MMR": ("ROAP",)}. If the mapping
        is to the top level, then it is a list of input admins like: ("AFG", "MMR").
        The input_values are a mapping from headers or HXL tags to column values
        expressed as a dictionary mapping from input admin to value.

        Args:
            use_hxl (bool): Whether to map from headers or from HXL tags
            header_or_hxltag (str): Column header or HXL hashtag depending on use_hxl
            datasetinfo (Dict): Information about dataset
            input_level (str): Input level to aggregate like national or subnational
            output_level (str): Output level of aggregated data like regional
            adm_aggregation (Union[Dict, ListTuple]): Mapping from input admins to aggregated output admins
            input_headers (Tuple[ListTuple, ListTuple]): Column headers and HXL hashtags
            runner(Runner): Runner object
            source_configuration (Optional[Dict]): Configuration for sources. Defaults to None (use defaults).
            aggregation_scrapers (List["Aggregator"]): Other aggregations needed. Defaults to list().

        Returns:
            Optional["Aggregator"]: The aggregation scraper or None if it couldn't be created
        """
        if use_hxl:
            main_index = 1
            header_or_hxltag_str = "hxltag"
        else:
            main_index = 0
            header_or_hxltag_str = "header"

        datasetinfo = deepcopy(datasetinfo)
        if "dataset" in datasetinfo:
            reader = Read.get_reader("hdx")
            reader.read_hdx_metadata(datasetinfo, do_resource_check=False)
        config_headers_or_hxltags = datasetinfo.get("input")
        if config_headers_or_hxltags:
            exists = True
            for i, config_header_or_hxltag in enumerate(
                config_headers_or_hxltags
            ):
                try:
                    input_headers[main_index].index(config_header_or_hxltag)
                except ValueError:
                    logger.error(
                        f"{output_level} {header_or_hxltag_str} {header_or_hxltag} not found in {input_level} input!"
                    )
                    exists = False
                    break
            if not exists:
                return None
            output = datasetinfo["output"]
        else:
            try:
                index = input_headers[main_index].index(header_or_hxltag)
                datasetinfo["input"] = (header_or_hxltag,)
                header_or_hxltag = datasetinfo.get("rename", header_or_hxltag)
                if use_hxl:
                    output = datasetinfo.get("output", input_headers[0][index])
                else:
                    output = datasetinfo.get("output", input_headers[1][index])
            except ValueError:
                logger.error(
                    f"{output_level} {header_or_hxltag_str} {header_or_hxltag} not found in {input_level} input!"
                )
                return None

        name = f"{slugify(header_or_hxltag.lower(), separator='_')}_{output_level}"
        if use_hxl:
            headers = ((output,), (header_or_hxltag,))
        else:
            headers = ((header_or_hxltag,), (output,))
        return cls(
            name,
            datasetinfo,
            {output_level: headers},
            adm_aggregation,
            use_hxl,
            source_configuration,
            aggregation_scrapers,
        )

    @staticmethod
    def get_float_or_int(valuestr: str) -> Union[float, int, None]:
        """Convert value string to float, int or None

        Args:
            valuestr (str): Value string

        Returns:
            Union[float, int, None]: Converted value
        """
        if not valuestr or valuestr == "N/A":
            return None
        if "." in valuestr:
            return float(valuestr)
        else:
            return int(valuestr)

    @classmethod
    def get_numeric(cls, valueinput: Any) -> Union[str, float, int]:
        """Convert value input to float or int. Values in pipe separated strings are
        summed. If any values in a pipe separated string are empty, an empty string is
        returned.

        Args:
            valueinput (Any): Value string

        Returns:
            Union[str, float, int]: Converted value
        """
        if isinstance(valueinput, str):
            total = 0
            hasvalues = False
            for value in valueinput.split("|"):
                value = cls.get_float_or_int(value)
                if value:
                    hasvalues = True
                    total += value
            if hasvalues is False:
                return ""
            return total
        return valueinput

    def process(self, output_level: str, output_values: Dict) -> None:
        """Perform aggregation putting results in output_values

        Args:
            output_level (str): Output level of aggregated data like regional
            output_values (Dict): Mapping from admin name to value

        Returns:
            None
        """
        action = self.datasetinfo["action"]
        if action == "sum" or action == "mean":
            for output_adm, valuelist in output_values.items():
                total = ""
                novals = 0
                for valuestr in valuelist:
                    value = ""
                    if isinstance(valuestr, int) or isinstance(
                        valuestr, float
                    ):
                        value = valuestr
                    else:
                        if valuestr:
                            value = self.get_numeric(valuestr)
                    if value != "":
                        novals += 1
                        if total == "":
                            total = value
                        else:
                            total += value
                if action == "mean":
                    if not isinstance(total, str):
                        if isinstance(total, int):
                            quotient, remainder = divmod(total, novals)
                            if remainder == 0:
                                total = quotient
                            else:
                                total /= novals
                        else:
                            total /= novals
                if isinstance(total, float):
                    output_values[output_adm] = number_format(
                        total, trailing_zeros=False
                    )
                else:
                    output_values[output_adm] = total
        elif action == "range":
            for output_adm, valuelist in output_values.items():
                min = sys.maxsize
                max = -min
                for valuestr in valuelist:
                    if valuestr:
                        value = self.get_numeric(valuestr)
                        if value > max:
                            max = value
                        if value < min:
                            min = value
                if min == sys.maxsize or max == -sys.maxsize:
                    output_values[output_adm] = ""
                else:
                    if isinstance(max, float):
                        max = number_format(max, trailing_zeros=False)
                    if isinstance(min, float):
                        min = number_format(min, trailing_zeros=False)
                    output_values[output_adm] = f"{str(min)}-{str(max)}"
        elif action == "eval":
            formula = self.datasetinfo["formula"]
            population_key = self.datasetinfo.get("population_key")
            if population_key is None:
                population_str = "self.population_lookup[output_adm]"
            else:
                population_str = "self.population_lookup[population_key]"
            arbitrary_string = "#pzbgvjh"
            headers_or_hxltags = list()
            if self.use_hxl:
                index = 1
            else:
                index = 0
            for aggregation_scraper in self.aggregation_scrapers:
                headers_or_hxltags.append(
                    aggregation_scraper.get_headers(output_level)[index][0]
                )
            # Indices of list sorted by length
            sorted_len_indices = sorted(
                range(len(headers_or_hxltags)),
                key=lambda k: len(headers_or_hxltags[k]),
                reverse=True,
            )
            for output_adm, valuelist in output_values.items():
                toeval = formula.replace("#population", arbitrary_string)
                for i in sorted_len_indices:
                    aggregation_scraper = self.aggregation_scrapers[i]
                    values = aggregation_scraper.get_values(output_level)[0]
                    value = values.get(output_adm, "")
                    toeval = toeval.replace(headers_or_hxltags[i], str(value))
                toeval = toeval.replace(arbitrary_string, population_str)
                total = eval(toeval)
                output_values[output_adm] = total

    def set_input_values_sources(self, input_values, input_sourcesinfo):
        self.input_values = input_values
        self.input_sourcesinfo = input_sourcesinfo

    def run(self) -> None:
        """Runs one aggregator given dataset information

        Returns:
            None
        """
        output_level = next(iter(self.headers.keys()))
        output_valdicts = self.get_values(output_level)
        output_values = output_valdicts[0]
        input_valdicts = list()
        input_headers_or_hxltags = self.datasetinfo["input"]
        for input_header_or_hxltag in input_headers_or_hxltags:
            input_valdicts.append(self.input_values[input_header_or_hxltag])
        found_adms = set()
        for input_values in input_valdicts:
            for input_adm in input_values:
                for output_adm in self.adm_aggregation[input_adm]:
                    key = f"{output_adm}|{input_adm}"
                    if key in found_adms:
                        continue
                    value = input_values[input_adm]
                    if value is not None:
                        found_adms.add(key)
                        dict_of_lists_add(output_values, output_adm, value)
        self.process(output_level, output_values)
        self.aggregation_scrapers.append(self)

    def add_sources(self) -> None:
        """There is no need to add any sources since the disaggregated values should
        already have sources

        Returns:
            None
        """
        config_headers_or_hxltags = self.datasetinfo.get("input")
        source_lookup = self.datasetinfo.get("source_copy")
        if (
            not source_lookup
            and config_headers_or_hxltags
            and len(config_headers_or_hxltags) == 1
        ):
            source_lookup = config_headers_or_hxltags[0]
        if not source_lookup:
            output_level = next(iter(self.headers.keys()))
            if self.use_hxl:
                source_lookup = self.headers[output_level][1][0]
            else:
                source_lookup = self.headers[output_level][0][0]
        sourceinfo = self.input_sourcesinfo.get(source_lookup)
        if sourceinfo:
            if "source_date" not in self.datasetinfo:
                self.datasetinfo["source_date"] = sourceinfo["source_date"]
            if "source" not in self.datasetinfo:
                self.datasetinfo["source"] = ",".join(sourceinfo["source"])
            if "source_url" not in self.datasetinfo:
                self.datasetinfo["source_url"] = ",".join(
                    sourceinfo["source_url"]
                )
        if "source" not in self.datasetinfo:
            return
        super().add_sources()
