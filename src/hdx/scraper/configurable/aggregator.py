import copy
import logging
import sys
from typing import Dict, List, Tuple, Union

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import (  # noqa: F401
    get_fraction_str,
    get_numeric_if_possible,
    number_format,
)
from slugify import slugify

from hdx.scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class Aggregator(BaseScraper):
    def __init__(
        self,
        name: str,
        datasetinfo: Dict,
        headers: Dict[str, Tuple],
        adm_aggregation: Union[Dict, List],
        input_values: Dict,
        aggregation_scrapers: [List[BaseScraper]],
        use_hxl: bool,
    ):
        super().__init__(name, datasetinfo, headers)
        self.adm_aggregation = adm_aggregation
        self.input_values = input_values
        self.aggregation_scrapers = aggregation_scrapers
        self.use_hxl = use_hxl

    @classmethod
    def get_scrapers(
        cls,
        configuration,
        input_level,
        output_level,
        adm_aggregation,
        runner,
        use_hxl=True,
    ):
        input_results = runner.get_results(levels=input_level, has_run=False)
        aggregation_scrapers = list()
        if not input_results:
            return aggregation_scrapers
        if use_hxl:
            main_index = 1
            header_or_hxltag_str = "hxltag"
        else:
            main_index = 0
            header_or_hxltag_str = "header"
        input_results = input_results[input_level]
        input_headers = input_results["headers"]
        input_vals = input_results["values"]
        input_values = dict()
        for index, input_header_or_hxltag in enumerate(
            input_headers[main_index]
        ):
            input_values[input_header_or_hxltag] = input_vals[index]
        if not isinstance(adm_aggregation, dict):
            adm_aggregation = {x: ("value",) for x in adm_aggregation}
        for header_or_hxltag, process_info in configuration.items():
            process_info = copy.deepcopy(process_info)
            name = f"{slugify(header_or_hxltag.lower(), separator='_')}_{output_level}"
            config_headers_or_hxltags = process_info.get("input")
            if config_headers_or_hxltags:
                exists = True
                for i, config_header_or_hxltag in enumerate(
                    config_headers_or_hxltags
                ):
                    try:
                        input_headers[main_index].index(
                            config_header_or_hxltag
                        )
                    except ValueError:
                        logger.error(
                            f"{output_level} {header_or_hxltag_str} {header_or_hxltag} not found in {input_level} input!"
                        )
                        exists = False
                        break
                if not exists:
                    continue
                output = process_info["output"]
                if use_hxl:
                    headers = ((output,), (header_or_hxltag,))
                else:
                    headers = ((header_or_hxltag,), (output,))
                scraper = cls(
                    name,
                    process_info,
                    {output_level: headers},
                    adm_aggregation,
                    input_values,
                    aggregation_scrapers,
                    use_hxl,
                )
                aggregation_scrapers.append(scraper)
            else:
                try:
                    index = input_headers[main_index].index(header_or_hxltag)
                    process_info["input"] = (header_or_hxltag,)
                    header_or_hxltag = process_info.get(
                        "rename", header_or_hxltag
                    )
                    if use_hxl:
                        output = process_info.get(
                            "output", input_headers[0][index]
                        )
                        headers = (
                            (output,),
                            (header_or_hxltag,),
                        )
                    else:
                        output = process_info.get(
                            "output", input_headers[1][index]
                        )
                        headers = (
                            (header_or_hxltag,),
                            (output,),
                        )
                    scraper = cls(
                        name,
                        process_info,
                        {output_level: headers},
                        adm_aggregation,
                        input_values,
                        aggregation_scrapers,
                        use_hxl,
                    )
                    aggregation_scrapers.append(scraper)
                except ValueError:
                    logger.error(
                        f"{output_level} {header_or_hxltag_str} {header_or_hxltag} not found in {input_level} input!"
                    )
        return aggregation_scrapers

    @staticmethod
    def get_float_or_int(valuestr):
        if not valuestr or valuestr == "N/A":
            return None
        if "." in valuestr:
            return float(valuestr)
        else:
            return int(valuestr)

    @classmethod
    def get_numeric(cls, valuestr):
        if isinstance(valuestr, str):
            total = 0
            hasvalues = False
            for value in valuestr.split("|"):
                value = cls.get_float_or_int(value)
                if value:
                    hasvalues = True
                    total += value
            if hasvalues is False:
                return ""
            return total
        return valuestr

    def process(self, output_level: str, output_hxltag, output_values):
        population_key = self.datasetinfo.get("population_key")

        def add_population(adm, value):
            if not value:
                return
            if output_hxltag == "#population":
                if population_key is None:
                    self.population_lookup[adm] = value
                else:
                    self.population_lookup[population_key] = value

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
                    add_population(output_adm, total)
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
                add_population(output_adm, total)

    def run(self):
        output_level = next(iter(self.headers.keys()))
        output_headers = self.get_headers(output_level)
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
        self.process(output_level, output_headers[1][0], output_values)

    def add_sources(self):
        pass

    def add_population(self):
        pass
