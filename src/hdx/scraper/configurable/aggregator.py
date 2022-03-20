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
    ):
        super().__init__(name, datasetinfo, headers)
        self.adm_aggregation = adm_aggregation
        self.input_values = input_values
        self.aggregation_scrapers = aggregation_scrapers

    @classmethod
    def get_scrapers(
        cls, configuration, input_level, output_level, adm_aggregation, runner
    ):
        input_results = runner.get_results(levels=input_level, has_run=False)
        aggregation_scrapers = list()
        if not input_results:
            return aggregation_scrapers
        input_results = input_results[input_level]
        input_headers = input_results["headers"]
        input_vals = input_results["values"]
        input_values = dict()
        for index, input_header in enumerate(input_headers[0]):
            input_values[input_header] = input_vals[index]
        if not isinstance(adm_aggregation, dict):
            adm_aggregation = {x: ("value",) for x in adm_aggregation}
        for header, process_info in configuration.items():
            name = f"{slugify(header.lower(), separator='_')}_{output_level}"
            config_headers = process_info.get("headers")
            if config_headers:
                exists = True
                for i, config_header in enumerate(config_headers):
                    try:
                        input_headers[0].index(config_header)
                    except ValueError:
                        logger.error(
                            f"{output_level} header {header} not found in {input_level} headers!"
                        )
                        exists = False
                        break
                if not exists:
                    continue
                headers = ((header,), (process_info["hxltag"],))
                scraper = cls(
                    name,
                    process_info,
                    {output_level: headers},
                    adm_aggregation,
                    input_values,
                    aggregation_scrapers,
                )
                aggregation_scrapers.append(scraper)
            else:
                try:
                    index = input_headers[0].index(header)
                    process_info = copy.deepcopy(process_info)
                    process_info["headers"] = (header,)
                    header = process_info.get("rename", header)
                    headers = ((header,), (input_headers[1][index],))
                    scraper = cls(
                        name,
                        process_info,
                        {output_level: headers},
                        adm_aggregation,
                        input_values,
                        aggregation_scrapers,
                    )
                    aggregation_scrapers.append(scraper)
                except ValueError:
                    logger.error(
                        f"{output_level} header {header} not found in {input_level} headers!"
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
            headers = list()
            for aggregation_scraper in self.aggregation_scrapers:
                headers.append(
                    aggregation_scraper.get_headers(output_level)[0][0]
                )
            # Indices of list sorted by length
            sorted_len_indices = sorted(
                range(len(headers)),
                key=lambda k: len(headers[k]),
                reverse=True,
            )
            for output_adm, valuelist in output_values.items():
                toeval = formula.replace("#population", arbitrary_string)
                for i in sorted_len_indices:
                    aggregation_scraper = self.aggregation_scrapers[i]
                    values = aggregation_scraper.get_values(output_level)[0]
                    value = values.get(output_adm, "")
                    toeval = toeval.replace(headers[i], str(value))
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
        input_headers = self.datasetinfo["headers"]
        for input_header in input_headers:
            input_valdicts.append(self.input_values[input_header])
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
