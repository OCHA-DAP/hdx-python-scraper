[![Build Status](https://github.com/OCHA-DAP/hdx-python-scraper/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-python-scraper/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-python-scraper/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-python-scraper?branch=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Downloads](https://img.shields.io/pypi/dm/hdx-python-scraper.svg)](https://pypistats.org/packages/hdx-python-scraper)

The HDX Python Scraper Library is designed to enable you to easily develop code that 
assembles data from one or more tabular sources that can be csv, xls, xlsx or JSON. It 
uses a YAML file that specifies for each source what needs to be read and allows some 
transformations to be performed on the data. The output is written to JSON, Google sheets 
and/or Excel and includes the addition of 
[Humanitarian Exchange Language (HXL)](https://hxlstandard.org/) hashtags specified in 
the YAML file. Custom Python scrapers can also be written that conform to a defined 
specification and the framework handles the execution of both configurable and custom 
scrapers.

For more information, please read the 
[documentation](https://hdx-python-scraper.readthedocs.io/en/latest/). 

This library is part of the 
[Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.
