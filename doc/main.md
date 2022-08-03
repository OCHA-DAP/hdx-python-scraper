# Summary

The HDX Python Scraper Library is designed to enable you to easily develop code that 
assembles data from one or more tabular sources that can be csv, xls, xlsx or JSON. It 
uses a YAML file that specifies for each source what needs to be read and allows some 
transformations to be performed on the data. The output is written to JSON, Google sheets 
and/or Excel and includes the addition of 
[Humanitarian Exchange Language (HXL)](https://hxlstandard.org/) hashtags specified in 
the YAML file. Custom Python scrapers can also be written that conform to a defined 
specification and the framework handles the execution of both configurable and custom 
scrapers.

# Information

This library is part of the 
[Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-python-scraper).
The library has detailed, but currently outdated API documentation which can be found 
in the menu at the top. 

To use the optional functions for outputting data from Pandas to JSON, Excel etc., 
install with:

    pip install hdx-python-scraper[pandas]

## Breaking Changes

From 1.8.7, FileCopier -> ResourceDownloader, get_scrapers calls in Aggregator,
ResourceDownloader and TimeSeries -> Runner.addXXX, read_resource -> download_resource,
add_to_run -> force_add_to_run

From 1.8.3, changes to update_sources and update_regional

From 1.7.5, new Read class to use instead of Retrieve class

From 1.6.7, retrievers are generated up front

From 1.6.6, configuration fields for output JSON renamed to `additional_inputs`, 
`output` and `additional_outputs`.

From 1.6.0, major renaming of configuration fields, mostly dropping _cols eg. `input`
instead of `input_cols`

From 1.4.4, significant refactor that adds custom scraper support and a runner class.

# Scraper Framework Configuration

A full project showing how the scraper framework is used in a real world scenario is 
[here](https://github.com/OCHA-DAP/hdx-scraper-ukraine-viz/blob/main/scrapers/main.py).
It is very helpful to look at that project to see a full working setup that demonstrates
usage of many of the features of this library.

## Input and Processing Setup

The library is set up broadly as follows:

    with temp_dir() as temp_folder:
        today = parse_date("2020-10-01")
        Read.create_readers(
            temp_folder,
            "saved_data",
            temp_folder,
            save,
            use_saved,
            hdx_auth=configuration.get_api_key(),
            header_auths=header_auths,
            basic_auths=basic_auths,
            param_auths=param_auths,
            today=today,
        )
        ...
        adminone = AdminOne(configuration)
        Fallbacks.add(json_path)
        runner = Runner(("AFG",), adminone, today)
        keys = runner.add_configurables(scraper_configuration, "national")
        education_closures = EducationClosures(
            datasetinfo, today, countries, region
        )
        runner.add_custom(education_closures)
        runner.run(prioritise_scrapers=("population_national", "population_subnational"))
        results = runner.get_results()["national"]
        assert results["headers"] == [("header1", header2"...), ("#hxltag1", "#hxltag2",...)]
        assert results["values"] == [{"AFG": 38041754, "PSE": ...}, {"AFG": 123, "PSE": ...}, ...]
        assert results["sources"] == [("#population", "2020-10-01", "World Bank", "https://..."), ...]
        
The framework is configured by passing in a configuration. Typically this will come from 
a YAML file such as `config/project_configuration.yml`.

### Read Class

The Read class is a utility for reading metadata and data from websites and APIs with
the ability to set up authorisations up front with Read objects held in a dictionary 
for subsequent use. Read objects can save copies of the data being downloaded or read 
from pre-saved data for tests. 

The first parameter is the location of fallback data (if available). The second 
specifies to where data should be saved if desired. The third parameter is the path of
a temporary folder. If the downloaded data should be saved, the fourth optional 
parameter `save` should be True. If a test is being run against pre-saved data, the fifth 
optional parameter `use_saved` should be True.

Additional readers are generated if any of header_auths, basic_auths or extra_params 
are populated. header_auths and basic_auths are dictionaries of form 
`{"scraper name": "auth", ...}`. extra_params is of form 
`{"scraper name": {"key": "auth", ...}, ...}`.

Scrapers that inherit from `BaseScraper` can call the method `get_reader(SCRAPER_NAME)`
to obtain the Read object associated with the supplied `SCRAPER_NAME`. If no special
authorisations are needed for the website the scraper accesses, then the default Read
object is returned.

### AdminOne Class

More about the AdminOne class can be found in the 
[HDX Python Country](https://github.com/OCHA-DAP/hdx-python-country) library. Briefly, 
that class accepts a configuration (which is shown below in YAML syntax) as follows:

`country_name_mappings` defines the country name overrides we use (ie. where we deviate 
from the names in the OCHA countries and territories file).

    country_name_mappings:
      PSE: "occupied Palestinian territory"
      BOL: "Bolivia"

`admin1_info` defines the admin level 1 names and pcodes. 

    admin1_info:
      - {pcode: AF01, name: Kabul, iso2: AF, iso3: AFG, country: Afghanistan}
      - {pcode: AF02, name: Kapisa, iso2: AF, iso3: AFG, country: Afghanistan}

`country_name_mappings` defines mappings from country name to iso3.

    country_name_mappings:
      "Congo DR": "COD"
      "CAR": "CAF"
      "oPt": "PSE"

`admin1_name_mappings` defines mappings from admin 1 name to pcode
    
    admin1_name_mappings:
      "Nord-Ouest": "HT09"
      "nord-ouest": "HT09"
    ...

`adm1_name_replacements` defines some find and replaces that are done to improve 
automatic admin1 name matching to pcode.

    adm1_name_replacements:
      " urban": ""
      "sud": "south"
      "ouest": "west"
    ...

`admin1_fuzzy_dont` defines admin 1 names to ignore in fuzzy matching.

    admin1_fuzzy_dont:
      - "nord"
    ...

### Runner Class

The Runner constructor takes various parameters. 

The first parameter is a list of country iso3s. 

The second is an AdminOne object. 

The third is the datetime you want to use for "today". If you pass None, it will use the 
current datetime.

The fourth is an optional object of class ErrorsOnExit from the 
[HDX Python Utilities](https://github.com/OCHA-DAP/hdx-python-utilities) library. This
class collects and outputs errors on exit.

The last optional parameter is a list of scrapers to run.

The method `add_configurables` is used to add scrapers that are configured from a YAML 
configuration. The method `add_custom` is used to add a custom scraper and `add_customs`
for multiple custom scrapers - these are scrapers written in Python that inherit the 
`BaseScraper` class. If running specific scrapers rather than all, and you want to force 
the inclusion of the scraper in the run regardless of the specific scrapers given, the 
parameter `force_add_to_run` should be set to True.

It is possible to add a post run step to a scraper that has been set up using:

    runner.add_post_run("SCRAPER_NAME", function_to_call)

Scrapers are run using the `run` method and results obtained using `get_results`.

### Fallbacks

Fallbacks can be defined which are used for example when there is a network issue. This
is done using the `Fallbacks.add()` call. This can only be done if there is a JSON 
output defined. `Fallbacks.add()` takes a few parameters. 

The first parameter is a path to the output JSON.

The second optional parameter is a mapping from level name to key in the JSON. The 
default is:

    {
        "global": "global_data",
        "regional": "regional_data",
        "national": "national_data",
        "subnational": "subnational_data",
    }

The third optional parameter specifies the key where the sources can be found, 
defaulting to `sources`.

The fourth parameter, also optional, specifies a mapping from level to admin name. The
default is:

    {
        "global": "value",
        "regional": "#region+name",
        "national": "#country+code",
        "subnational": "#adm1+code",
    }

## Scrapers

### Custom Scrapers

It is possible to define custom scrapers written in Python which must inherit 
[BaseScraper](https://github.com/OCHA-DAP/hdx-python-scraper/blob/main/src/hdx/scraper/base_scraper.py), 
calling its constructor and providing a `run` method. Other methods where a default 
implementation has been provided can be overridden such as `add_sources` and 
`add_population`. There are also two hooks for running steps at particular points.
`run_after_fallbacks` is executed after fallbacks are used and `post_run` is executed 
after running whether or not fallbacks were used.

The structure is broadly as follows:

    class MyScraper(BaseScraper):
        def __init__(
            self, datasetinfo: Dict, today, countryiso3s, downloader
        ):
            super().__init__(
                "scraper_name",
                datasetinfo,
                {
                    "national": (("Header1",), ("#hxltag1",)),
                    "regional": (("Header1",), ("#hxltag1",),),
                },
            )
            self.today = today
            self.countryiso3s = countryiso3s
            self.downloader = downloader
    
        def run(self) -> None:
            headers, iterator = read(
                self.downloader, self.datasetinfo
            )
            output_national = self.get_values("national")[0]
            ...

As can be seen above, headers take the form of a mapping from a level such as "national" 
to a tuple of column headers and HXL hashtags. Values are populated by the scraper as it
runs and are of the form below where each dictionary would represent one column in the
output:

        {"national": ({"AFG": 1.2, "PSE": 1.4}, {"AFG": 123, "PSE": 241}, ...})}

Sources are also populated and take the form below where each tuple includes the source 
HXL hashtag, source date, source and source url:

        {"national": [("#food-prices", "2022-07-15", "WFP", "https://data.humdata.org/dataset/global-wfp-food-prices"), ...]

The code earlier would go on to populate the dictionary `output_national` which is one
dictionary in values representing one column. It is a mapping from national admin names 
(ie. countries) to values. `output_regional` would also be populated. It is a mapping 
from regions to values. In this case, since national and regional each have only one 
header and HXL hashtag, there is only one dictionary to populate for each. 

An example of a custom scraper can be seen 
[here](https://github.com/OCHA-DAP/hdx-python-scraper/blob/main/tests/hdx/scraper/education_closures.py).

### Configurable Scrapers

Configurable scrapers take their configuration from a dictionary usually provided
by reading from a YAML file. They use that information to work out headers, values and
sources which can be later used to populate an output such as a Google Sheet.

scraper_tabname in the configuration YAML defines a set of configurable scrapers that 
use the framework and produce data for the tab tabname which typically corresponds to a 
level like national or subnational eg.

    scraper_national:
    …

It is helpful to look at a few example configurable scrapers to see how they are 
configured:

The economicindex configurable scraper reads the dataset 
“covid-19-economic-exposure-index” on HDX, taking from it dataset source, date of 
dataset and using the url of the dataset in HDX as the source url. (In HDX data
explorers, these are used by the DATA links.) The scraper framework finds the first 
resource that is of format `xlsx`, reads the “economic exposure” sheet and looks for the 
headers in row 1 (by default). Note that it is possible to specify a specific resource 
name using the key `resource` instead of searching for the first resource of a 
particular format.

`admin` defines the column or columns in which to look for admin information. As this 
is a national level scraper, it uses the "Country" column. `input` specifies the 
column(s) to use for values, in this case “Covid 19 Economic exposure index”. 
`output` and `output_hxl` define the header name(s) and HXL tag(s) to 
use for the `input`.

     economicindex:
        dataset: "covid-19-economic-exposure-index"
        format: "xlsx"
        sheet: "economic exposure"
        admin:
          - "Country"
        input:
          - "Covid 19 Economic exposure index"
        output:
          - "EconomicExposure"
        output_hxl:
          - "#severity+economic+num"

The casualties configurable scraper reads from a file that has data for only one admin 
unit which is specified using admin_single. The latest row by date is obtained by 
specifying `date` and `date_type` (which can be date, year or int):

      casualties:
        source: "OHCHR"
        dataset: "ukraine-key-figures-2022"
        format: "csv"
        headers: 2
        date: "Date"
        date_type: "date"
        admin_single: "UKR"
        input:
          - "Civilian casualities(OHCHR) - Killed"
          - "Civilian casualities(OHCHR) - Injured"
        output:
          - "CiviliansKilled"
          - "CiviliansInjured"
        output_hxl:
          - "#affected+killed"
          - "#affected+injured"

The population configurable scraper configuration directly provides metadata for source, 
source_url and the download  location given by url only taking the source date from the 
dataset. The scraper pulls subnational data so admin defines both a country column 
`alpha_3` and an admin 1 pcode column `ADM1_PCODE`. Running this scraper will result in
`population_lookup` in the `BaseScraper` being populated with key value pairs.

`transform` defines operations to be performed on each value in the column. In 
this case, the value is converted to either an int or float if it is possible.

     population:
        source: "Multiple Sources"
        source_url: "https://data.humdata.org/search?organization=worldpop&q=%22population%20counts%22"
        dataset: "global-humanitarian-response-plan-covid-19-administrative-boundaries-and-population-statistics"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vS3_uBOV_uRDSxOggfBus_pkCs6Iw9lm0nAxzwG14YF_frpm13WPKiM1oNnQ9zrUA/pub?gid=1565793974&single=true&output=csv"
        format: "csv"
        admin:
          - "alpha_3"
          - "ADM1_PCODE"
        input:
          - "POPULATION"
        transform:
          "POPULATION": "get_numeric_if_possible(POPULATION)"
        output:
          - "Population"
        output_hxl:
          - "#population"

The travel configurable scraper reads values from the “info” and “published” columns of 
the source. append_cols defines any columns where if the same admin appears again (in 
this case, the same country), then that data is appended to the existing. `keep_cols` 
defines any columns where if the same admin appears again, the existing value is kept 
rather than replaced.

     travel:
        dataset: "covid-19-global-travel-restrictions-and-airline-information"
        format: "csv"
        admin:
          - "iso3"
        input:
          - "info"
          - "published"
        append_cols:
          - "info"
        keep_cols:
          - "published"
        output:
          - "TravelRestrictions"
          - "TravelRestrictionsPublished"
        output_hxl:
          - "#severity+travel"
          - "#severity+date+travel"

The gam configurable scraper reads from a spreadsheet that has a multiline header 
(headers defined as rows 3 and 4). Experimentation is often needed with row numbers 
since in my experience, they are sometimes offset from the real row numbers seen when 
opening the spreadsheet. `date` defines a column that contains date information and 
`date_type` specifies in what form the date information is held eg. as a date, a year or
an int. The scraper will for each admin, obtain the data (in this case the “National 
Point Estimate”) for the latest date up to the current date (unless `ignore_future_date` 
is set to False then future dates will be allowed).

     gam:
        dataset: "world-global-expanded-database-on-severe-wasting"
        format: "xlsx"
        sheet: "Trend"
        headers:
          - 3
          - 4
        admin:
          - "ISO"
        date: "Year*"
        date_type: "year"
        input:
          - "National Point Estimate"
        output:
          - "Malnutrition Estimate"
        output_hxl:
          - "#severity+malnutrition+num+national"

The covidtests configurable scraper gets “new_tests” and “new_tests_per_thousand” for 
the latest date where a date_condition is satisfied which is that “new_tests” is a value 
greater than zero. Here, the default sheet of 1 and the default headers rows of 1 are 
assumed. These defaults apply for both xls and xlsx.

     covidtests:
        dataset: "total-covid-19-tests-performed-by-country"
        format: "xlsx"
        date: "date"
        date_type: "date"
        date_condition: "new_tests is not None and new_tests > 0"
        admin:
          - "iso_code"
        input:
          - "new_tests"
          - "new_tests_per_thousand"
        output:
          - "New Tests"
          - "New Tests Per Thousand"
        output_hxl:
          - "#affected+tested"
          - "#affected+tested+per1000"

The oxcgrt configurable scraper reads from a data source that has HXL tags and these can 
be used instead of the header names provided use_hxl is set to True. By default all the 
HXLated columns are read with the admin related ones inferred and the rest taken as 
values except if defined as a `date`.

     oxcgrt:
        dataset: "oxford-covid-19-government-response-tracker"
        format: "csv"
        use_hxl: True
        date: "#date"
        date_type: "date"

In the imperial configurable scraper, `output` and `output_hxl` are defined 
which specify which columns and HXL tags in the HXLated file should be used rather than 
using all HXLated columns.

     imperial:
        dataset: "imperial-college-covid-19-projections"
        format: "xlsx"
        use_hxl: True
        output:
          - "Imp: Total Cases(min)"
          - "Imp: Total Cases(max)"
          - "Imp: Total Deaths(min)"
          - "Imp: Total Deaths(max)"
        output_hxl:
          - "#affected+infected+min+imperial"
          - "#affected+infected+max+imperial"
          - "#affected+killed+min+imperial"
          - "#affected+killed+max+imperial"

The idmc configurable scraper reads 2 HXLated columns defined in `input`. In 
`transform`, a cast to int is performed if the value is not None or it is set to 
0. `process` defines new column(s) that can be combinations of the other columns in 
`input`. In this case, `process` specifies a new column which sums the 2 
columns in `input`. That new column is given a header and a HXL tag (in 
`output` and `output_hxl`).


     idmc:
        dataset: "idmc-internally-displaced-persons-idps"
        format: "csv"
        use_hxl: True
        date: "#date+year"
        date_type: "year"
        input:
          - "#affected+idps+ind+stock+conflict"
          - "#affected+idps+ind+stock+disaster"
        transform:
          "#affected+idps+ind+stock+conflict": "int(#affected+idps+ind+stock+conflict) if #affected+idps+ind+stock+conflict else 0"
          "#affected+idps+ind+stock+disaster": "int(#affected+idps+ind+stock+disaster) if #affected+idps+ind+stock+disaster else 0"
        process:
          - "#affected+idps+ind+stock+conflict + #affected+idps+ind+stock+disaster"
        output:
          - "TotalIDPs"
        output_hxl:
          - "#affected+displaced"

The needs configurable scraper takes data for the latest available date for each country. 
`subsets` allows the definition of multiple indicators by way of filters. A filter is 
defined for each indicator (in this case there is one) which contains one or more 
filters in Python syntax. Column names can be used directly and if all are not already 
specified in `input` and `date`, then all should be put as a list under the key 
`filter_cols`.

     needs:
        dataset: "global-humanitarian-overview-2020-figures"
        format: "xlsx"
        sheet: "Raw Data"
        headers: 1
        admin:
          - "Country Code"
        date: "Year"
        date_type: "year"
        filter_cols:
          - "Metric"
          - "PiN Value for Dataviz"
        subsets:
          - filter: "Metric == 'People in need' and PiN Value for Dataviz == 'yes'"
            input:
              - "Value"
            output:
              - "PeopleInNeed"
            output_hxl:
              - "#affected+inneed"

The population configurable scraper matches country code only using exact matching 
(`admin_exact` is set to True) rather than the default which tries fuzzy matching in the 
event of a failure to match exactly. This is useful when matching produces false 
positives which is very rare so is usually not needed. It populates the 
`population_lookup` dictionary of `BaseScraper`.

     population:
        source: "World Bank"
        source_url: "https://data.humdata.org/organization/world-bank-group"
        url: "http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel&dataformat=list"
        format: "xls"
        sheet: "Data"
        headers: 3
        admin:
          - "Country Code"
        admin_exact: True
        date: "Year"
        date_type: "year"
        date_condition: "Value is not None"
        input:
          - "Value"
        output:
          - "Population"
        output_hxl:
          - "#population"

The configurable scraper below which is intended to produce global data pulls out only 
the "WLD" value from the input data. It populates the `population_lookup` dictionary of
`BaseScraper` using the key `global` taken from `population_key` which must be defined 
at the top level not in a subset.

        source: "World Bank"
        source_url: "https://data.humdata.org/organization/world-bank-group"
        url: "tests/fixtures/API_SP.POP.TOTL_DS2_en_excel_v2_1302508_LIST.xls"
        format: "xls"
        sheet: "Data"
        headers: 3
        sort:
          keys:
            - "Year"
        admin:
          - "Country Code"
        admin_filter:
          - "WLD"
        date: "Year"
        date_type: "year"
        input:
          - "Value"
        population_key: "global"
        output:
          - "Population"
        output_hxl:
          - "#population"

The covid tests configurable scraper applies a `prefilter` to the data that only 
processes rows where the value in the column "new_tests" is not None and is greater than 
zero. If "new_tests" was not specified in `input` or `date`, then it would need 
to be under a key `filter_cols`.

    covidtests:
        source: "Our World in Data"
        dataset: "total-covid-19-tests-performed-by-country"
        url: "tests/fixtures/owid-covid-data.xlsx"
        format: "xlsx"
        prefilter: "new_tests is not None and new_tests > 0"
     ...

The sadd configurable scraper reads data from the dataset 
“covid-19-sex-disaggregated-data-tracker”. It filters that data using data from another 
file, the url of which is defined in `external_filter`. Specifically, it cuts down the 
sadd data to only include countries listed in the “#country+code+v_iso2” column of the 
`external_filter` file.
 
     sadd:
         dataset: "covid-19-sex-disaggregated-data-tracker"
         format: "csv"
         external_filter:
           url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vR9PhPG7-aH0EkaBGzXYlrO9252gqs-UuKIeDQr9D3pOLBOdQ_AoSwWi21msHsdyT7thnjuhSY6ykSX/pub?gid=434885896&single=true&output=csv"
           hxl:
             - "#country+code+v_iso2"
     ...

The fsnwg configurable scraper first applies a sort to the data it reads. The reverse 
sort is based on the keys “reference_year” and “reference_code”. `admin` defines a 
country column "adm0_pcod3" and three admin 1 level columns (“adm1_pcod2”, “adm1_pcod3”, 
“adm1_name”) which are examined consecutively until a match with the internal admin 1 is 
made. 

`date` is comprised of the amalgamation of two columns “reference_year” and 
“reference_code” (corresponding to the two columns which were used for sorting earlier). 
`sum` is used to sum values in a column. For example, the formula 
`get_fraction_str(phase3, population)` takes the sum of all phase 3 values for an 
admin 1 and divides it by the sum of all population values for an admin 1. 

`mustbepopulated` determines if values are included or not and is by default False. If 
it is True, then only when all columns in `input` for the row are populated is the 
value included. This means that when using multiple summed columns together in a 
formula, the number of values that were summed in each column will be the same. The last 
formula uses “#population” which is replaced by the population for the admin unit (which 
is taken from the `population_lookup` variable of `BaseScraper`).

     fsnwg:
        dataset: "cadre-harmonise"
        format: "xlsx"
        sort:
          reverse: True
          keys:
            - "reference_year"
            - "reference_code"
        admin:
          - "adm0_pcod3"
          - - "adm1_pcod2"
            - "adm1_pcod3"
            - "adm1_name"
        date:
          - "reference_year"
          - "reference_code"
        date_type: "int"
        filter_cols:
          - "chtype"
        subsets:
          - filter: "chtype == 'current'"
            input:
              - "phase3"
              - "phase4"
              - "phase5"
              - "phase35"
              - "population"
            transform:
              "phase3": "float(phase3)"
              "phase4": "float(phase4)"
              "phase5": "float(phase5)"
              "phase35": "float(phase35)"
              "population": "float(population)"
            sum:
              - formula: "get_fraction_str(phase3, population)"
                mustbepopulated: True
              - formula: "get_fraction_str(phase4, population)"
                mustbepopulated: True
              - formula: "get_fraction_str(phase5, population)"
                mustbepopulated: True
              - formula: "get_fraction_str(phase35, population)"
                mustbepopulated: True
              - formula: "get_fraction_str(population, #population)"
                mustbepopulated: True
            output:
              - "FoodInsecurityCHP3"
              - "FoodInsecurityCHP4"
              - "FoodInsecurityCHP5"
              - "FoodInsecurityCHP3+"
              - "FoodInsecurityCHAnalysed"
            output_hxl:
              - "#affected+ch+food+p3+pct"
              - "#affected+ch+food+p4+pct"
              - "#affected+ch+food+p5+pct"
              - "#affected+ch+food+p3plus+pct"
              - "#affected+ch+food+analysed+pct"

The who_subnational configurable scraper defines two values in `input_ignore_vals` which 
if found are ignored. Since `mustbepopulated` is True, then only when all columns in 
`input` for the row are populated and do not contain either “-2222” or “-4444” is 
the value included in the sum of any column used in `sum`. 

     who_subnational:
        source: "WHO"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vRfjaIXE1hvEIXD66g6cuCbPrGdZkx6vLIgXO_znVbjQ-OgwfaI1kJPhxhgjw2Yg08CmtBuMLAZkTnu/pub?gid=337443769&single=true&output=csv"
        format: "csv"
        admin:
          - "iso"
          - "Admin1"
        date: "Year"
        date_type: "year"
        filter_cols:
          - "Vaccine"
        subsets:
          - filter: "Vaccine == 'HepB1'"
            input:
              - "Numerator"
              - "Denominator"
            input_ignore_vals:
              - "-2222"
              - "-4444"
            transform:
              Numerator: "float(Numerator)"
              Denominator: "float(Denominator)"
            sum:
              - formula: "get_fraction_str(Numerator, Denominator)"
                mustbepopulated: True
            output:
              - "HepB1 Coverage"
            output_hxl:
              - "#population+hepb1+pct+vaccinated"

The access configurable scraper provides different sources for each HXL tag by providing 
dictionaries instead of strings in `source` and `source_url`. It maps specific HXL tags 
by key to sources or falls back on a “default_source” and “default_url” for all 
unspecified HXL tags.

     access:
        source:
          "#access+visas+pct": "OCHA"
          "#access+travel+pct": "OCHA"
          "#event+year+previous+num": "Aid Workers Database"
          "#event+year+todate+num": "Aid Workers Database"
          "#event+year+previous+todate+num": "Aid Workers Database"
          "#activity+cerf+project+insecurity+pct": "OCHA"
          "#activity+cbpf+project+insecurity+pct": "OCHA"
          "#population+education": "UNESCO"
          "default_source": "Multiple sources"
        source_url:
          "#event+year+previous+num": "https://data.humdata.org/dataset/security-incidents-on-aid-workers"
          "#event+year+todate+num": "https://data.humdata.org/dataset/security-incidents-on-aid-workers"
          "#event+year+previous+todate+num": "https://data.humdata.org/dataset/security-incidents-on-aid-workers"
          "default_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv"
        format: "csv"
        use_hxl: True
        transform:
          "#access+visas+pct": "get_numeric_if_possible(#access+visas+pct)"
          "#access+travel+pct": "get_numeric_if_possible(#access+travel+pct)"
          "#activity+cerf+project+insecurity+pct": "get_numeric_if_possible(#activity+cerf+project+insecurity+pct)"
          "#activity+cbpf+project+insecurity+pct": "get_numeric_if_possible(#activity+cbpf+project+insecurity+pct)"
          "#population+education": "get_numeric_if_possible(#population+education)"

The field `admin_filter` allows overriding the country iso 3 codes and admin 1 pcodes for 
the specific configurable scraper so that only those specified in `admin_filter` are used:

      gam:
        source: "UNICEF"
        url: "tests/fixtures/unicef_who_wb_global_expanded_databases_severe_wasting.xlsx"
        format: "xlsx"
        sheet: "Trend"
        headers:
          - 3
          - 4
        flatten:
          - original: "Region {{1}} Region Name"
            new: "Region Name"
          - original: "Region {{1}} Point Estimate"
            new: "Region Point Estimate"
        admin:
          - "ISO"
          - "Region Name"
        admin_filter:
          - ["AFG"]
          - ["AF09", "AF24"]
        date: "Year*"
        date_type: "year"
        input:
          - "Region Point Estimate"
        output:
          - "Malnutrition Estimate"
        output_hxl:
          - "#severity+malnutrition+num+subnational"

The `date_level` field enables reading of data containing dates where the latest date 
must be used at a particular level such as per country or per admin 1. For example, we 
might want to produce a global value by summing the latest available value per country 
as shown below. We have specified a date and sum. The library will apply a sort 
by date (since we have not specified one) to ensure correct results. This would also 
happen if we had used `process` or `append_cols`. 

      ourworldindata:
        source: "Our World in Data"
        url: "tests/fixtures/ourworldindata_vaccinedoses.csv"
        format: "csv"
        use_hxl: True
        admin:
          - "#country+code"
        date: "#date"
        date_type: "date"
        date_level: "national"
        input:
          - "#total+vaccinations"
        sum:
          - formula: "number_format(#total+vaccinations, format='%.0f')"
        output:
          - "TotalDosesAdministered"
        output_hxl:
          - "#capacity+doses+administered+total"

An example of combining `admin_filter`, `date` and `date_level` is getting the latest 
global value in which global data has been treated as a special case of national data by 
using "OWID_WRL" in the "#country+code" field:

      ourworldindata:
        source: "Our World in Data"
        url: "tests/fixtures/ourworldindata_vaccinedoses.csv"
        format: "csv"
        use_hxl: True
        admin:
          - "#country+code"
        admin_filter:
          - "OWID_WRL"
        date: "#date"
        date_type: "date"
        date_level: "national"
        input:
          - "#total+vaccinations"
        output:
          - "TotalDosesAdministered"
        output_hxl:
          - "#capacity+doses+administered+total"

This filtering for "OWID_WRL" can be more simply achieved by using a prefilter as in the
below example. This configurable scraper also outputs a calculated column using
`process`. That column is evaluated using population data from `population_lookup` 
of `BaseScraper` using the key `global` taken from `population_key` which must be 
defined in the subset not at the top level. 

      ourworldindata:
        source: "Our World in Data"
        url: "tests/fixtures/ourworldindata_vaccinedoses.csv"
        format: "csv"
        use_hxl: True
        prefilter: "#country+code == 'OWID_WRL'"
        date: "#date"
        date_type: "date"
        filter_cols:
          - "#country+code"
        input:
          - "#total+vaccinations"
        population_key: "global"
        process:
          - "#total+vaccinations"
          - "number_format((#total+vaccinations / 2) / #population)"
        output:
          - "TotalDosesAdministered"
          - "PopulationCoverageAdministeredDoses"
        output_hxl:
          - "#capacity+doses+administered+total"
          - "#capacity+doses+administered+coverage+pct"

If columns need to be summed and the latest date chosen overall not per admin unit, then 
we can specify `single_maxdate` as shown below:

      cerf_global:
        dataset: "cerf-covid-19-allocations"
        url: "tests/fixtures/full_pfmb_allocations.csv"
        format: "csv"
        force_date_today: True
        headers: 1
        date: "AllocationYear"
        date_type: "year"
        single_maxdate: True
        filter_cols:
          - "FundType"
          - "GenderMarker"
        subsets:
          ...
          - filter: "FundType == 'CBPF' and GenderMarker == '0'"
            input:
              - "Budget"
            transform:
              Budget: "float(Budget)"
            sum:
              - formula: "Budget"
            output:
              - "CBPFFundingGM0"
            output_hxl:
              - "#value+cbpf+funding+gm0+total+usd"
           ...

## Population Data

Population data is treated as a special class of data. By default, configurable and 
custom scrapers detect population data by looking for the output HXL hashtag 
`#population` and add it to a dictionary `population_lookup` that is a variable of the
`BaseScraper` class and hence accessible to all scrapers. For most data, the admin 
names will be taken from the data. For top level data, the admin name is taken from the 
level name unless `population_key` is defined in which case the value in there will be 
used instead. 

For configurable scrapers where columns are evaluated (rather than assigned), it is 
possible to use `#population` and the appropriate population value for the 
administrative unit will be substituted automatically. Where output is a single value,
for example where working on global data, then `population_key` must be specified both
for any configurable scraper that outputs a single population value and for any
configurable scraper that needs that single population value. `population_key` defines
what key will be used with `population_lookup`.

When using subsets, the rule is when reading from`population_lookup`, define 
`population_key` in the subset and when writing to it, define `population_key` at the 
top level of the configuration.

## Output Specification and Setup

The YAML configuration should have a mapping from the internal dictionaries to the tabs 
in the spreadsheet / keys in the JSON output file(s):

    tabs:
      world: "WorldData"
      regional: "RegionalData"
      national: "NationalData"
      subnational: "SubnationalData"
      covid_series: "CovidSeries"
      covid_trend: "CovidTrend"
      sources: "Sources"

Then the location of Google spreadsheets are defined, for prod (production), test and 
scratch:

    googlesheets:
      prod: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_PROD/edit"
      test: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_TEST/edit"
      scratch: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_SCRATCH/edit"

The json outputs are then specified:

    json:
      additional_inputs:
        - name: "Other"
          source: "Some org"
          source_url: "https://data.humdata.org/organization/world-bank-group"
          format: "json"
          url: "https://raw.githubusercontent.com/mcarans/hdx-python-scraper/master/tests/fixtures/additional_json.json"
          jsonpath: "[*]"
      output: "test_tabular_all.json"
      additional_outputs:
        - filepath: "test_tabular_population.json"
          tabs:
            - tab: "national"
              key: "cumulative"
              filters:
                "#country+code": "{{countries_to_save}}"
              output:
                - "#country+code"
                - "#country+name"
                - "#population"
        - filepath: "test_tabular_population_2.json"
          tabs:
            - tab: "national"
              key: "cumulative"
              filters:
                "#country+code":
                  - "AFG"
              output:
                - "#country+code"
                - "#country+name"
                - "#population"
        - filepath: "test_tabular_other.json"
          remove:
            - "national"

The key `additional_inputs` defines JSON files to be downloaded and added to the JSON 
under the appropriate key (eg. `Other` in the example configuration above). Source
information is added as well. 

The key `output` contains the path of the output JSON file. 

Under the key `additional_outputs`, subsets of the full JSON output can be saved as 
separate files. For each additional output, `filepath` defines the path of the output 
cut down JSON file. A subset of each input `tab` in `tabs` is output under `key`. 
`filters` can be applied for example to restrict to a set of country codes which can be 
given in the configuration or passed in as variables to the `save` call (see below).
The HXL hashtags to be outputted (ie. the columns) are defined under `output`. If 
`remove` is supplied instead of `tabs` then all of the data in the full JSON file is
outputted except for the tabs defined under `remove`.

The code to go with the configuration is as follows: 

    excelout = ExcelFile(excel_path, tabs, updatetabs)
    gsheets = GoogleSheets(gsheet_config, gsheet_auth, updatesheets, tabs, updatetabs)
    jsonout = JsonFile(json_config, updatetabs)
    outputs = {"gsheets": gsheets, "excel": excelout, "json": jsonout}
    ...
    update_subnational(runner, scraper_names, adminone, outputs)
    ...
    excelout.save()
    filepaths = jsonout.save(tempdir, countries_to_save=["AFG"])

Output from the scrapers can go to Excel, Google Sheets and/or a JSON file. 

There are standard functions for updating level data in `update_tabs.py` including 
`update_toplevel` ("global" for example), `update_regional`, `update_national` and 
`update_subnational`.

`update_national` is set up as follows:

    flag_countries = {
        "header": "ishrp",
        "hxltag": "#meta+ishrp",
        "countries": hrp_countries,
    }
    update_national(
        runner,
        gho_countries,
        outputs,
        names=national_names,
        flag_countries=flag_countries,
        iso3_to_region=RegionLookup.iso3_to_regions["GHO"],
        ignore_regions=("GHO",),
        level="national",
        tab="national",
    )

The first parameter is the Runner object. The second is a list of country ISO 3 codes.
The third is a dictionary of outputs such as to Google Sheets, Excel or JSON. The
fourth optional parameter is the `names` of the scrapers to include. The fifth optional
parameter, `flag_countries`, is a dictionary where the "header" and "hxltag" keys define 
an additional column to output which can contain "Y" or "N" depending upon whether the 
country is one of those defined in the key "countries". The sixth optional parameter,
`iso3_to_region`, is a dictionary which maps from ISO 3 country code to a set of regions 
and it will result in an additional column (with header "Region" and HXL hashtag 
"#region+name") in which regions are listed separated by "|". The seventh optional 
`ignore_regions` parameter defines a list of regions that should not be output in the 
"Region" column. The eighth parameter `level` defaults to "national" and is the key to
use when obtaining the results of the scrapers. The last parameter is the `tab` to
update and defaults to "national".

`update_subnational` is used as follows:

    update_subnational(
        runner, 
        adminone, 
        outputs, 
        names=subnational_names,
        level="subnational",
        tab="subnational",
    )

The first parameter is the Runner object. The second is an AdminOne object (described 
earlier). The third is a dictionary of outputs such as to Google Sheets, Excel or JSON. 
The fourth optional parameter is the `names` of the scrapers to include. The parameter 
`level` defaults to "subnational" and is the key to use when obtaining the results of 
the scrapers. The last parameter is the `tab` to update and defaults to "subnational".

`update_toplevel` and `update_regional` require the output from two other functions.

    regional_rows = get_regional_rows(
        runner,
        RegionLookup.regions + ["global"],
        names=regional_names,
        level="regional",
    )
    global_rows = get_toplevel_rows(
        runner,
        names=global_names,
        overrides={"who_covid": {"gho": "global"}},
        toplevel="global",
    )

The first parameter of `get_regional_rows` is the Runner object. The second is a list
of regions. The third optional parameter is the `names` of the scrapers to include. The 
parameter `level` defaults to "regional" and is the key to use when obtaining the 
results of the scrapers.

The first parameter of `get_toplevel_rows` is the Runner object. The second optional 
parameter is the `names` of the scrapers to include. The optional parameter `overrides`
defines levels to get for specific scrapers, for example for "who_covid", output the
data for the level "gho" as "global". The last parameter `toplevel` defaults to 
"allregions" and is the key to use when obtaining the results of the scrapers.

    update_regional(
        outputs,
        regional_rows,
        toplevel_rows=global_rows,
        toplevel_hxltags=additional_global_hxltags,
        toplevel="global",
    )

    update_toplevel(
        outputs,
        global_rows,
        tab="world",
        regional_rows=regional_rows,
        regional_adm="GHO",
        regional_hxltags=configuration["regional"]["global"],
        regional_first=False,
    )

The first parameter of `update_regional` is a dictionary of outputs such as to Google 
Sheets, Excel or JSON. The second is the regional rows obtained from 
`get_regional_rows`. The third optional parameter is the `toplevel_rows` obtained from 
`get_toplevel_rows`. The fourth optional parameter, `toplevel_hxltags` specifies top 
level data to include. It will correspond to one row in the regional output. The last 
parameter `toplevel` defaults to "allregions" and is used as the region name.

The first parameter of `update_toplevel` is a dictionary of outputs such as to Google 
Sheets, Excel or JSON. The second is the `toplevel_rows` obtained from 
`get_toplevel_rows`. The third optional parameter is the `tab` to update and defaults to
"allregions". The fourth optional parameter is the `regional_rows` (obtained from
`get_regional_rows`) from which data for the admin given by the sixth optional parameter 
`regional_adm` is extracted. The specific regional columns to include is given by the 
seventh optional parameter `regional_hxltags`. The last parameter `regional_first` which 
defaults to `False` specifies whether columns from regional data are put in front of 
columns from top level data.

### Sources

There is an `update_sources` function to update source information and it is set up as 
follows:

    update_sources(
        runner,
        outputs,
        additional_sources=configuration["additional_sources"],
        names=names,
        secondary_runner=None,
        custom_sources=(get_report_source(configuration),),
        tab="sources",
    )

The first parameter is the Runner object. The second is a dictionary of outputs such as 
to Google Sheets, Excel or JSON. The third optional parameter `additional_sources`
enables additional sources to be declared according to a specification eg. from YAML:

    additional_sources:
      - indicator: "#food-prices"
        dataset: "global-wfp-food-prices"
      - indicator: "#affected+food+p3plus+num"
        source: "Multiple sources"
        force_date_today: True
        source_url: "https://data.humdata.org/search?q=(name:ipc-country-data%20OR%20name:cadre-harmonise)"

This allows additional HXL hashtags to be associated with a dataset date, source and 
url. The metadata for "#food-prices" is obtained from a dataset on HDX, while for
"#affected+food+p3plus+num", it is all specified. 

The fourth optional `names` parameter allows the specific scrapers for which sources are 
to be output to be chosen by name. The fifth optional parameter `secondary_runner` 
allows a second Runner object to be supplied and the sources from the scrapers 
associated with that Runner object to be included. The sixth optional parameter 
`custom_sources` allows sources that have been obtained for example from a function call
to be added directly without any processing or changes to them. They should be in the 
form: (HXL tag, source date, source, source url). The last parameter is the `tab` to 
update and defaults to "sources".

For more fine-grained control, it is also possible to obtain sources by calling 
`get_sources` on a Runner object with or without the optional `additional_sources` 
parameter:

    runner.get_sources(additional_sources=[...])

## Other Configurable Scrapers

Some other configurable scrapers are provided for specific tasks. If running specific 
scrapers rather than all, and you want to force the inclusion of the scraper in the run 
regardless of the specific scrapers given, the final parameter `force_add_to_run` in the
`add_*` call of the Runner object should be set to True. 

### Time Series Scraper

This scraper reads and outputs time series data. One or more instances can be set up as 
follows:

    runner.add_timeseries_scrapers(configuration["timeseries"], outputs)

The first parameter defines the YAML configuration where the scrapers are configured as
shown below. The second is a dictionary where the values are objects that inherit from
`BaseOutput` and which serve to send output somewhere like Google Sheets.

      casualties:
        source: "OHCHR"
        source_url: "https://data.humdata.org/dataset/ukraine-key-figures-2022"
        dataset: "ukraine-who-does-what-where-3w"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vQIdedbZz0ehRC0b4fsWiP14R7MdtU1mpmwAkuXUPElSah2AWCURKGALFDuHjvyJUL8vzZAt3R1B5qg/pub?gid=0&single=true&output=csv"
        format: "csv"
        headers: 2
        date: "Date"
        date_type: "date"
        date_hxl: "#date"
        input:
          - "Civilian casualities(OHCHR) - Killed"
          - "Civilian casualities(OHCHR) - Injured"
        output:
          - "CiviliansKilled"
          - "CiviliansInjured"
        output_hxl:
          - "#affected+killed"
          - "#affected+injured"

This reads from the given url looking for the date column given by `date`. The given
`input` columns are output along with the date using the column names given by `output`
and the HXL hashtags given by `output_hxl`.

### Aggregator

The aggregator scraper is used for aggregating data from other scrapers. One or more are
set up as shown below:

    regional_names_gho = runner.add_aggregators(
        True,
        regional_configuration["aggregate_gho"],
        "national",
        "regional",
        RegionLookup.iso3_to_regions["GHO"],
        force_add_to_run=True
    )

The first parameter is whether to use columns headers (False) or HXL hash tags (True).
The second points to a YAML configuration which is outlined below. The third is the 
level of the input scraper data to be aggregated (like national). The fourth is the 
level of the aggregated output data (like regional). The fifth is a mapping from admin 
units of the input level to admin units of the output level of form: 
`{"AFG": ("ROAP",), "MMR": ("ROAP",)}`. If the mapping is to the top level, then it is 
a list of input admins like: `("AFG", "MMR")`.

      aggregate_gho:
        "#population":
          action: "sum"
        "#affected+infected":
          action: "sum"
        ...
        "#value+funding+hrp+required+usd":
          output: "RequiredFunding"
          action: "sum"
        "#value+funding+hrp+total+usd":
          output: "Funding"
          action: "sum"
        "#value+funding+hrp+pct":
          output: "PercentFunded"
          action: "eval"
          formula: "get_fraction_str(#value+funding+hrp+total+usd, #value+funding+hrp+required+usd) if #value+funding+hrp+total+usd is not None and #value+funding+hrp+required+usd is not None else ''"
        "#access+visas+pct":
          action: "mean"
        "#access+travel+pct":
          action: "mean"
        ...
        "#affected+food+ipc+p3plus+num":
          output: "FoodInsecurityP3+"
          action: "sum"
          input:
            - "#affected+ch+food+p3plus+num"
            - "#affected+food+ipc+p3plus+num"

The configuration lists input HXL tags along with what sort of aggregation will be 
performed ("sum", "mean" or "eval" under `action`). "eval" allows combining already
aggregated columns together. Where input comes from multiple columns, these can be 
defined with `input` and the output column name with `output`.

### Resource Downloader

The resource downloader is a simple scraper that downloads resources from HDX datasets. 
One or more is set up as follows:

    res_dlds = runner.add_resourcedownloaders(configuration["copyfiles"], folder)

The first parameter is a YAML configuration shown below. The second is the folder to 
which to download (which defaults to "").

    download_resources:
      - dataset: "ukraine-border-crossings"
        format: "geojson"
        filename: "UKR_Border_Crossings.geojson"
        hxltag: "#geojson"
      - dataset: "ukraine-hostilities"
        format: "geojson"
        filename: "UKR_Hostilities.geojson"
        hxltag: "#event+loc"

The `dataset` from which to copy is specified along with the `format` of the resource to 
be copied. The output `filename` is given along with a `hxltag` that is used in the
reporting of sources (with source information taken from the HDX dataset).

## Other Utilities

### Region Lookup

A class is provided that allows creating lookups from ISO 3 country codes to regions. 
It is set up like this:

    RegionLookup.load(regional_configuration, gho_countries, {"HRPs": hrp_countries})

The configuration comes from a YAML file shown below. The second parameter is a list of
countries. The third is a dictionary containing additional regions.

    regional:
      dataset: "unocha-office-locations"
      format: "xlsx"
      iso3_header: "ISO3"
      region_header: "Regional_office"
      toplevel_region: "GHO"
      ignore:
        - "NO COVERAGE"

The configuration above reads from a `dataset` from HDX, looking for a resource of 
`format` "xlsx". In that file, it uses columns specified by `iso3_header` and
`regional_header`. Regions in the `ignore` list are not included. A country can map to 
not only what is specified in the dataset but also to `toplevel_region` (eg. GHO) which 
covers all countries given by the second parameter of the `load` call (eg. 
gho_countries) and to one or more additional regions given in the optional third 
parameter (eg. "HRPs"). For the third parameter, each key value pair is a mapping from a 
region name to a list of countries in that region. 

RegionLookup provides class variables `regions` (list of regions) and `iso3_to_region` 
(one-to-one mapping from country ISO3 code to region name based purely on the dataset 
read from the configuration). It also provides `iso3_to_regions` which is a one-to-many 
mapping from country ISO3 code to multiple region names which will include the 
`toplevel_region` and additional regions specified in the third parameter. 


# Real World Usage

This framework has been used to power the data behind a few visualisations. It can be 
helpful to examine these to see the framework being used in a complete setup. 

The project [here](https://github.com/OCHA-DAP/hdx-scraper-covid-viz) provides data for 
the [Covid Data Explorer](https://data.humdata.org/visualization/covid19-humanitarian-operations/).

The project [here](https://github.com/OCHA-DAP/hdx-scraper-ukraine-viz) provides data for 
the [Ukraine Data Explorer](https://data.humdata.org/visualization/ukraine-humanitarian-operations/).
