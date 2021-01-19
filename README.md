[![Build Status](https://github.com/OCHA-DAP/hdx-python-scraper/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-python-scraper/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-python-scraper/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-python-scraper?branch=master)

The HDX Python Scraper Library is designed to enable you to easily develop code that assembles data from one or more 
sources. 

If you have humanitarian-related data, please upload your datasets to HDX.

## Usage

The library has detailed API documentation which can be found
here: <http://ocha-dap.github.io/hdx-python-scraper/>. The code for the
library is here: <https://github.com/ocha-dap/hdx-python-scraper>.

To use the optional functions for outputting data from Pandas to JSON, Excel etc., install with:

    pip install hdx-python-scraper[pandas]

## Scraper Framework Configuration

The following is an example of how the framework is set up:

    today = datetime.now()
    adminone = AdminOne(configuration)
    population_lookup = dict()
    headers, columns, sources = get_tabular(configuration, ['AFG'], adminone, 'national', downloader, today=today, scrapers=['population'], population_lookup=population_lookup)
    assert headers == [['Population'], ['#population']]
    assert columns == [{'AFG': 38041754}]
    assert sources == [('#population', '2020-10-01', 'World Bank', 'https://data.humdata.org/organization/world-bank-group')]
    headers, columns, sources = get_tabular(configuration, ['AFG'], adminone, 'national', downloader, today=today, scrapers=['who'], population_lookup=population_lookup)
    assert headers == [['CasesPer100000', 'DeathsPer100000', 'Cases2Per100000', 'Deaths2Per100000'], ['#affected+infected+per100000', '#affected+killed+per100000', '#affected+infected+2+per100000', '#affected+killed+2+per100000']]
    assert columns == [{'AFG': '96.99'}, {'AFG': '3.41'}, {'AFG': '96.99'}, {'AFG': '3.41'}]
    assert sources == [('#affected+infected+per100000', '2020-08-06', 'WHO', 'tests/fixtures/WHO-COVID-19-global-data.csv'), ('#affected+killed+per100000', '2020-08-06', 'WHO', 'tests/fixtures/WHO-COVID-19-global-data.csv'), ('#affected+infected+2+per100000', '2020-08-06', 'WHO', 'tests/fixtures/WHO-COVID-19-global-data.csv'), ('#affected+killed+2+per100000', '2020-08-06', 'WHO', 'tests/fixtures/WHO-COVID-19-global-data.csv')]

The first parameter is a configuration. More on this later.

The second parameter is a list of country iso3s. The third is an AdminOne object. More about this can be found in 
the [HDX Python Country](https://github.com/OCHA-DAP/hdx-python-country) library, but briefly that class accepts a 
configuration as follows:

country_name_mappings defines the country name overrides we use (ie. where we deviate from the names in the OCHA 
countries and territories file).

    country_name_mappings:
      PSE: "occupied Palestinian territory"
      BOL: "Bolivia"

admin_info defines the admin level 1 names and pcodes. 

    admin_info:
      - {pcode: AF01, name: Kabul, iso2: AF, iso3: AFG, country: Afghanistan}
      - {pcode: AF02, name: Kapisa, iso2: AF, iso3: AFG, country: Afghanistan}

adm_mappings defines mappings from country name to iso3 and from admin 1 name to pcode.

    adm_mappings:
      - "Congo DR": "COD"
        "CAR": "CAF"
        "oPt": "PSE"
      - "Nord-Ouest": "HT09"
        "nord-ouest": "HT09"
    ...

adm1_name_replacements defines some find and replaces that are done to improve automatic admin1 name matching to 
pcode.

    adm1_name_replacements:
      " urban": ""
      "sud": "south"
      "ouest": "west"
    ...

adm1_fuzzy_ignore defines admin 1 names to ignore.

    adm1_fuzzy_ignore:
      - "nord"
    ...

The third parameter specifies which scraper set to process corresponding to a name in the configuration beginning
scraper_ (but without needing the scraper_ part in the parameter).

The population scraper is a special case, so it is read first so that populations can be used by subsequent 
scrapers in calculations.

### Configuration Parameter

The framework is configured by passing in a configuration. Typically this will come from a yaml file such as 
config/project_configuration.yml.

The configuration should have a mapping from the internal dictionaries to the tabs in the spreadsheet or keys in 
the JSON output file(s):

    tabs:
      world: "WorldData"
      regional: "RegionalData"
      national: "NationalData"
      subnational: "SubnationalData"
      covid_series: "CovidSeries"
      covid_trend: "CovidTrend"
      sources: "Sources"

Then the location of Google spreadsheets are defined, for prod (production), test and scratch:

    googlesheets:
      prod: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_PROD/edit"
      test: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_TEST/edit"
      scratch: "https://docs.google.com/spreadsheets/d/SPREADSHEET_KEY_SCRATCH/edit"

The json outputs are then specified. Under the key “additional”, subsets of the full json can be saved as separate 
files.

    json:
      additional_json:
        - name: "Other"
          source: "Some org"
          source_url: "https://data.humdata.org/organization/world-bank-group"
          format: "json"
          url: "https://raw.githubusercontent.com/mcarans/hdx-python-scraper/master/tests/fixtures/additional_json.json"
          jsonpath: "[*]"
      filepath: "test_tabular_all.json"
      additional:
        - filepath: "test_tabular_population.json"
          tabs:
            - tab: "national"
              key: "cumulative"
              filters:
                "#country+code": "{{countries_to_save}}"
              hxltags:
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
              hxltags:
                - "#country+code"
                - "#country+name"
                - "#population"
        - filepath: "test_tabular_other.json"
          remove:
            - "national"

Next comes additional sources. This allows additional HXL hashtags to be associated with a dataset date, source 
and url. In the ones below, the metadata is obtained from datasets on HDX.

    additional_sources:
      - indicator: "#food-prices"
        dataset: "wfp-food-prices"
      - indicator: "#vaccination-campaigns"
        dataset: "immunization-campaigns-impacted"

scraper_tabname defines a set of mini scrapers that use the framework and produce data for the tab tabname eg.

    scraper_national:
    …

#### Examples

It is helpful to look at a few example mini scrapers to see how they are configured:

The economicindex mini scraper reads the dataset “covid-19-economic-exposure-index” on HDX, taking from it dataset 
source, date of dataset and using the url of the dataset in HDX as the source url (used by the DATA link in the 
visual). The scraper framework finds the first resource that is of format “xlsx”, reads the “economic exposure” 
sheet and looks for the headers in row 1. 

adm_cols defines the column or columns in which to look for admin information. As this is a national level 
scraper, it uses the “Country” column. input_cols specifies the column(s) to use for values, in this case 
“Covid 19 Economic exposure index”. output_columns and output_hxltags define the header name(s) and HXL tag(s) to 
use for the input_cols.

     economicindex:
        format: "xlsx"
        dataset: "covid-19-economic-exposure-index"
        sheet: "economic exposure"
        headers: 1
        adm_cols:
          - "Country"
        input_cols:
          - "Covid 19 Economic exposure index"
        output_columns:
          - "EconomicExposure"
        output_hxltags:
          - "#severity+economic+num"

The population mini scraper configuration directly provides metadata for source, source_url and the download 
location given by url only taking the source date from the dataset. The scraper pulls subnational data so adm_cols 
defines both a country column “alpha_3” and an admin 1 pcode column “ADM1_PCODE”. 

input_transforms defines operations to be performed on each value in the column. In this case, the value is 
converted to either an int or float if it is possible.

     population:
        source: "Multiple Sources"
        source_url: "https://data.humdata.org/search?organization=worldpop&q=%22population%20counts%22"
        format: "csv"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vS3_uBOV_uRDSxOggfBus_pkCs6Iw9lm0nAxzwG14YF_frpm13WPKiM1oNnQ9zrUA/pub?gid=1565793974&single=true&output=csv"
        dataset: "global-humanitarian-response-plan-covid-19-administrative-boundaries-and-population-statistics"
        headers: 1
        adm_cols:
          - "alpha_3"
          - "ADM1_PCODE"
        input_cols:
          - "POPULATION"
        input_transforms:
          "POPULATION": "get_numeric_if_possible(POPULATION)"
        output_columns:
          - "Population"
        output_hxltags:
          - "#population"

The travel mini scraper reads values from the “info” and “published” columns of the source. append_cols defines 
any columns where if the same admin appears again (in this case, the same country), then that data is appended to 
the existing. keep_cols defines any columns where if the same admin appears again, the existing value is kept 
rather than replaced.

     travel:
        format: "csv"
        dataset: "covid-19-global-travel-restrictions-and-airline-information"
        headers: 1
        adm_cols:
          - "iso3"
        input_cols:
          - "info"
          - "published"
        append_cols:
          - "info"
        keep_cols:
          - "published"
        output_columns:
          - "TravelRestrictions"
          - "TravelRestrictionsPublished"
        output_hxltags:
          - "#severity+travel"
          - "#severity+date+travel"

The gam mini scraper reads from a spreadsheet that has a multiline header (headers defined as rows 3 and 4). 
Experimentation is often needed with row numbers since in my experience, they are sometimes offset from the real 
row numbers seen when opening the spreadsheet. date_col defines a column that contains date information and 
date_type specifies in what form the date information is held eg. as an integer (int) or a date. The scraper will 
for each admin, obtain the data (in this case the “National Point Estimate”) for the latest date.

     gam:
        format: "xlsx"
        dataset: "world-global-expanded-database-on-severe-wasting"
        sheet: "Trend"
        headers:
          - 3
          - 4
        adm_cols:
          - "ISO"
        date_col: "Year*"
        date_type: "int"
        input_cols:
          - "National Point Estimate"
        output_columns:
          - "Malnutrition Estimate"
        output_hxltags:
          - "#severity+malnutrition+num+national"

The covidtests mini scraper gets “new_tests” and “new_tests_per_thousand” for the latest date where a 
date_condition is satisfied which is that “new_tests” is a value greater than zero.

     covidtests:
        format: "xlsx"
        dataset: "total-covid-19-tests-performed-by-country"
        headers: 1
        sheet: 1
        date_col: "date"
        date_type: "date"
        date_condition: "new_tests is not None and new_tests > 0"
        adm_cols:
          - "iso_code"
        input_cols:
          - "new_tests"
          - "new_tests_per_thousand"
        output_columns:
          - "New Tests"
          - "New Tests Per Thousand"
        output_hxltags:
          - "#affected+tested"
          - "#affected+tested+per1000"

The oxcgrt mini scraper reads from a data source that has HXL tags and these can be used instead of the header 
names provided use_hxl is set to True. By default all the HXLated columns are read with the admin related ones 
inferred and the rest taken as values except if defined as a date_col.

     oxcgrt:
        format: "csv"
        dataset: "oxford-covid-19-government-response-tracker"
        headers: 1
        use_hxl: True
        date_col: "#date"
        date_type: "int"

In the imperial mini scraper, output_columns and output_hxltags are defined which specify which columns and HXL 
tags in the HXLated file should be used rather than using all HXLated columns.

     imperial:
        format: "xlsx"
        dataset: "imperial-college-covid-19-projections"
        sheet: 1
        headers: 1
        use_hxl: True
        output_columns:
          - "Imp: Total Cases(min)"
          - "Imp: Total Cases(max)"
          - "Imp: Total Deaths(min)"
          - "Imp: Total Deaths(max)"
        output_hxltags:
          - "#affected+infected+min+imperial"
          - "#affected+infected+max+imperial"
          - "#affected+killed+min+imperial"
          - "#affected+killed+max+imperial"

The idmc mini scraper reads 2 HXLated columns defined in input_cols. In input_transforms, a cast to int is 
performed if the value is not None or it is set to 0. einput_cols defines new column(s) that can be combinations 
of the other columns in input_cols. In this case, einput_cols specifies a new column which sums the 2 columns in 
input_cols. That new column is given a header and a HXL tag (in output_columns and output_hxltags).

     idmc:
        format: "csv"
        dataset: "idmc-internally-displaced-persons-idps"
        headers: 1
        use_hxl: True
        date_col: "#date+year"
        date_type: "int"
        input_cols:
          - "#affected+idps+ind+stock+conflict"
          - "#affected+idps+ind+stock+disaster"
        input_transforms:
          "#affected+idps+ind+stock+conflict": "int(#affected+idps+ind+stock+conflict) if #affected+idps+ind+stock+conflict else 0"
          "#affected+idps+ind+stock+disaster": "int(#affected+idps+ind+stock+disaster) if #affected+idps+ind+stock+disaster else 0"
        input_cols:
          - "#affected+idps+ind+stock+conflict + #affected+idps+ind+stock+disaster"
        output_columns:
          - "TotalIDPs"
        output_hxltags:
          - "#affected+displaced"

The needs mini scraper takes data for the latest available date for each country. subsets allows the definition of 
multiple indicators by way of filters. A filter is defined for each indicator (in this case there is one) which 
contains one or more filters of the form column=value. The pipe (|) is used as a separator - it means “and” not 
“or”.

     needs:
        format: "xlsx"
        dataset: "global-humanitarian-overview-2020-figures"
        sheet: "Raw Data"
        headers: 1
        adm_cols:
          - "Country Code"
        date_col: "Year"
        date_type: "int"
        subsets:
          - filter: "Metric=People in need|PiN Value for Dataviz=yes"
            input_cols:
              - "Value"
            output_columns:
              - "PeopleInNeed"
            output_hxltags:
              - "#affected+inneed"

The population mini scraper matches admin names only using exact matching (adm_exact is set to True) rather than 
the default which tries fuzzy matching in the event of a failure to match exactly. In this case, the fuzzy 
matching was incorrect. Note: a future fix in HDX Python Country may render this flag unnecessary as it will only 
fuzzy match if the input has more than 3 characters. 

     population:
        source: "World Bank"
        source_url: "https://data.humdata.org/organization/world-bank-group"
        format: "xls"
        url: "http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=excel&dataformat=list"
        sheet: "Data"
        headers: 3
        adm_cols:
          - "Country Code"
        adm_exact: True
        date_col: "Year"
        date_type: "int"
        date_condition: "Value != ''"
        input_cols:
          - "Value"
        output_columns:
          - "Population"
        output_hxltags:
          - "#population"

The sadd mini scraper reads data from the dataset “covid-19-sex-disaggregated-data-tracker”. It filters that data 
using data from another file, the url of which is defined in external_filter. Specifically, it cuts down the sadd 
data to only include countries listed in the  “#country+code+v_iso2” column of the external_filter file.
 
     sadd:
         format: "csv"
         dataset: "covid-19-sex-disaggregated-data-tracker"
         headers: 1
         external_filter:
           url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vR9PhPG7-aH0EkaBGzXYlrO9252gqs-UuKIeDQr9D3pOLBOdQ_AoSwWi21msHsdyT7thnjuhSY6ykSX/pub?gid=434885896&single=true&output=csv"
           hxltags:
             - "#country+code+v_iso2"
     ...

The fsnwg mini scraper first applies a sort to the data it reads. The reverse sort is based on the keys 
“reference_year” and “reference_code”. adm_cols defines a country column adm0_pcod3 and three admin 1 level 
columns (“adm1_pcod2”, “adm1_pcod3”, “adm1_name”) which are examined consecutively until a match with the internal 
admin 1 is made. 

date_col is comprised of the amalgamation of two columns “reference_year” and “reference_code” 
(corresponding to the two columns which were used for sorting earlier). sum_cols is used to sum values in a 
column. For example, the formula “get_fraction_str(phase3, population)” takes the sum of all phase 3 values for an 
admin 1 and divides it by the sum of all population values for an admin 1. 

mustbepopulated determines if values are included or not. If it is True, then only when all columns in input_cols 
for the row are populated is the value included. This means that when using multiple summed columns together in a 
formula, the number of values that were summed in each column will be the same. The last formula uses 
“#population” which is replaced by the population for the admin (which has been previously obtained by another 
mini scraper).

     fsnwg:
        format: "xlsx"
        dataset: "cadre-harmonise"
        headers: 1
        sheet: 1
        sort:
          reverse: True
          keys:
            - "reference_year"
            - "reference_code"
        adm_cols:
          - "adm0_pcod3"
          - - "adm1_pcod2"
            - "adm1_pcod3"
            - "adm1_name"
        date_col:
          - "reference_year"
          - "reference_code"
        date_type: "int"
        subsets:
          - filter: "chtype=current"
            input_cols:
              - "phase3"
              - "phase4"
              - "phase5"
              - "phase35"
              - "population"
            input_transforms:
              "phase3": "float(phase3)"
              "phase4": "float(phase4)"
              "phase5": "float(phase5)"
              "phase35": "float(phase35)"
              "population": "float(population)"
            sum_cols:
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
            output_columns:
              - "FoodInsecurityCHP3"
              - "FoodInsecurityCHP4"
              - "FoodInsecurityCHP5"
              - "FoodInsecurityCHP3+"
              - "FoodInsecurityCHAnalysed"
            output_hxltags:
              - "#affected+ch+food+p3+pct"
              - "#affected+ch+food+p4+pct"
              - "#affected+ch+food+p5+pct"
              - "#affected+ch+food+p3plus+pct"
              - "#affected+ch+food+analysed+pct"

The who_subnational mini scraper defines two values in input_ignore_vals which if found are ignored. Since 
mustbepopulated is True, then only when all columns in input_cols for the row are populated and do not contain 
either “-2222” or “-4444” is the value included in the sum of any column used in sum_cols. 

     who_subnational:
        source: "WHO"
        format: "csv"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vRfjaIXE1hvEIXD66g6cuCbPrGdZkx6vLIgXO_znVbjQ-OgwfaI1kJPhxhgjw2Yg08CmtBuMLAZkTnu/pub?gid=337443769&single=true&output=csv"
        headers: 1
        adm_cols:
          - "iso"
          - "Admin1"
        date_col: "Year"
        date_type: "int"
        subsets:
          - filter: "Vaccine=HepB1"
            input_cols:
              - "Numerator"
              - "Denominator"
            input_ignore_vals:
              - "-2222"
              - "-4444"
            input_transforms:
              Numerator: "float(Numerator)"
              Denominator: "float(Denominator)"
            sum_cols:
              - formula: "get_fraction_str(Numerator, Denominator)"
                mustbepopulated: True
            output_columns:
              - "HepB1 Coverage"
            output_hxltags:
              - "#population+hepb1+pct+vaccinated"

The access mini scraper provides different sources for each HXL tag by providing dictionaries instead of strings 
in source and source_url. It maps specific HXL tags by key to sources or falls back on a “default_source” and 
“default_url” for all unspecified HXL tags.

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
        format: "csv"
        url: "https://docs.google.com/spreadsheets/d/e/2PACX-1vRSzJzuyVt9i_mkRQ2HbxrUl2Lx2VIhkTHQM-laE8NyhQTy70zQTCuFS3PXbhZGAt1l2bkoA4_dAoAP/pub?gid=1565063847&single=true&output=csv"
        headers: 1
        use_hxl: True
        input_transforms:
          "#access+visas+pct": "get_numeric_if_possible(#access+visas+pct)"
          "#access+travel+pct": "get_numeric_if_possible(#access+travel+pct)"
          "#activity+cerf+project+insecurity+pct": "get_numeric_if_possible(#activity+cerf+project+insecurity+pct)"
          "#activity+cbpf+project+insecurity+pct": "get_numeric_if_possible(#activity+cbpf+project+insecurity+pct)"
          "#population+education": "get_numeric_if_possible(#population+education)"



