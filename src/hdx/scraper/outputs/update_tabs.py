import logging
from copy import deepcopy

from hdx.location.country import Country

logger = logging.getLogger(__name__)

regional_headers = (("regionnames",), ("#region+name",))
national_headers = (
    ["iso3", "countryname"],
    ["#country+code", "#country+name"],
)
subnational_headers = (
    ("iso3", "countryname", "adm1_pcode", "adm1_name"),
    ("#country+code", "#country+name", "#adm1+code", "#adm1+name"),
)
sources_headers = (
    ("Indicator", "Date", "Source", "Url"),
    ("#indicator+name", "#date", "#meta+source", "#meta+url"),
)


def update_tab(outputs, name, data):
    if not data:
        return
    logger.info(f"Updating tab: {name}")
    for output in outputs.values():
        output.update_tab(name, data)


def get_toplevel_rows(
    runner, names=None, overrides=dict(), toplevel="allregions"
):
    return runner.get_rows(
        toplevel, ("value",), names=names, overrides=overrides
    )


def get_regional_rows(runner, regional, names=None, level="regional"):
    return runner.get_rows(
        level, regional, regional_headers, (lambda adm: adm,), names=names
    )


def update_toplevel(
    outputs,
    toplevel_rows,
    tab="allregions",
    regional_rows=tuple(),
    regional_adm="ALL",
    regional_hxltags=None,
    regional_first=False,
):
    if not toplevel_rows:
        toplevel_rows = [list(), list(), list()]
    if regional_rows:
        adm_header = regional_rows[1].index("#region+name")
        rows_to_insert = (list(), list(), list())
        for row in regional_rows[2:]:
            if row[adm_header] == regional_adm:
                for i, hxltag in enumerate(regional_rows[1]):
                    if hxltag == "#region+name":
                        continue
                    if regional_hxltags and hxltag not in regional_hxltags:
                        continue
                    rows_to_insert[0].append(regional_rows[0][i])
                    rows_to_insert[1].append(hxltag)
                    rows_to_insert[2].append(row[i])
        if regional_first:
            toplevel_rows[0] = rows_to_insert[0] + toplevel_rows[0]
            toplevel_rows[1] = rows_to_insert[1] + toplevel_rows[1]
            toplevel_rows[2] = rows_to_insert[2] + toplevel_rows[2]
        else:
            toplevel_rows[0] += rows_to_insert[0]
            toplevel_rows[1] += rows_to_insert[1]
            toplevel_rows[2] += rows_to_insert[2]
    update_tab(outputs, tab, toplevel_rows)


def update_regional(
    outputs,
    regional_rows,
    toplevel_rows=tuple(),
    additional_toplevel_headers=tuple(),
    tab="regional",
    toplevel="allregions",
):
    if not regional_rows:
        return
    toplevel_values = dict()
    toplevel_hxltags = dict()
    if toplevel_rows:
        for i, header in enumerate(toplevel_rows[0]):
            if header in additional_toplevel_headers:
                toplevel_values[header] = toplevel_rows[2][i]
                if header not in regional_rows[0]:
                    toplevel_hxltags[header] = toplevel_rows[1][i]
    adm_header = regional_rows[1].index("#region+name")
    found_adm = False

    def add_value(row):
        value_found = False
        for i, header in enumerate(regional_rows[0]):
            value = toplevel_values.get(header)
            if value is None:
                continue
            row[i] = value
            value_found = True
        for header, hxltag in toplevel_hxltags.items():
            value = toplevel_values.get(header)
            if value is None:
                continue
            regional_rows[0].append(header)
            regional_rows[1].append(hxltag)
            row.append(value)
            value_found = True
        return value_found

    for row in regional_rows[2:]:
        if row[adm_header] == toplevel:
            add_value(row)
            found_adm = True
            break
    if not found_adm:
        row = [toplevel]
        for _ in regional_rows[0][1:]:
            row.append(None)
        if add_value(row):
            regional_rows.append(row)
    length = len(regional_rows[0])
    for row in regional_rows[2:]:
        while len(row) < length:
            row.append(None)
    update_tab(outputs, tab, regional_rows)


def update_national(
    runner,
    countries,
    outputs,
    names=None,
    flag_countries=None,
    iso3_to_region=None,
    ignore_regions=tuple(),
    level="national",
    tab="national",
):
    headers = deepcopy(national_headers)
    fns = [
        lambda adm: adm,
        lambda adm: Country.get_country_name_from_iso3(adm),
    ]

    if flag_countries:
        headers[0].append(flag_countries["header"])
        headers[1].append(flag_countries["hxltag"])
        isfc_fn = (
            lambda adm: "Y" if adm in flag_countries["countries"] else "N"
        )
        fns.append(isfc_fn)

    if iso3_to_region:
        headers[0].append("region")
        headers[1].append("#region+name")

        def region_fn(adm):
            regions = sorted(list(iso3_to_region[adm]))
            for region in reversed(regions):
                if region in ignore_regions:
                    regions.remove(region)
            return "|".join(regions)

        fns.append(region_fn)

    rows = runner.get_rows(level, countries, headers, fns, names=names)
    if rows:
        update_tab(outputs, tab, rows)


def update_subnational(
    runner,
    adminone,
    outputs,
    names=None,
    level="subnational",
    tab="subnational",
):
    def get_country_name(adm):
        countryiso3 = adminone.pcode_to_iso3[adm]
        return Country.get_country_name_from_iso3(countryiso3)

    fns = (
        lambda adm: adminone.pcode_to_iso3[adm],
        get_country_name,
        lambda adm: adm,
        lambda adm: adminone.pcode_to_name[adm],
    )
    rows = runner.get_rows(
        level, adminone.pcodes, subnational_headers, fns, names=names
    )
    update_tab(outputs, tab, rows)


def update_sources(
    runner,
    configuration,
    outputs,
    names=None,
    secondary_runner=None,
    additional_sources=list(),
    tab="sources",
):
    sources = runner.get_sources(
        names=names, additional_sources=configuration["additional_sources"]
    )
    if secondary_runner:
        secondary_sources = secondary_runner.get_sources()
        sources.extend(secondary_sources)
    sources.extend(additional_sources)
    update_tab(outputs, tab, list(sources_headers) + sources)
