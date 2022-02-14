def get_fallbacks(fallback_data, level):
    data = fallback_data[f"{level}_data"]
    sources = fallback_data["sources"]
    if level == "national":
        admin_hxltag = "#country+code"
    else:
        admin_hxltag = "global"
    fallbacks = {
        "data": data,
        "admin hxltag": admin_hxltag,
        "sources": sources,
        "sources hxltags": [
            "#indicator+name",
            "#date",
            "#meta+source",
            "#meta+url",
        ],
    }
    return fallbacks


def bool_assert(actual, expected, msg):
    assert actual is expected, msg
