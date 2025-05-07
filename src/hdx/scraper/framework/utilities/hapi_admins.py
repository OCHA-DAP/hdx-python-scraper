from typing import List, Tuple

from hdx.location.adminlevel import AdminLevel


def complete_admins(
    admins: List[AdminLevel],
    countryiso3: str,
    provider_adm_names: List,
    adm_codes: List,
    adm_names: List,
    fuzzy_match: bool = True,
) -> Tuple[int, List[str]]:
    """Use information from adm_codes to populate adm_names and from
    provider_adm_names to populate adm_codes with outptu of the admin level
    and arnings for unknown and mismatched p-codes. All provided lists
    should be of the same length.

    Args:
        admins (List[AdminLevel]): List of AdminLevel objects
        countryiso3 (str): Country ISO3 code
        provider_adm_names (List): List of provider adm names
        adm_codes (List): List of adm codes
        adm_names (List): List of adm names
        fuzzy_match (bool): Whether to use fuzzy matching. Default is True.

    Returns:
        Tuple[int, List[str]]: Admin level and warnings
    """

    warnings = []
    child = None
    adm_level = len(provider_adm_names)

    def check_unknown_pcode(adm_code: str, pcode: str) -> str:
        if pcode:
            warnings.append(f"PCode unknown {adm_code}->{pcode} ({warntxt})")
            return pcode
        else:
            warnings.append(f"PCode unknown {adm_code}->''")
            return ""

    for i, provider_adm_name in reversed(list(enumerate(provider_adm_names))):
        adm_code = adm_codes[i]
        parent = admins[i].pcode_to_parent.get(adm_code)
        if not parent and i > 0:
            parent = adm_codes[i - 1]
        if not provider_adm_name:
            provider_adm_name = ""
            provider_adm_names[i] = ""
        if child:
            pcode = admins[i + 1].pcode_to_parent.get(child)
            warntxt = "parent"
        elif provider_adm_name:
            pcode, _ = admins[i].get_pcode(
                countryiso3,
                provider_adm_name,
                parent=parent,
                fuzzy_match=fuzzy_match,
            )
            warntxt = f"provider_adm{i + 1}_name"
        else:
            pcode = None
        if adm_code:
            if adm_code not in admins[i].pcodes:
                if admins[i].looks_like_pcode(adm_code):
                    adj_adm_code = admins[i].convert_admin_pcode_length(
                        countryiso3, adm_code, parent=parent
                    )
                    if adj_adm_code:
                        warnings.append(f"PCode length {adm_code}->{adj_adm_code}")
                        adm_code = adj_adm_code
                    else:
                        adm_code = check_unknown_pcode(adm_code, pcode)
                else:
                    adm_code = check_unknown_pcode(adm_code, pcode)
            elif pcode and adm_code != pcode:
                if child:
                    warnings.append(f"PCode mismatch {adm_code}->{pcode} ({warntxt})")
                    adm_code = pcode
                else:
                    warnings.append(f"PCode mismatch {adm_code} != {provider_adm_name}")
        elif pcode:
            adm_code = pcode
        else:
            adm_code = ""
        adm_codes[i] = adm_code
        if adm_code:
            adm_names[i] = admins[i].pcode_to_name.get(adm_code, "")
            child = adm_code
        else:
            adm_names[i] = ""
            if provider_adm_name == "":
                adm_level -= 1
    return adm_level, warnings


def pad_admins(
    provider_adm_names: List[str],
    adm_codes: List[str],
    adm_names: List[str],
    adm_level: int = 2,
) -> None:
    """Pad lists to size given in adm_level adding as many "" as needed.

    Args:
        provider_adm_names (List): List of provider adm names
        adm_codes (List): List of adm codes
        adm_names (List): List of adm names
        adm_level (int): Admin level to which to pad. Default is 2.

    Returns:
        Tuple[int, List[str]]: Admin level and warnings
    """

    for i in range(len(provider_adm_names), adm_level):
        provider_adm_names.append("")
        adm_codes.append("")
        adm_names.append("")
