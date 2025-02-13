from typing import List

from hdx.location.adminlevel import AdminLevel


def complete_admins(
    admins: List[AdminLevel],
    countryiso3: str,
    provider_adm_names: List[str],
    adm_codes: List[str],
    adm_names: List[str],
) -> None:
    parent = None
    for i, adm_code in enumerate(adm_codes):
        if adm_code:
            adm_name = admins[i].pcode_to_name.get(adm_code, "")
            adm_names[i] = adm_name
            continue
        provider_adm_name = provider_adm_names[i]
        if not provider_adm_name:
            continue
        adm_code, _ = admins[i].get_pcode(
            countryiso3, provider_adm_name, parent=parent
        )
        if adm_code:
            adm_codes[i] = adm_code
            adm_name = admins[i].pcode_to_name.get(adm_code, "")
            adm_names[i] = adm_name
            parent = adm_code
