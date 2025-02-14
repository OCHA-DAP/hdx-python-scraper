from typing import List, Tuple

from hdx.location.adminlevel import AdminLevel


def complete_admins(
    admins: List[AdminLevel],
    countryiso3: str,
    provider_adm_names: List,
    adm_codes: List,
    adm_names: List,
) -> Tuple[int, List[str]]:
    warnings = []
    parent = None
    adm_level = len(provider_adm_names)
    for i, provider_adm_name in reversed(list(enumerate(provider_adm_names))):
        adm_code = adm_codes[i]
        if not provider_adm_name:
            provider_adm_name = ""
            provider_adm_names[i] = ""
        if parent:
            pcode = admins[i + 1].pcode_to_parent.get(parent)
            warntxt = "parent"
        elif provider_adm_name:
            pcode, _ = admins[i].get_pcode(
                countryiso3, provider_adm_name, parent=parent
            )
            warntxt = f"provider_adm{i + 1}_name"
        else:
            pcode = None
        if adm_code:
            if adm_code not in admins[i].pcodes:
                if pcode:
                    warnings.append(
                        f"PCode unknown {adm_code}->{pcode} ({warntxt})"
                    )
                    adm_code = pcode
                else:
                    warnings.append(f"PCode unknown {adm_code}->''")
                    adm_code = ""
            elif pcode and adm_code != pcode:
                warnings.append(
                    f"PCode mismatch {adm_code}->{pcode} ({warntxt})"
                )
                adm_code = pcode
        elif pcode:
            adm_code = pcode
        else:
            adm_code = ""
        adm_codes[i] = adm_code
        if adm_code:
            adm_names[i] = admins[i].pcode_to_name.get(adm_code, "")
            parent = adm_code
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
    for i in range(len(provider_adm_names), adm_level):
        provider_adm_names.append("")
        adm_codes.append("")
        adm_names.append("")
