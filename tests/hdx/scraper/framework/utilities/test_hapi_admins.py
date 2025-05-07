from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.hapi_admins import (
    complete_admins,
    pad_admins,
)
from hdx.scraper.framework.utilities.reader import Read


class TestAdmins:
    def test_complete_admins(self, configuration):
        countryiso3 = "AFG"
        admins = []
        reader = Read.get_reader("hdx")
        for i in range(2):
            admin = AdminLevel(admin_level=i + 1, retriever=reader)
            admin.setup_from_url(countryiso3s=(countryiso3,))
            admin.load_pcode_formats()
            admins.append(admin)

        provider_adm_names = ["", ""]
        adm_codes = ["", ""]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["", ""]
        assert adm_names == ["", ""]
        assert adm_level == 0
        assert warnings == []

        provider_adm_names = ["Kabul", ""]
        adm_codes = ["AF01", ""]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", ""]
        assert adm_names == ["Kabul", ""]
        assert adm_level == 1
        assert warnings == []

        provider_adm_names = ["Kabul", ""]
        adm_codes = ["AF01", "AF010X"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", ""]
        assert adm_names == ["Kabul", ""]
        assert adm_level == 1
        assert warnings == ["PCode unknown AF010X->''"]

        provider_adm_names = ["Kabal", "Paghman"]
        adm_codes = ["AF01", "AF0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Ghazni", "Qara Bagh"]
        adm_codes = ["AF11", "AF1111"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF11", "AF1111"]
        assert adm_names == ["Ghazni", "Qara Bagh"]
        assert adm_level == 2
        assert warnings == []

        # There are two Qara Baghs!
        provider_adm_names = ["Kabul", "Qara Bagh"]
        adm_codes = ["AF01", "AF0114"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0114"]
        assert adm_names == ["Kabul", "Qara Bagh"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Kabul", "Qara Bagh"]
        adm_codes = ["AF01", ""]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0114"]
        assert adm_names == ["Kabul", "Qara Bagh"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Kabal", "Paghman"]
        adm_codes = ["AF01", ""]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF01", "AF01XX"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == ["PCode unknown AF01XX->AF0102 (provider_adm2_name)"]

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["", "AF0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF0X", "AF0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == ["PCode unknown AF0X->AF01 (parent)"]

        provider_adm_names = ["Kabul", ""]
        adm_codes = ["AF01", "AF0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert provider_adm_names == ["Kabul", ""]
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == []

        provider_adm_names = ["Kabul", None]
        adm_codes = ["AF01", None]
        adm_names = [None, None]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", ""]
        assert adm_names == ["Kabul", ""]
        assert adm_level == 1
        assert warnings == []

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF02", "AF0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == ["PCode mismatch AF02->AF01 (parent)"]

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF01", "AF0103"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert provider_adm_names == ["Kabul", "Paghman"]
        assert adm_codes == ["AF01", "AF0103"]
        assert adm_names == ["Kabul", "Chahar Asyab"]
        assert adm_level == 2
        assert warnings == ["PCode mismatch AF0103 != Paghman"]

        provider_adm_names = ["Kabal", "Paghman"]
        adm_codes = ["AF01", "AFG0102"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == ["PCode length AFG0102->AF0102"]

        provider_adm_names = ["Kabal", "Paghman"]
        adm_codes = ["AF01", "AFG9999"]
        adm_names = ["", ""]
        adm_level, warnings = complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
        assert adm_level == 2
        assert warnings == ["PCode unknown AFG9999->AF0102 (provider_adm2_name)"]

    def test_pad_admins(self):
        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF01", "AF0102"]
        adm_names = ["", ""]
        pad_admins(provider_adm_names, adm_codes, adm_names, 3)
        assert provider_adm_names == ["Kabul", "Paghman", ""]
        assert adm_codes == ["AF01", "AF0102", ""]
        assert adm_names == ["", "", ""]

        provider_adm_names = ["Kabul"]
        adm_codes = ["AF01"]
        adm_names = [""]
        pad_admins(provider_adm_names, adm_codes, adm_names)
        assert provider_adm_names == ["Kabul", ""]
        assert adm_codes == ["AF01", ""]
        assert adm_names == ["", ""]

        provider_adm_names = []
        adm_codes = []
        adm_names = []
        pad_admins(provider_adm_names, adm_codes, adm_names)
        assert provider_adm_names == ["", ""]
        assert adm_codes == ["", ""]
        assert adm_names == ["", ""]

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF01", "AF0102"]
        adm_names = ["", ""]
        pad_admins(provider_adm_names, adm_codes, adm_names, 1)
        assert provider_adm_names == ["Kabul", "Paghman"]
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["", ""]

        provider_adm_names = ["Kabul", "Paghman"]
        adm_codes = ["AF01", "AF0102"]
        adm_names = ["", ""]
        pad_admins(provider_adm_names, adm_codes, adm_names, 2)
        assert provider_adm_names == ["Kabul", "Paghman"]
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["", ""]
