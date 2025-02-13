from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
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
        provider_adm_names = ["Kabal", "Paghman"]
        adm_codes = ["AF01", ""]
        adm_names = ["", ""]
        complete_admins(
            admins, countryiso3, provider_adm_names, adm_codes, adm_names
        )
        assert adm_codes == ["AF01", "AF0102"]
        assert adm_names == ["Kabul", "Paghman"]
