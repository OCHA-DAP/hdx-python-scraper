from hdx.scraper.framework.utilities.org_type import OrgType
from hdx.scraper.framework.utilities.sector import Sector


class TestLookup:
    def test_sector(self, configuration):
        sector = Sector()
        assert sector.get_code("child protection") == "PRO-CPN"
        assert sector.get_code("gestion des sites daccueil temporaires") == "SHL"
        assert "Intersectoral" in sector.get_code_to_name()
        assert sector.get_name("PRO-MIN") == "Mine Action"
        assert sector.get_name("xxx") is None

    def test_org_type(self, configuration):
        org_type = OrgType()
        assert org_type.get_code("embassy") == "434"
        assert org_type.get_code("ong int") == "437"
        assert org_type.get_name("xxx", "") == ""
