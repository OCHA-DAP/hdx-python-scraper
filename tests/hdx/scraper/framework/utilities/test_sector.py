from hdx.scraper.framework.utilities.sector import Sector


class TestSector:
    def test_sector(self, configuration):
        sector = Sector()
        assert sector.get_sector_code("child protection") == "PRO-CPN"
        assert (
            sector.get_sector_code("gestion des sites daccueil temporaires")
            == "SHL"
        )
