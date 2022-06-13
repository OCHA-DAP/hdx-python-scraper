from hdx.scraper.utilities.region_lookup import RegionLookup


class TestRegionLookup:
    def test_regionlookup(self, configuration):
        gho_countries = configuration["gho"]
        hrp_countries = configuration["HRPs"]

        regional_configuration = configuration["regional"]
        RegionLookup.load(
            regional_configuration, gho_countries, {"HRPs": hrp_countries}
        )
        assert RegionLookup.iso3_to_region["BRA"] == "ROLAC"
        assert RegionLookup.iso3_to_region["PAK"] == "ROAP"
        assert RegionLookup.iso3_to_region["UKR"] == "ROCCA"
        assert RegionLookup.iso3_to_regions["GHO"]["PAK"] == {
            "HRPs",
            "GHO",
            "ROAP",
        }
        assert RegionLookup.iso3_to_regions["HRPs"]["UKR"] == {
            "HRPs",
            "GHO",
            "ROCCA",
        }
        assert RegionLookup.iso3_to_regions["GHO"]["UKR"] == {
            "HRPs",
            "GHO",
            "ROCCA",
        }
        RegionLookup.iso3_to_region = dict()
        RegionLookup.iso3_to_regions = dict()

        regional_configuration = configuration["regional_ignore"]
        RegionLookup.load(
            regional_configuration, gho_countries, {"HRPs": hrp_countries}
        )
        assert RegionLookup.iso3_to_region["BRA"] == "ROLAC"
        assert RegionLookup.iso3_to_region["PAK"] == "ROAP"
        assert RegionLookup.iso3_to_region.get("UKR") is None
        assert RegionLookup.iso3_to_regions["GHO"]["PAK"] == {"GHO", "ROAP"}
        assert RegionLookup.iso3_to_regions.get("HRPs") is None
        assert RegionLookup.iso3_to_regions["GHO"]["UKR"] == {"GHO"}
        RegionLookup.iso3_to_region = dict()
        RegionLookup.iso3_to_regions = dict()
