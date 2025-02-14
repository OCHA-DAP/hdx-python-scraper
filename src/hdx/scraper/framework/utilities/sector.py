from .lookup import Lookup


class Sector(Lookup):
    """Populate the sector mapping."""

    def __init__(self):
        super().__init__("sector_configuration.yaml", Sector)
