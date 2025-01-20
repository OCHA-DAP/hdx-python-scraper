"""Populate the sector mapping."""

from .lookup import Lookup


class Sector(Lookup):
    def __init__(self):
        super().__init__("sector_configuration.yaml", Sector)
