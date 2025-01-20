"""Populate the org type mapping."""

from .lookup import Lookup


class OrgType(Lookup):
    def __init__(self):
        super().__init__("org_type_configuration.yaml", OrgType)
