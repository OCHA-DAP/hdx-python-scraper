from .lookup import Lookup


class OrgType(Lookup):
    """Populate the org type mapping."""

    def __init__(self):
        super().__init__("org_type_configuration.yaml", OrgType)
