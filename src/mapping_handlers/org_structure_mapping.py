from typing import List

from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping

class OrgStructureMapping(BaseDataspotMapping):
    """
    A lookup table that maps Staatskalender IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles organizational units.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str, scheme: str):
        """
        Initialize the mapping table for organizational units.
        The CSV filename is derived from the database_name and scheme.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 Example: "feature-staatskalender_DNK_staatskalender-dataspot-mapping.csv"
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
        """
        super().__init__(database_name, "staatskalender_id", "staatskalender-dataspot", scheme)
