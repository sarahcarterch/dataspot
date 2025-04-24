import logging
from typing import List

from dataspot_mapping import BaseDataspotMapping

class StaatskalenderDataspotMapping(BaseDataspotMapping):
    """
    A lookup table that maps Staatskalender IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles organizational units.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str):
        """
        Initialize the mapping table for organizational units.
        The CSV filename is derived from the database_name.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 The file will be named "staatskalender-dataspot-mapping_{database_name}.csv".
        """
        super().__init__(database_name, "staatskalender_id", "staatskalender-dataspot")
        
    def get_all_staatskalender_ids(self) -> List[str]:
        """
        Get a list of all Staatskalender IDs in the mapping.

        Returns:
            List[str]: A list of all Staatskalender IDs
        """
        return self.get_all_ids() 