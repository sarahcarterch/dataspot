import logging
from typing import List

from src.dataspot_mapping import BaseDataspotMapping

class ODSDataspotMapping(BaseDataspotMapping):
    """
    A lookup table that maps ODS IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles only datasets for now.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str, scheme: str):
        """
        Initialize the mapping table for datasets.
        The CSV filename is derived from the database_name and scheme.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 Example: "feature-staatskalender_DNK_ods-dataspot-mapping.csv"
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
        """
        super().__init__(database_name, "ods_id", "ods-dataspot", scheme)
        
    def get_all_ods_ids(self) -> List[str]:
        """
        Get a list of all ODS IDs in the mapping.

        Returns:
            List[str]: A list of all ODS IDs
        """
        return self.get_all_ids()