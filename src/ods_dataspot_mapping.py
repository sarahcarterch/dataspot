import csv
import os
from typing import Tuple, Optional, Dict


class ODSDataspotMapping:
    """
    A lookup table that maps ODS IDs to Dataspot UUIDs and HREFs.
    Stores the mapping in a CSV file for persistence.
    """

    def __init__(self, database_name: str = None, csv_file_path: str = None):
        """
        Initialize the mapping table.
        
        Args:
            database_name (str, optional) (Recommended): Name of the database to use for file naming.
                                          If provided, the file will be named "ods-dataspot-mapping_{database_name}.csv".
            csv_file_path (str, optional): Path to the CSV file for storing the mapping.
                                          Default is "ods_dataspot_mapping.csv" in the current directory.
                                          This parameter is ignored if database_name is provided.
        """
        self.logger = logging.getLogger(__name__)
        
        if database_name:
            self.csv_file_path = f"ods-dataspot-mapping_{database_name}.csv"
        else:
            self.csv_file_path = csv_file_path or "ods-dataspot-mapping.csv"
        
        self.mapping: Dict[str, Tuple[str, str]] = {}
        self._load_mapping()
    
    def _load_mapping(self) -> None:
        """Load the mapping from the CSV file if it exists."""
        if not os.path.exists(self.csv_file_path):
            # Create the file with headers if it doesn't exist
            with open(self.csv_file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ods_id', 'uuid', 'href'])
            return
        
        with open(self.csv_file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            self.mapping = {
                row['ods_id']: (row['uuid'], row['href'])
                for row in reader
            }
    
    def _save_mapping(self) -> None:
        """Save the current mapping to the CSV file."""
        # Sort the items by ods_id
        sorted_mapping = sorted(self.mapping.items(), key=lambda x: x[0])
        with open(self.csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ods_id', 'uuid', 'href'])
            for ods_id, (uuid, href) in sorted_mapping:
                writer.writerow([ods_id, uuid, href])
    
    def get_entry(self, ods_id: str) -> Optional[Tuple[str, str]]:
        """
        Get the UUID and HREF for an ODS ID if it exists.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[Tuple[str, str]]: A tuple of (uuid, href) if found, None otherwise
        """
        return self.mapping.get(ods_id)
    
    def add_entry(self, ods_id: str, uuid: str, href: str) -> None:
        """
        Add a new mapping entry or update an existing one.
        
        Args:
            ods_id (str): The ODS ID
            uuid (str): The Dataspot UUID
            href (str): The Dataspot HREF
        """
        self.mapping[ods_id] = (uuid, href)
        self._save_mapping()
    
    def remove_entry(self, ods_id: str) -> bool:
        """
        Remove a mapping entry if it exists.
        
        Args:
            ods_id (str): The ODS ID to remove
            
        Returns:
            bool: True if the entry was removed, False if it didn't exist
        """
        if ods_id in self.mapping:
            del self.mapping[ods_id]
            self._save_mapping()
            return True
        return False
    
    def get_uuid(self, ods_id: str) -> Optional[str]:
        """
        Get just the UUID for an ODS ID.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[str]: The UUID if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[0] if entry else None
    
    def get_href(self, ods_id: str) -> Optional[str]:
        """
        Get just the HREF for an ODS ID.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[str]: The HREF if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[1] if entry else None