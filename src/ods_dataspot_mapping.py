import csv
import os
import uuid # Only for validating dataspot uuid
import logging
from typing import Tuple, Optional, Dict, List


class ODSDataspotMapping:
    """
    A lookup table that maps ODS IDs to Dataspot UUIDs, HREFs, and inCollection.
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
        
        self.mapping: Dict[str, Tuple[str, str, Optional[str]]] = {}
        self._load_mapping()
    
    def _load_mapping(self) -> None:
        """Load the mapping from the CSV file if it exists."""
        if not os.path.exists(self.csv_file_path):
            # Create the file with headers if it doesn't exist
            try:
                with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['ods_id', 'uuid', 'href', 'inCollection'])
            except (IOError, PermissionError) as e:
                self.logger.warning("Could not create mapping file: %s", str(e))
            return
        
        try:
            with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                self.mapping = {
                    row['ods_id']: (
                        row['uuid'],
                        row['href'],
                        row.get('inCollection')  # This might be None for older mapping files
                    )
                    for row in reader
                }
        except (IOError, PermissionError) as e:
            self.logger.warning("Could not read mapping file: %s", str(e))
        except Exception as e:
            self.logger.warning("Error parsing mapping file: %s", str(e))
    
    def _save_mapping(self) -> None:
        """Save the current mapping to the CSV file."""
        try:
            # Sort the items by ods_id
            sorted_mapping = sorted(self.mapping.items(), key=lambda x: x[0])
            with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ods_id', 'uuid', 'href', 'inCollection'])
                for ods_id, (uuid, href, inCollection) in sorted_mapping:
                    writer.writerow([ods_id, uuid, href, inCollection or ''])
        except (IOError, PermissionError) as e:
            self.logger.warning("Could not write to mapping file: %s", str(e))
    
    def _is_valid_uuid(self, uuid_str: str) -> bool:
        """
        Check if the string is a valid UUID format.
        
        Args:
            uuid_str (str): The UUID string to validate
            
        Returns:
            bool: True if the UUID is valid, False otherwise
        """
        try:
            # Try to parse it as a UUID
            uuid_obj = uuid.UUID(uuid_str)
            return str(uuid_obj) == uuid_str
        except (ValueError, AttributeError):
            return False
    
    def get_entry(self, ods_id: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Get the UUID, HREF, and inCollection for an ODS ID if it exists.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[Tuple[str, str, Optional[str]]]: A tuple of (uuid, href, inCollection) if found, None otherwise
        """
        return self.mapping.get(ods_id)
    
    def add_entry(self, ods_id: str, uuid_str: str, href: str, inCollection: str = None) -> bool:
        """
        Add a new mapping entry or update an existing one.
        
        Args:
            ods_id (str): The ODS ID
            uuid_str (str): The Dataspot UUID
            href (str): The Dataspot HREF
            inCollection (str, optional): The business key of the collection containing this dataset
            
        Returns:
            bool: True if the entry was added successfully, False otherwise
        """
        # Check for empty values with specific error messages
        empty_params = []
        if not ods_id:
            empty_params.append("ods_id")
        if not uuid_str:
            empty_params.append("uuid_str")
        if not href:
            empty_params.append("href")
            
        if empty_params:
            self.logger.warning("Cannot add entry with empty values for: %s", ", ".join(empty_params))
            self.logger.warning("Provided values - ods_id: '%s', uuid_str: '%s', href: '%s'", 
                             ods_id, uuid_str, href)
            return False
        
        # Validate UUID format
        if not self._is_valid_uuid(uuid_str):
            self.logger.warning("Invalid UUID format: '%s'", uuid_str)
            self.logger.warning("UUID must match the format: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' (8-4-4-4-12 hex digits)")
            return False
        
        self.mapping[ods_id] = (uuid_str, href, inCollection)
        self._save_mapping()
        return True
    
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
    
    def get_inCollection(self, ods_id: str) -> Optional[str]:
        """
        Get just the inCollection UUID for an ODS ID.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[str]: The inCollection UUID if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[2] if entry and len(entry) > 2 else None
    
    def get_all_entries(self) -> Dict[str, Tuple[str, str, Optional[str]]]:
        """
        Get all mapping entries.
        
        Returns:
            Dict[str, Tuple[str, str, Optional[str]]]: Dictionary of all ODS ID to (UUID, HREF, inCollection) mappings
        """
        return dict(self.mapping)
    
    def get_all_ods_ids(self) -> List[str]:
        """
        Get all ODS IDs in the mapping.
        
        Returns:
            List[str]: List of all ODS IDs
        """
        return list(self.mapping.keys())