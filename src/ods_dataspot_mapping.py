import csv
import os
import uuid # Only for validating dataspot uuid
import logging
from typing import Tuple, Optional, Dict, List


def _get_mapping_file_path(database_name: str) -> str:
    return f"ods-dataspot-mapping_{database_name}.csv"

class ODSDataspotMapping:
    """
    A lookup table that maps ODS IDs to Dataspot asset type, UUID, REST API Endpoint, and inCollection.
    Stores the mapping in a CSV file for persistence. Handles only datasets for now.
    """

    def __init__(self, database_name: str):
        """
        Initialize the mapping table for datasets.
        The CSV filename is derived from the database_name.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 The file will be named "ods-dataspot-mapping_{database_name}.csv".
        """
        self.logger = logging.getLogger(__name__)

        if not database_name:
            raise ValueError("database_name cannot be empty")

        # Derive csv_file_path from the mandatory database_name
        self.csv_file_path = _get_mapping_file_path(database_name)
        self.logger.info(f"Using mapping file: {self.csv_file_path}")

        # Mapping: Dict[str, Tuple[str, str, str, Optional[str]]] -> Dict[ods_id, (_type, uuid, endpoint_rest, inCollection)]
        self.mapping: Dict[str, Tuple[str, str, str, Optional[str]]] = {}
        self._load_mapping()

    def _load_mapping(self) -> None:
        """Load the mapping from the CSV file if it exists."""
        # Define expected headers including _type
        expected_headers = ['ods_id', '_type', 'uuid', 'endpoint_rest', 'inCollection']
        if not os.path.exists(self.csv_file_path):
            # Create the file with headers if it doesn't exist
            try:
                with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(expected_headers) # Use defined headers
            except (IOError, PermissionError) as e:
                self.logger.warning("Could not create dataset mapping file: %s", str(e))
            return

        try:
            with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check headers
                if reader.fieldnames != expected_headers:
                    self.logger.warning(f"CSV file header mismatch in {self.csv_file_path}. "
                                        f"Expected: {expected_headers}, Found: {reader.fieldnames}. "
                                        f"Attempting to load anyway, but data might be misinterpreted.")

                self.mapping = {} # Clear existing mapping before loading
                for row in reader:
                    # Check if required columns exist in the row
                    if not all(h in row for h in ['ods_id', '_type', 'uuid', 'endpoint_rest']):
                         self.logger.warning(f"Skipping row due to missing required columns: {row}")
                         continue

                    ods_id = row['ods_id']
                    _type = row['_type']
                    uuid_val = row['uuid']
                    endpoint_rest = row['endpoint_rest']
                    # Handle potentially missing or empty inCollection
                    inCollection = row.get('inCollection')
                    if inCollection == '':
                        inCollection = None

                    self.mapping[ods_id] = (_type, uuid_val, endpoint_rest, inCollection)

        except (IOError, PermissionError) as e:
            self.logger.warning("Could not read dataset mapping file: %s", str(e))
        except Exception as e:
            self.logger.warning("Error parsing dataset mapping file: %s", str(e))

    def _save_mapping(self) -> None:
        """Save the current mapping to the CSV file."""
        expected_headers = ['ods_id', '_type', 'uuid', 'endpoint_rest', 'inCollection'] # Define headers again
        try:
            # Sort the items by ods_id
            sorted_mapping = sorted(self.mapping.items(), key=lambda x: x[0])
            with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(expected_headers) # Write the headers
                for ods_id, (_type, uuid_val, endpoint_rest, inCollection) in sorted_mapping:
                    # Ensure inCollection is written as empty string if None
                    writer.writerow([ods_id, _type, uuid_val, endpoint_rest, inCollection or ''])
        except (IOError, PermissionError) as e:
            self.logger.warning("Could not write to dataset mapping file: %s", str(e))
    
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
    
    def get_entry(self, ods_id: str) -> Optional[Tuple[str, str, str, Optional[str]]]:
        """
        Get the _type, UUID, REST API Endpoint, and inCollection for an ODS ID if it exists.

        Args:
            ods_id (str): The ODS ID to look up

        Returns:
            Optional[Tuple[str, str, str, Optional[str]]]: A tuple of (_type, uuid, endpoint_rest, inCollection) if found, None otherwise
        """
        return self.mapping.get(ods_id)
    
    def add_entry(self, ods_id: str, _type: str, uuid_str: str, endpoint_rest: str, in_collection: Optional[str] = None) -> bool:
        """
        Add a new mapping entry or update an existing one.

        Args:
            ods_id (str): The ODS ID
            _type (str): The Dataspot asset type (e.g., "Dataset")
            uuid_str (str): The Dataspot UUID
            endpoint_rest (str): The Dataspot REST API Endpoint (e.g., /rest/db/datasets/<uuid>)
            in_collection (str, optional): The business key of the collection containing this dataset

        Returns:
            bool: True if the entry was added successfully, False otherwise
        """
        # Check for empty values with specific error messages
        empty_params = []
        if not ods_id:
            empty_params.append("ods_id")
        if not _type:
            empty_params.append("_type")
        if not uuid_str:
            empty_params.append("uuid_str")
        if not endpoint_rest:
            empty_params.append("endpoint_rest")

        if empty_params:
            self.logger.warning("Cannot add entry with empty values for: %s", ", ".join(empty_params))
            self.logger.warning("Provided values - ods_id: '%s', _type: '%s', uuid_str: '%s', endpoint_rest: '%s'",
                             ods_id, _type, uuid_str, endpoint_rest)
            return False

        # Validate UUID format
        if not self._is_valid_uuid(uuid_str):
            self.logger.warning("Invalid UUID format: '%s'", uuid_str)
            self.logger.warning("UUID must match the format: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' (8-4-4-4-12 hex digits)")
            return False

        # Store the entry including _type
        self.mapping[ods_id] = (_type, uuid_str, endpoint_rest, in_collection)
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
    
    def get_type(self, ods_id: str) -> Optional[str]:
        """
        Get just the _type for an ODS ID.

        Args:
            ods_id (str): The ODS ID to look up

        Returns:
            Optional[str]: The asset _type if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[0] if entry else None # Index 0 for _type
        
    def get_uuid(self, ods_id: str) -> Optional[str]:
        """
        Get just the UUID for an ODS ID.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[str]: The UUID if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[1] if entry else None # Index 1 for UUID
    
    def get_endpoint_rest(self, ods_id: str) -> Optional[str]:
        """
        Get just the REST API Endpoint for an ODS ID.
        
        Args:
            ods_id (str): The ODS ID to look up
            
        Returns:
            Optional[str]: The REST API Endpoint if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        return entry[2] if entry else None # Index 2 for REST API Endpoint
    
    def get_inCollection(self, ods_id: str) -> Optional[str]:
        """
        Get just the inCollection business key for an ODS ID.

        Args:
            ods_id (str): The ODS ID to look up

        Returns:
            Optional[str]: The inCollection business key if found, None otherwise
        """
        entry = self.get_entry(ods_id)
        # Check entry exists and has at least 4 elements (index 3 is inCollection)
        return entry[3] if entry and len(entry) >= 4 else None
    
    
    def get_all_entries(self) -> Dict[str, Tuple[str, str, str, Optional[str]]]:
        """
        Get all mapping entries.

        Returns:
            Dict[str, Tuple[str, str, str, Optional[str]]]: Dictionary of all ODS ID to (_type, UUID, REST API Endpoint, inCollection) mappings
        """
        return dict(self.mapping)
    
    def get_all_ods_ids(self) -> List[str]:
        """
        Get all ODS IDs in the mapping.
        
        Returns:
            List[str]: List of all ODS IDs
        """
        return list(self.mapping.keys())