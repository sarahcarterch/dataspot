import csv
import os
import uuid
import logging
from typing import Tuple, Optional, Dict, List

_CSV_HEADERS = ['staatskalender_id', '_type', 'uuid', 'inCollection']

def _get_mapping_file_path(database_name: str) -> str:
    return f"staatskalender-dataspot-mapping_{database_name}.csv"

class StaatskalenderDataspotMapping:
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
        self.logger = logging.getLogger(__name__)

        if not database_name:
            raise ValueError("database_name cannot be empty")

        # Store database name for endpoint construction
        self.database_name = database_name

        # Derive csv_file_path from the mandatory database_name
        self.csv_file_path = _get_mapping_file_path(database_name)
        self.logger.info(f"Using staatskalender mapping file: {self.csv_file_path}")

        # Mapping: Dict[str, Tuple[str, str, Optional[str]]] -> Dict[staatskalender_id, (_type, uuid, inCollection)]
        self.mapping: Dict[str, Tuple[str, str, Optional[str]]] = {}
        self._load_mapping()

    def _load_mapping(self) -> None:
        """Load the mapping from the CSV file if it exists."""
        if not os.path.exists(self.csv_file_path):
            # Create the file with headers if it doesn't exist
            try:
                with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(_CSV_HEADERS) # Use defined headers
            except (IOError, PermissionError) as e:
                self.logger.warning("Could not create staatskalender mapping file: %s", str(e))
            return

        try:
            with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Check headers
                if reader.fieldnames != _CSV_HEADERS:
                    self.logger.warning(f"CSV file header mismatch in {self.csv_file_path}. "
                                        f"Expected: {_CSV_HEADERS}, Found: {reader.fieldnames}. "
                                        f"Attempting to load anyway, but data might be misinterpreted or fail.")

                self.mapping = {} # Clear existing mapping before loading
                for row in reader:
                    # Check if required columns exist in the row
                    # 'inCollection' is optional in the data, but the column must exist
                    if not all(h in row for h in ['staatskalender_id', '_type', 'uuid']):
                         self.logger.warning(f"Skipping row due to missing required columns (staatskalender_id, _type, uuid): {row}")
                         continue

                    staatskalender_id = row['staatskalender_id']
                    _type = row['_type']
                    uuid_val = row['uuid']
                    # Handle potentially missing or empty inCollection
                    inCollection = row.get('inCollection')
                    if inCollection == '':
                        inCollection = None

                    self.mapping[staatskalender_id] = (_type, uuid_val, inCollection)

        except (IOError, PermissionError) as e:
            self.logger.warning("Could not read staatskalender mapping file: %s", str(e))
        except KeyError as e:
             self.logger.warning(f"Missing expected column '{e}' while parsing row in {self.csv_file_path}. Check header format.")
        except Exception as e:
            self.logger.warning("Error parsing staatskalender mapping file: %s", str(e))

    def _save_mapping(self) -> None:
        """Save the current mapping to the CSV file."""
        try:
            # Sort the items by staatskalender_id
            sorted_mapping = sorted(self.mapping.items(), key=lambda x: x[0])
            with open(self.csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(_CSV_HEADERS) # Write the headers
                for staatskalender_id, (_type, uuid_val, inCollection) in sorted_mapping:
                    # Ensure inCollection is written as empty string if None
                    writer.writerow([staatskalender_id, _type, uuid_val, inCollection or ''])
        except (IOError, PermissionError) as e:
            self.logger.warning("Could not write to staatskalender mapping file: %s", str(e))

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

    def get_entry(self, staatskalender_id: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Get the _type, UUID, and inCollection for a Staatskalender ID if it exists.

        Args:
            staatskalender_id (str): The Staatskalender ID to look up

        Returns:
            Optional[Tuple[str, str, Optional[str]]]: A tuple of (_type, uuid, inCollection) if found, None otherwise
        """
        return self.mapping.get(staatskalender_id)

    def add_entry(self, staatskalender_id: str, _type: str, uuid_str: str, in_collection: Optional[str] = None) -> bool:
        """
        Add a new mapping entry or update an existing one.

        Args:
            staatskalender_id (str): The Staatskalender ID
            _type (str): The Dataspot asset type (e.g., "Collection")
            uuid_str (str): The Dataspot UUID
            in_collection (str, optional): The business key of the collection containing this asset. Defaults to None.

        Returns:
            bool: True if the entry was added successfully, False otherwise
        """
        # Check for empty required values with specific error messages
        empty_params = []
        if not staatskalender_id:
            empty_params.append("staatskalender_id")
        if not _type:
            empty_params.append("_type")
        if not uuid_str:
            empty_params.append("uuid_str")

        if empty_params:
            self.logger.warning("Cannot add entry with empty values for: %s", ", ".join(empty_params))
            self.logger.warning("Provided values - staatskalender_id: '%s', _type: '%s', uuid_str: '%s'",
                             staatskalender_id, _type, uuid_str)
            return False

        # Validate UUID format
        if not self._is_valid_uuid(uuid_str):
            self.logger.warning("Invalid UUID format: '%s' for staatskalender_id '%s'", uuid_str, staatskalender_id)
            self.logger.warning("UUID must match the format: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' (8-4-4-4-12 hex digits)")
            return False

        # Store the entry including _type
        self.mapping[staatskalender_id] = (_type, uuid_str, in_collection)
        self._save_mapping()
        return True

    def remove_entry(self, staatskalender_id: str) -> bool:
        """
        Remove a mapping entry if it exists.

        Args:
            staatskalender_id (str): The Staatskalender ID to remove

        Returns:
            bool: True if the entry was removed, False if it didn't exist
        """
        if staatskalender_id in self.mapping:
            del self.mapping[staatskalender_id]
            self._save_mapping()
            return True
        return False

    def get_type(self, staatskalender_id: str) -> Optional[str]:
        """
        Get just the _type for a Staatskalender ID.

        Args:
            staatskalender_id (str): The Staatskalender ID to look up

        Returns:
            Optional[str]: The asset _type if found, None otherwise
        """
        entry = self.get_entry(staatskalender_id)
        return entry[0] if entry else None # Index 0 for _type

    def get_uuid(self, staatskalender_id: str) -> Optional[str]:
        """
        Get just the UUID for a Staatskalender ID.

        Args:
            staatskalender_id (str): The Staatskalender ID to look up

        Returns:
            Optional[str]: The UUID if found, None otherwise
        """
        entry = self.get_entry(staatskalender_id)
        return entry[1] if entry else None # Index 1 for UUID

    def get_inCollection(self, staatskalender_id: str) -> Optional[str]:
        """
        Get just the inCollection business key for a Staatskalender ID.

        Args:
            staatskalender_id (str): The Staatskalender ID to look up

        Returns:
            Optional[str]: The inCollection business key if found, None otherwise
        """
        entry = self.get_entry(staatskalender_id)
        return entry[2] if entry else None # Index 2 for inCollection

    def get_all_entries(self) -> Dict[str, Tuple[str, str, Optional[str]]]:
        """
        Get all mapping entries.

        Returns:
            Dict[str, Tuple[str, str, Optional[str]]]: Dictionary of all Staatskalender ID to (_type, UUID, inCollection) mappings
        """
        return dict(self.mapping)

    def get_all_staatskalender_ids(self) -> List[str]:
        """
        Get a list of all Staatskalender IDs in the mapping.

        Returns:
            List[str]: A list of all Staatskalender IDs
        """
        return list(self.mapping.keys()) 