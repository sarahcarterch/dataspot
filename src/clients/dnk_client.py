import logging
from typing import Dict, Any, List

from requests import HTTPError

from src import config
from src.clients.base_client import BaseDataspotClient
from src.clients.org_structure_handler import OrgStructureHandler
from src.clients.helpers import url_join, get_uuid_from_response
from src.dataspot_dataset import Dataset
# TODO (Renato): Do not import requests_get; solve this somehow.
from src.common import requests_get # BUT DO NOT IMPORT THESE: requests_post, requests_put, requests_patch
from src.ods_dataspot_mapping import ODSDataspotMapping
from src.staatskalender_dataspot_mapping import StaatskalenderDataspotMapping

class DNKClient(BaseDataspotClient):
    """Client for interacting with the DNK (Datennutzungskatalog)."""
    
    def __init__(self):
        """
        Initialize the DNK client.
        """
        super().__init__(base_url=config.base_url,
                         database_name=config.database_name,
                         scheme_name=config.dnk_scheme_name,
                         scheme_name_short=config.dnk_scheme_name_short,
                         ods_imports_collection_name=config.ods_imports_collection_name)
        
        # Set up ODS mapping
        self.mapping = ODSDataspotMapping(database_name=self.database_name, scheme=self.scheme_name_short)
        
        # Initialize the organization handler
        self.org_handler = OrgStructureHandler(self)
        
        # Initialize the organization mapping (for direct access if needed)
        self.org_mapping = self.org_handler.org_mapping
        
    def _download_and_update_mappings(self, target_ods_ids: List[str] = None) -> int:
        """
        Helper method to download datasets and update ODS ID to Dataspot UUID mappings.
        
        Args:
            target_ods_ids (List[str], optional): If provided, only update mappings for these ODS IDs.
                                                 If None, update all mappings found.
        
        Returns:
            int: Number of mappings successfully updated
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If the response format is unexpected or invalid
        """
        datasets = self.get_all_assets_from_scheme()

        if not datasets:
            logging.warning(f"No datasets with ods_ids found in {self.scheme_name}")
            return 0
        
        # Create a lookup dictionary for faster access
        dataset_by_ods_id = {}
        for dataset in datasets:
            ods_id = dataset.get('ODS_ID')
            if ods_id:
                dataset_by_ods_id[ods_id] = dataset
        
        # Process each dataset and update the mapping
        updated_count = 0
        
        # Check for datasets in mapping that are not in downloaded datasets
        if not target_ods_ids:
            removed_count = 0
            for ods_id, entry in list(self.mapping.mapping.items()):
                if ods_id not in dataset_by_ods_id:
                    logging.warning(f"Dataset {ods_id} exists in local mapping but not in dataspot. Removing from local mapping.")
                    if len(entry) >= 2:
                        _type, uuid = entry[0], entry[1]
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}")
                    self.mapping.remove_entry(ods_id)
                    removed_count += 1
            if removed_count > 0:
                logging.info(f"Found {removed_count} datasets that exist locally but not in dataspot.")
        
        # If we have target IDs, prioritize those
        if target_ods_ids and len(target_ods_ids) > 0:
            total_targets = len(target_ods_ids)
            target_ods_ids.sort()
            for idx, ods_id in enumerate(target_ods_ids, 1):
                logging.debug(f"[{idx}/{total_targets}] Processing dataset with ODS ID: {ods_id}")
                
                # Find this ODS ID in our downloaded datasets
                dataset = dataset_by_ods_id.get(ods_id)
                if not dataset:
                    logging.warning(f"Could not find dataset with ODS ID {ods_id} in downloaded data, skipping")
                    continue
                
                # Get the UUID and _type
                uuid = dataset.get('id')
                _type = dataset.get('_type') # Get the _type
                if not uuid:
                    logging.warning(f"Dataset with ODS ID {ods_id} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Dataset with ODS ID {ods_id} missing _type, skipping")
                    continue
                
                # Extract inCollection business key directly from the downloaded dataset
                inCollection_key = dataset.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.mapping.get_entry(ods_id)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in dataset {ods_id}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update dataset {ods_id} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating dataset {ods_id} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Dataset {ods_id} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Dataset {ods_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Dataset {ods_id} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Dataset {ods_id} has been removed from '{old_inCollection}'")
                        
                        logging.debug(f"    - old_type: {old_type}, old_uuid: {old_uuid}, old_inCollection: {old_inCollection}")
                        logging.debug(f"    - new_type: {_type}, new_uuid: {uuid}, new_inCollection: {inCollection_key}")
                        self.mapping.add_entry(ods_id, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add dataset {ods_id} with uuid {uuid}")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                        self.mapping.add_entry(ods_id, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for dataset with ODS ID: {ods_id}")
        else:
            # No target IDs, process all datasets
            total_datasets = len(datasets)
            for idx, dataset in enumerate(datasets, 1):
                # Extract the ID from customProperties
                ods_id = dataset.get('ODS_ID')
                
                # Skip if this dataset doesn't have an ODS ID
                if not ods_id:
                    continue
                
                logging.debug(f"[{idx}/{total_datasets}] Processing dataset with ODS ID: {ods_id}")
                
                # Get the UUID and _type
                uuid = dataset.get('id')
                _type = dataset.get('_type') # Get the _type
                if not uuid:
                    logging.warning(f"Dataset with ODS ID {ods_id} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Dataset with ODS ID {ods_id} missing _type, skipping")
                    continue

                # Extract inCollection business key directly from the downloaded dataset
                inCollection_key = dataset.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.mapping.get_entry(ods_id)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in dataset {ods_id}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update dataset {ods_id} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating dataset {ods_id} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Dataset {ods_id} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Dataset {ods_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Dataset {ods_id} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Dataset {ods_id} has been removed from '{old_inCollection}'")
                        
                        logging.debug(f"    - old_type: {old_type}, old_uuid: {old_uuid}, old_inCollection: {old_inCollection}")
                        logging.debug(f"    - new_type: {_type}, new_uuid: {uuid}, new_inCollection: {inCollection_key}")
                        self.mapping.add_entry(ods_id, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add dataset {ods_id} with uuid {uuid}")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                        self.mapping.add_entry(ods_id, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for dataset with ODS ID: {ods_id}")
        
        logging.info(f"Updated mappings for {updated_count} datasets. Did not update mappings for the other {len(datasets) - updated_count} datasets.")
        return updated_count

    def sync_staatskalender_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """Delegate to org_handler"""
        return self.org_handler.sync_staatskalender_org_units(org_data, validate_urls)

    # Dataset methods remain directly in DNKClient
    def create_dataset(self, dataset: Dataset) -> dict:
        """
        Create a new dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        
        Args:
            dataset (Dataset): The dataset instance to be uploaded.
            
        Returns:
            dict: The JSON response from the API containing the dataset data
            
        Raises:
            ValueError: If the dataset is missing required properties
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Preload mapping from DNK to ensure we have the latest mapping data
        try:
            logging.info("Preloading ODS ID to Dataspot mappings from DNK system")
            self._download_and_update_mappings()
        except Exception as e:
            logging.warning(f"Failed to preload mappings, continuing with existing mappings: {str(e)}")
        
        # Get ODS ID from dataset
        ods_id = dataset.to_json().get('customProperties', {}).get('ODS_ID')
        if not ods_id:
            logging.error("Dataset missing 'ODS_ID' property required for ODS ID")
            raise ValueError("Dataset must have an 'ODS_ID' property to use as ODS ID")
        
        # Check if dataset with this ODS ID already exists
        existing_entry = self.mapping.get_entry(ods_id)
        if existing_entry:
            # Entry is now (_type, uuid, inCollection)
            _type, uuid, _ = existing_entry
            logging.info(f"Dataset with ODS ID {ods_id} already exists (Type: {_type}, UUID: {uuid}). Use update_dataset or create_or_update_dataset method to update.")
            raise ValueError(f"Dataset with ODS ID {ods_id} already exists. Use update_dataset or create_or_update_dataset method.")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Creating dataset: '{title}' with ODS ID: {ods_id}")
        
        # Ensure ODS-Imports collection exists
        logging.debug(f"Ensuring ODS-Imports collection exists")
        collection_data = self.ensure_ods_imports_collection_exists()
        logging.debug(f"Collection info: {collection_data}")

        # Get the collection UUID
        collection_uuid = get_uuid_from_response(collection_data)

        if not collection_uuid:
            error_msg = "Failed to get collection UUID"
            logging.error(error_msg)
            raise ValueError(error_msg)

        collection_href = url_join('rest', self.database_name, 'datasets', collection_uuid, leading_slash=True)
        logging.debug(f"Using collection UUID: {collection_uuid} and constructed href: {collection_href}")
        
        # Create a new dataset
        dataset_creation_endpoint = url_join(collection_href, "datasets")
        
        # Ensure inCollection property is set with the full path
        dataset_json = dataset.to_json()
        collection_path = getattr(config, 'ods_imports_collection_path', [])
        
        if collection_path:
            # Build the full business key path for inCollection
            # If path is ['A', 'B', 'C'], then inCollection should be "A/B/C/ODS-Imports"
            full_path = '/'.join(collection_path + [self.ods_imports_collection_name])
            
            # Check for slashes in the path components which would require escaping
            has_special_chars = any('/' in folder for folder in collection_path)
            
            if has_special_chars:
                # We need to escape the whole path with quotes since it contains slashes
                from src.clients.helpers import escape_special_chars
                full_path = escape_special_chars(full_path)
                
            logging.debug(f"Using full path inCollection: '{full_path}'")
            dataset_json['inCollection'] = full_path
        else:
            # No path, just use the collection name directly
            logging.debug(f"Using default inCollection: '{self.ods_imports_collection_name}'")
            dataset_json['inCollection'] = self.ods_imports_collection_name
        
        response = self._create_asset(
            endpoint=dataset_creation_endpoint,
            data=dataset_json
        )
        
        # Store the mapping for future reference
        if ods_id:
            uuid = get_uuid_from_response(response)
            if uuid:
                # For newly created datasets, store the ODS-Imports collection name as the business key
                # The _type for datasets created here is always "Dataset"
                logging.debug(f"Adding mapping entry for ODS ID {ods_id} with Type 'Dataset', UUID {uuid}, and inCollection '{self.ods_imports_collection_name}'")
                self.mapping.add_entry(ods_id, "Dataset", uuid, self.ods_imports_collection_name)
            else:
                logging.warning(f"Could not extract UUID from response for dataset '{title}'")
        
        logging.info(f"Successfully created dataset '{title}'")
        return response

    def bulk_create_or_update_datasets(self, datasets: List[Dataset],
                                       operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple datasets in bulk in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        The datasets will be created at the scheme level, but each dataset will have its inCollection
        field set to place it within the ODS-Imports collection or the collection stored in the mapping.
        
        Args:
            datasets (List[Dataset]): List of dataset instances to be uploaded.
            operation (str, optional): Upload operation mode. Defaults to "ADD".
                                      "ADD": Add or update only. Existing datasets not in the upload remain unchanged.
                                      "REPLACE": Reconcile elements. Datasets not in the upload are considered obsolete.
                                      "FULL_LOAD": Reconcile model. Completely replaces with the uploaded datasets.
            dry_run (bool, optional): Whether to perform a test run without changing data. Defaults to False.
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If any dataset is missing required properties
            HTTPError: If API requests fail
        """
        # Verify we have datasets to process
        if not datasets:
            logging.warning("No datasets provided for bulk upload")
            return {"status": "error", "message": "No datasets provided"}

        # Preload mapping from DNK to ensure we have the latest mapping data
        try:
            logging.info("Preloading ODS ID to Dataspot mappings from DNK system")
            self._download_and_update_mappings()
        except Exception as e:
            logging.warning(f"Failed to preload mappings, continuing with existing mappings: {str(e)}")
        
        # Ensure ODS-Imports collection exists and get its UUID
        try:
            logging.debug("Ensuring ODS-Imports collection exists")
            collection_data = self.ensure_ods_imports_collection_exists()
            
            # Get the collection UUID
            collection_uuid = get_uuid_from_response(collection_data)

            if not collection_uuid:
                error_msg = "Failed to get collection UUID"
                logging.error(error_msg)
                raise ValueError(error_msg)

            logging.debug(f"Using collection UUID: {collection_uuid}")
        except HTTPError as e:
            logging.error(f"HTTP error ensuring ODS-Imports collection exists: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error ensuring ODS-Imports collection exists: {str(e)}")
            raise ValueError(f"Could not access collection information: {str(e)}")
        
        # Validate and transform datasets
        dataset_jsons = []
        ods_ids = []
        validation_errors = []
        
        for i, dataset in enumerate(datasets):
            try:
                # Get ODS ID from dataset
                dataset_json = dataset.to_json()
                ods_id = dataset_json.get('customProperties', {}).get('ODS_ID')
                
                if not ods_id:
                    validation_errors.append(f"Dataset at index {i} missing 'ODS_ID' property")
                    continue
                
                ods_ids.append(ods_id)
                
                # Read the dataset title for logging
                title = dataset_json.get('label', f"<Unnamed Dataset {ods_id}>")
                logging.debug(f"Processing dataset '{title}' with ODS ID: {ods_id}")
                
                # Check if this dataset has a stored inCollection (business key)
                inCollection = self.mapping.get_inCollection(ods_id)
                
                if inCollection:
                    # Use the stored inCollection business key
                    logging.debug(f"Using stored inCollection '{inCollection}' for dataset with ODS ID: {ods_id}")
                    dataset_json['inCollection'] = inCollection
                else:
                    # Default to ODS-Imports collection with full path
                    collection_path = getattr(config, 'ods_imports_collection_path', [])
                    
                    # TODO (Renato): Have a look at this if block again to see if it makes sense.
                    if collection_path:
                        # Build the full business key path for inCollection
                        # If path is ['A', 'B', 'C'], then inCollection should be "A/B/C/ODS-Imports"
                        full_path = '/'.join(collection_path + [self.ods_imports_collection_name])
                        
                        # Check for slashes in the path components which would require escaping
                        has_special_chars = any('/' in folder for folder in collection_path)
                        
                        if has_special_chars:
                            # We need to escape the whole path with quotes since it contains slashes
                            from src.clients.helpers import escape_special_chars
                            full_path = escape_special_chars(full_path)
                            
                        logging.debug(f"Using full path inCollection: '{full_path}' for dataset with ODS ID: {ods_id}")
                        dataset_json['inCollection'] = full_path
                    else:
                        # No path, just use the collection name directly
                        logging.debug(f"Using default inCollection: '{self.ods_imports_collection_name}' for dataset with ODS ID: {ods_id}")
                        dataset_json['inCollection'] = self.ods_imports_collection_name
                
                dataset_jsons.append(dataset_json)
            except Exception as e:
                validation_errors.append(f"Error processing dataset at index {i}: {str(e)}")
        
        # If we encountered validation errors, raise an exception
        if validation_errors:
            error_msg = f"Validation errors in {len(validation_errors)} datasets: {validation_errors[:3]}"
            if len(validation_errors) > 3:
                error_msg += f" and {len(validation_errors) - 3} more"
            logging.error(error_msg)
            raise ValueError(error_msg)
            
        # Count of datasets
        num_datasets = len(dataset_jsons)
        if num_datasets == 0:
            logging.warning("No valid datasets to upload after validation")
            return {"status": "error", "message": "No valid datasets to upload"}
            
        logging.info(f"Bulk creating {num_datasets} datasets (operation: {operation}, dry_run: {dry_run})...")
        
        # Bulk create datasets using the scheme name
        try:
            response = self.bulk_create_or_update_assets(
                scheme_name=self.scheme_name,
                data=dataset_jsons,
                operation=operation,
                dry_run=dry_run
            )

            logging.info(f"Bulk creation complete")

            # Update mapping for each dataset (only for non-dry runs)
            if not dry_run:
                try:
                    # After bulk upload, retrieve the datasets and update mapping
                    self.update_mappings_from_upload(ods_ids)
                except Exception as e:
                    logging.warning(f"Failed to update mapping after bulk upload: {str(e)}")
                    # Continue despite mapping update failure - the upload was successful
            
            logging.info(f"Bulk dataset creation completed successfully")
            return response
            
        except HTTPError as e:
            logging.error(f"HTTP error during bulk upload: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    logging.error(f"Error response details: {error_details}")
                except:
                    logging.error(f"Error response status: {e.response.status_code}, text: {e.response.text[:500]}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during bulk upload: {str(e)}")
            raise
    
    def update_mappings_from_upload(self, ods_ids: List[str]) -> None:
        """
        Updates the mapping between ODS IDs and Dataspot UUIDs after uploading datasets.
        Uses the download API to retrieve all datasets and then updates the mapping for matching ODS IDs.
        
        Args:
            ods_ids (List[str]): List of ODS IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve dataset information
        """
        logging.info(f"Updating mappings for {len(ods_ids)} datasets using download API")
        
        try:
            updated_count = self._download_and_update_mappings(ods_ids)
            logging.info(f"Updated mappings for {updated_count} out of {len(ods_ids)} datasets")
            
            if updated_count < len(ods_ids):
                missing_ids = [ods_id for ods_id in ods_ids if not self.mapping.get_entry(ods_id)]
                if missing_ids:
                    logging.warning(f"Could not find mappings for {len(missing_ids)} ODS IDs: {missing_ids[:5]}" + 
                                   (f"... and {len(missing_ids)-5} more" if len(missing_ids) > 5 else ""))
        except Exception as e:
            logging.error(f"Error updating mappings: {str(e)}")
            raise

    def update_dataset(self, dataset: Dataset, href: str, force_replace: bool = False) -> dict:
        """
        Update an existing dataset in the DNK.
        
        Args:
            dataset (Dataset): The dataset instance with updated data
            href (str): The href of the dataset to update
            force_replace (bool): Whether to completely replace the dataset (True) or just update properties (False)
            
        Returns:
            dict: The JSON response from the API containing the updated dataset data
            
        Raises:
            ValueError: If the dataset is missing required properties
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Get ODS ID from dataset
        ods_id = dataset.to_json().get('customProperties', {}).get('ODS_ID')
        if not ods_id:
            logging.error("Dataset missing 'ODS_ID' property required for ODS ID")
            raise ValueError("Dataset must have an 'ODS_ID' property to use as ODS ID")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Updating dataset: '{title}' with ODS ID: {ods_id}")
        
        # Get the inCollection from mapping if available (this is now a business key)
        inCollection = self.mapping.get_inCollection(ods_id)
        
        # Set inCollection in the dataset JSON
        dataset_json = dataset.to_json()
        if inCollection:
            dataset_json['inCollection'] = inCollection
            logging.debug(f"Using stored inCollection '{inCollection}' from mapping")
        else:
            # Default to ODS-Imports collection with full path
            collection_path = getattr(config, 'ods_imports_collection_path', [])
            
            if collection_path:
                # Build the full business key path for inCollection
                full_path = '/'.join(collection_path + [self.ods_imports_collection_name])
                
                # Check for slashes in the path components which would require escaping
                has_special_chars = any('/' in folder for folder in collection_path)
                
                if has_special_chars:
                    # We need to escape the whole path with quotes since it contains slashes
                    from src.clients.helpers import escape_special_chars
                    full_path = escape_special_chars(full_path)
                    
                logging.debug(f"Using full path inCollection: '{full_path}'")
                dataset_json['inCollection'] = full_path
            else:
                # No path, just use the collection name directly
                logging.debug(f"Using default inCollection: '{self.ods_imports_collection_name}'")
                dataset_json['inCollection'] = self.ods_imports_collection_name
        
        # Update the existing dataset
        logging.debug(f"Update method: {'PUT (replace)' if force_replace else 'PATCH (partial update)'}")
        response = self._update_asset(
            endpoint=href,
            data=dataset_json,
            replace=force_replace
        )
        
        # Ensure the mapping is updated
        if ods_id:
            uuid = get_uuid_from_response(response)
            if uuid:
                # The _type for datasets updated here is always "Dataset"
                # Use the determined inCollection value (either from mapping or default)
                final_inCollection = dataset_json.get('inCollection')
                logging.debug(f"Updating mapping for ODS ID {ods_id} with Type 'Dataset', UUID {uuid}, inCollection {final_inCollection}")
                self.mapping.add_entry(ods_id, "Dataset", uuid, final_inCollection)
            else:
                logging.warning(f"Could not extract UUID from response for dataset '{title}'")
        
        logging.info(f"Successfully updated dataset '{title}'")
        return response

    def create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update',
                                     force_replace: bool = False) -> dict:
        """
        Create a new dataset or update an existing dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        All datasets are placed directly in the ODS-Imports collection, regardless of their internal path structure.

        The method behavior is controlled by the update_strategy parameter:
        - 'create_only': Only creates a new dataset using POST. Fails if the dataset already exists.
        - 'update_only': Only updates an existing dataset. Fails if the dataset doesn't exist.
        - 'create_or_update' (default): Creates a new dataset if it doesn't exist, updates it if it does.

        The force_replace parameter controls the update behavior:
        - False (default): Uses PATCH to update only the specified properties, preserving other properties.
        - True: Uses PUT to completely replace the dataset with the new values.

        Args:
            dataset (Dataset): The dataset instance to be uploaded.
            update_strategy (str): Strategy for handling dataset existence ('create_only', 'update_only', 'create_or_update').
            force_replace (bool): Whether to completely replace an existing dataset (True) or just update properties (False).

        Returns:
            dict: The JSON response from the API containing the dataset data

        Raises:
            ValueError: If the update_strategy is invalid
            HTTPError: If API requests fail
            json.JSONDecodeError: If response parsing fails
        """
        # Preload mapping from DNK to ensure we have the latest mapping data
        try:
            logging.info("Preloading ODS ID to Dataspot mappings from DNK system")
            self._download_and_update_mappings()
        except Exception as e:
            logging.warning(f"Failed to preload mappings, continuing with existing mappings: {str(e)}")
        
        # Validate update strategy
        valid_strategies = ['create_only', 'update_only', 'create_or_update']
        if update_strategy not in valid_strategies:
            logging.error(f"Invalid update_strategy: {update_strategy}. Must be one of {valid_strategies}")
            raise ValueError(f"Invalid update_strategy: {update_strategy}. Must be one of {valid_strategies}")
        
        # Get ODS ID from dataset
        ods_id = dataset.to_json().get('customProperties', {}).get('ODS_ID')
        if not ods_id:
            logging.error("Dataset missing 'ODS_ID' property required for ODS ID")
            raise ValueError("Dataset must have an 'ODS_ID' property to use as ODS ID")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Processing dataset: '{title}' with ODS ID: {ods_id}")
        
        # Check if dataset exists in Dataspot
        dataset_exists = False
        href = None
        
        # Check mapping for existing entry
        logging.debug(f"Checking if dataset with ODS ID {ods_id} exists in mapping")
        entry = self.mapping.get_entry(ods_id)
        if entry:
            dataset_exists = True
            # Build the API href from the UUID (which is the second item in the entry tuple)
            uuid = entry[1]
            href = url_join('rest', self.database_name, 'datasets', uuid, leading_slash=True)
            logging.debug(f"Found existing dataset in mapping with UUID: {uuid}, building href: {href}")
            
            # Verify that the dataset still exists at this href
            logging.debug(f"Verifying dataset still exists at: {href}")
            asset_data = self._get_asset(href)
            if not asset_data:
                # Dataset doesn't exist at the expected location
                logging.warning(f"Dataset no longer exists at {href}, removing from mapping")
                dataset_exists = False
                self.mapping.remove_entry(ods_id)
        
        # Handle according to update strategy
        if dataset_exists:
            if update_strategy == 'create_only':
                logging.error(f"Dataset '{title}' already exists and update_strategy is 'create_only'")
                raise ValueError(f"Dataset '{title}' already exists and update_strategy is 'create_only'")
            
            if update_strategy in ['update_only', 'create_or_update']:
                # Update the existing dataset
                return self.update_dataset(dataset, href, force_replace)
        else:
            if update_strategy == 'update_only':
                logging.error(f"Dataset '{title}' does not exist and update_strategy is 'update_only'")
                raise ValueError(f"Dataset '{title}' does not exist and update_strategy is 'update_only'")
            
            if update_strategy in ['create_only', 'create_or_update']:
                # Create a new dataset
                return self.create_dataset(dataset)
        
        # This should not happen if the code is correct
        logging.error("Unexpected error in create_or_update_dataset")
        raise RuntimeError("Unexpected error in create_or_update_dataset")

    def delete_dataset(self, ods_id: str, fail_if_not_exists: bool = False) -> bool:
        """
        Delete a dataset from the DNK.
        
        Args:
            ods_id (str): The ODS ID of the dataset to delete
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted, False if it didn't exist
            
        Raises:
            ValueError: If the dataset doesn't exist and fail_if_not_exists is True
            HTTPError: If API requests fail
        """
        # Check if the dataset exists in the mapping
        entry = self.mapping.get_entry(ods_id)
        
        if not entry:
            if fail_if_not_exists:
                raise ValueError(f"Dataset with ODS ID '{ods_id}' does not exist in mapping")
            logging.warning(f"Dataset with ODS ID '{ods_id}' not found in mapping, cannot delete.")
            return False
        
        # Get UUID from the entry and build the href dynamically
        _type, uuid, _inCollection = entry
        href = url_join('rest', self.database_name, 'datasets', uuid, leading_slash=True)
        
        # Delete the dataset
        logging.info(f"Deleting dataset with ODS ID '{ods_id}' (UUID: {uuid}) at {href}")
        self._delete_asset(href)
        
        # Remove entry from mapping
        self.mapping.remove_entry(ods_id)
        
        return True

    # TODO (Renato): Remove these two wrappers, and instead, write a sync function similar to sync_staatskalender_org_units and write a wrapper for that function here. Implement the actual function in the mapping function
    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any], validate_urls: bool = False) -> dict:
        """Delegate to org_handler"""
        return self.org_handler.build_organization_hierarchy_from_ods_bulk(org_data, validate_urls)

    def bulk_create_or_update_organizational_units(self, organizational_units: List[Dict[str, Any]], 
                                                  operation: str = "ADD", dry_run: bool = False) -> dict:
        """Delegate to org_handler"""
        return self.org_handler.bulk_create_or_update_organizational_units(organizational_units, operation, dry_run)
