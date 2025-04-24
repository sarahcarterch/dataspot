import logging
from typing import Dict, Any, List

from requests import HTTPError

from src import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, get_uuid_from_response, escape_special_chars
from src.dataspot_dataset import Dataset
from src.common import requests_get # BUT DO NOT IMPORT THESE: requests_post, requests_put, requests_patch
from src.ods_dataspot_mapping import ODSDataspotMapping
from src.staatskalender_dataspot_mapping import StaatskalenderDataspotMapping

class DNKClient(BaseDataspotClient):
    """Client for interacting with the DNK (Datennutzungskatalog)."""
    
    def __init__(self):
        """
        Initialize the DNK client.
        """
        super().__init__()
        
        # Load scheme name from config
        self.database_name = config.database_name
        self.scheme_name = config.dnk_scheme_name
        self.scheme_name_short = config.dnk_scheme_name_short
        
        # Set up mapping
        self.mapping = ODSDataspotMapping(database_name=self.database_name, scheme=self.scheme_name_short)
        
        # Initialize the organization mapping
        self.org_mapping = StaatskalenderDataspotMapping(database_name=self.database_name, scheme=self.scheme_name_short)
        
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
        logging.info(f"Downloading datasets from DNK scheme for mapping update")
        
        # Use the download API to retrieve datasets from the scheme
        download_path = f"/api/{self.database_name}/schemes/{self.scheme_name}/download?format=JSON"
        full_url = url_join(self.base_url, download_path)
        
        logging.debug(f"Downloading datasets from: {full_url}")
        response = requests_get(full_url, headers=self.auth.get_headers())
        response.raise_for_status()
        
        # Parse the JSON response
        datasets = response.json()
        
        # If we got a list directly, use it
        if isinstance(datasets, list):
            # Filter to only include datasets
            datasets = [item for item in datasets if item.get('_type') == 'Dataset' and item.get('ODS_ID')]
            datasets.sort(key=lambda x: x.get('ODS_ID'))
            logging.info(f"Downloaded {len(datasets)} datasets from scheme")
        else:
            # We might have received a job ID instead
            logging.error(f"Received unexpected response format from {full_url}. Expected a list of datasets.")
            logging.debug(f"Response content: {datasets}")
            raise ValueError(f"Unexpected response format from download API. Expected a list but got: {type(datasets)}")
        
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
        
        response = self.create_resource(
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
            response = self.bulk_create_or_update_resources(
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
        response = self.update_resource(
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
            resource_data = self.get_resource_if_exists(href)
            if not resource_data:
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
        self.delete_resource(href)
        
        # Remove entry from mapping
        self.mapping.remove_entry(ods_id)
        
        return True

    def require_scheme_exists(self) -> str:
        """
        Assert that the DNK scheme exists and return its API endpoint. Throw an error if it doesn't.

        Returns:
            str: The API endpoint of the DNK scheme (starting with /rest/...)

        Raises:
            ValueError: If the DNK scheme doesn't exist
            HTTPError: If API requests fail
        """
        dnk_scheme_path = url_join('rest', self.database_name, 'schemes', self.scheme_name)
        scheme_response = self.get_resource_if_exists(dnk_scheme_path)
        if not scheme_response:
            raise ValueError(f"DNK scheme '{self.scheme_name}' does not exist")
        return scheme_response['_links']['self']['href']

    def ensure_ods_imports_collection_exists(self) -> dict:
        """
        Ensures that the ODS-Imports collection exists within the Datennutzungskatalog scheme.
        
        The path is defined by config.ods_imports_collection_path, which is a list of folder names.
        For example, if config.ods_imports_collection_path is ['A', 'B', 'C'], the function:
        1. First checks if 'A/B/C' path already exists
        2. If the path doesn't exist, logs an error and throws an exception
        3. If the path exists, checks if ODS-Imports collection exists at that location
        4. Creates the ODS-Imports collection if it doesn't exist, or returns the existing one if it does

        Returns:
            dict: The JSON response containing information about the ODS-Imports collection
            
        Raises:
            ValueError: If the DNK scheme does not exist or the configured path contains a '/' or the configured path doesn't exist
            HTTPError: If API requests fail
        """
        logging.info("Ensuring ODS-Imports collection exists")
        # Assert that the DNK scheme exists.
        self.require_scheme_exists()

        # Get the path from config
        collection_path = getattr(config, 'ods_imports_collection_path', [])

        # Validate that the path contains only strings
        for item in collection_path:
            if type(item) != str:
                logging.error(f"Path defined in config.py contains {item}, which is not a string.")
                raise ValueError(f"Invalid path component in ods_imports_collection_path: {item}. All path components must be strings.")

        if collection_path:
            logging.debug(f"Using configured path for ODS-Imports: {'/'.join(collection_path)}")
        else:
            logging.debug("No specific path configured for ODS-Imports, using scheme root")

        # Check for special characters that would prevent using business keys
        has_special_chars = False
        for folder in collection_path:
            if '/' in folder:
                has_special_chars = True
                logging.warning(f"Collection path contains forward slashes, which can't be used in business keys: {folder}")
                break
        
        # Check if the configured path exists
        if not collection_path:
            # No path specified, check directly under scheme
            parent_endpoint = url_join('rest', self.database_name, 'schemes', self.scheme_name)
            parent_response = self.get_resource_if_exists(parent_endpoint)
            if not parent_response:
                error_msg = f"Scheme '{self.scheme_name}' does not exist"
                logging.error(error_msg)
                raise ValueError(error_msg)
            
            # Parent exists (scheme root), check if ODS-Imports exists
            ods_imports_endpoint = url_join(parent_endpoint, 'collections', self.ods_imports_collection_name, leading_slash=True)
            collections_endpoint = url_join(parent_endpoint, 'collections', leading_slash=True)
            existing_collection = self.get_resource_if_exists(ods_imports_endpoint)
            
            # Check both existence and correct parent
            ods_imports_exists = False
            if existing_collection:
                # For root collections, parentId should match the scheme UUID
                if 'parentId' in existing_collection and existing_collection['parentId'] == parent_response['id']:
                    ods_imports_exists = True
                else:
                    logging.warning(f"Found ODS-Imports collection but it's not under the expected parent. Will create new one.")
            
        elif has_special_chars:
            error_msg = ("Path contains special characters that prevent using business keys. Fix the path in config. "
                         "Using Collections that contain a slash is currently not supported in ODS-Imports path. "
                         "Implementing this would be time-consuming and likely introduce errors. "
                         "Also, I don't think this error will ever happen, so I will not fix it at the moment.")
            logging.error(error_msg)
            raise ValueError(error_msg)
        else:
            # Construct business key path to check if the full path exists
            # Format: /rest/{db}/schemes/{scheme}/collections/{col1}/collections/{col2}/...
            path_elements = ['rest', self.database_name, 'schemes', self.scheme_name]
            
            # Build up the path with 'collections' between each element
            for folder in collection_path:
                path_elements.append('collections')
                path_elements.append(folder)

            # Check if the parent path exists
            parent_path = url_join(*path_elements, leading_slash=True)
            parent_response = self.get_resource_if_exists(parent_path)
            
            if not parent_response:
                # Parent path doesn't exist - throw error instead of creating it
                error_msg = (f"Configured path '{'/'.join(collection_path)}' not found. "
                             f"Please make sure the ods_imports_collection_path field in config.py is set correctly!")
                logging.error(error_msg)
                raise ValueError(error_msg)
            
            # Parent path exists, check if ODS-Imports exists under it
            collections_endpoint = url_join(parent_path, 'collections', leading_slash=True)
            
            # Create ODS-Imports endpoint for checking existence
            ods_imports_elements = path_elements.copy()
            ods_imports_elements.append('collections')
            ods_imports_elements.append(self.ods_imports_collection_name)
            ods_imports_endpoint = url_join(*ods_imports_elements, leading_slash=True)
            existing_collection = self.get_resource_if_exists(ods_imports_endpoint)
            
            # Check both existence and correct parent
            ods_imports_exists = False
            if existing_collection:
                # Verify the collection is under the expected parent
                if 'parentId' in existing_collection and existing_collection['parentId'] == parent_response['id']:
                    ods_imports_exists = True
                else:
                    logging.warning(f"Found ODS-Imports collection but it's not under the expected parent. Will create new one.")
        
        try:
            # Return existing or create new
            if ods_imports_exists:
                logging.debug(f"ODS-Imports collection already exists under the correct parent, using it as is")
                path_str = "/".join(collection_path) if collection_path else "scheme root"
                logging.info(f"ODS-Imports collection found at: {path_str}")
                return existing_collection
            else:
                logging.debug(f"ODS-Imports collection does not exist under the correct parent, creating it")
                collection_data = {
                    "label": self.ods_imports_collection_name,
                    "_type": "Collection"
                }
                response_json = self.create_resource(
                    endpoint=collections_endpoint, 
                    data=collection_data
                )
                path_str = "/".join(collection_path) if collection_path else "scheme root"
                logging.info(f"Created ODS-Imports collection at: {path_str}")
                return response_json
                
        except HTTPError as create_error:
            logging.error(f"Failed to create ODS-Imports collection: {str(create_error)}")
            raise

    def bulk_create_or_update_organizational_units(self, organizational_units: List[Dict[str, Any]], 
                                        operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple organizational units in bulk in the Datennutzungskatalog scheme in Dataspot.
        
        Args:
            organizational_units (List[Dict[str, Any]]): List of organizational unit data to upload
            operation (str, optional): Upload operation mode. Defaults to "ADD".
                                      "ADD": Add or update only. Existing units not in the upload remain unchanged.
                                      "REPLACE": Reconcile elements. Units not in the upload are considered obsolete.
                                      "FULL_LOAD": Reconcile model. Completely replaces with the uploaded units.
            dry_run (bool, optional): Whether to perform a test run without changing data. Defaults to False.
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If no organizational units are provided or if the DNK scheme doesn't exist
            HTTPError: If API requests fail
        """
        # Verify we have organizational units to process
        if not organizational_units:
            logging.warning("No organizational units provided for bulk upload")
            return {"status": "error", "message": "No organizational units provided"}
        
        # Ensure DNK scheme exists
        try:
            self.require_scheme_exists()
        except HTTPError as e:
            logging.error(f"HTTP error ensuring DNK scheme exists: {str(e)}")
            raise
        except ValueError as e:
            logging.error(f"Error while ensuring that scheme exists: {str(e)}. Did you correctly set the database name in the config (currently it is: {self.database_name})?")
            raise ValueError(f"Error while ensuring that scheme exists: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error ensuring DNK scheme exists: {str(e)}")
            raise ValueError(f"Could not access scheme information: {str(e)}")
        
        # Count of units
        num_units = len(organizational_units)
        logging.debug(f"Bulk creating {num_units} organizational units (operation: {operation}, dry_run: {dry_run})...")
        
        # Bulk create organizational units using the scheme name
        try:
            response = self.bulk_create_or_update_resources(
                scheme_name=self.scheme_name,
                data=organizational_units,
                operation=operation,
                dry_run=dry_run
            )

            logging.info(f"Bulk creation of organizational units complete")
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
            
    def get_validated_staatskalender_url(self, title: str, url_website: str, validate_url: bool = False) -> str:
        """
        Validate a Staatskalender URL for an organization or use the provided URL.
        
        Args:
            title (str): The organization title
            url_website (str): The URL provided in the data
            validate_url (bool): Whether to validate the URL by making an HTTP request
            
        Returns:
            str: The validated URL for the organization, or empty string if invalid or validation fails
            
        Note:
            If validation fails or no URL is provided, an empty string is returned.
            No exceptions are raised from this method, validation errors are logged.
        """
        # If URL is already provided, optionally validate it
        if url_website:
            if not validate_url:
                return url_website
                
            # Validate the provided URL
            try:
                response = requests_get(url_website)
                if response.status_code == 200:
                    return url_website
                logging.warning(f"Invalid provided URL for organization '{title}': {url_website}")
            except Exception as e:
                logging.warning(f"Error validating URL for organization '{title}': {url_website}")
                
        # If no URL or validation failed, return empty string
        return ""

    def transform_organization_for_bulk_upload(self, org_data: Dict[str, Any], validate_urls: bool = False) -> List[Dict[str, Any]]:
        """
        Build organization hierarchy from flat data using parent_id and children_id fields.
        
        Args:
            org_data (Dict[str, Any]): Organization data from ODS API
            validate_urls (bool): Whether to validate Staatskalender URLs
            
        Returns:
            List[Dict[str, Any]]: List of organizational units with hierarchy info
            
        Raises:
            ValueError: If organization data is invalid, missing 'results' key, or no root nodes found
            Exception: If there's an error processing children_id fields
        """
        if not org_data or 'results' not in org_data:
            logging.error("Invalid organization data format. Data must contain a 'results' key.")
            raise ValueError("Invalid organization data format. Data must contain a 'results' key.")
        
        # TODO (Renato): Check what the id is in the api of the ods dataset 100349. If it is an int, we should treat it as an int. If it is a string, we should question it.
        # Build a lookup dictionary for quick access to organization by ID
        org_lookup : Dict[str, Dict[str, Any]] = {str(org['id']): org for org in org_data['results']} # Assume that the id is a string
        logging.info(f"Processing {len(org_lookup)} organizations from input data")
        
        # Create a dictionary to track parent-child relationships
        parent_child_map = {}
        
        # Track missing entries that are referenced
        missing_entries = set()
        
        # Track organizations that reference missing parents - these will be excluded
        invalid_orgs = set()
        
        # Process each organization to build parent-child relationships
        for org_id, org in org_lookup.items():
            # Get parent ID
            parent_id = str(org.get('parent_id', '')).strip()
            if parent_id and parent_id.lower() != 'nan':
                # Check if parent exists
                if parent_id not in org_lookup:
                    missing_entries.add(parent_id)
                    logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing parent {parent_id}. Skipping...")
                    # Mark this organization as invalid (has missing parent)
                    invalid_orgs.add(org_id)
                    continue
                
                # Add this org as a child of its parent
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = []
                parent_child_map[parent_id].append(org_id)
            
            # Check children IDs for consistency
            children_ids = org.get('children_id', '')
            if children_ids:
                # Parse the children IDs - they might be in various formats
                try:
                    # TODO (Renato): We don't need this. If it exists, then it is a list with strings of ids (I think). Throw an error otherwise.
                    if isinstance(children_ids, str):
                        # Try to split by comma if it's a string
                        children_list = [id.strip() for id in children_ids.split(',')]
                    elif isinstance(children_ids, list):
                        children_list = [str(id).strip() for id in children_ids]
                    else:
                        children_list = [str(children_ids).strip()]
                    
                    # Add these children to the map if not already added by parent_id
                    if org_id not in parent_child_map:
                        parent_child_map[org_id] = []
                    
                    # Check each child
                    for child_id in children_list:
                        if child_id and child_id.lower() != 'nan':
                            if child_id not in org_lookup:
                                missing_entries.add(child_id)
                                logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing child {child_id}")
                            else:
                                # Only add if not already in the list
                                if child_id not in parent_child_map[org_id]:
                                    parent_child_map[org_id].append(child_id)
                except Exception as e:
                    logging.error(f"Error processing children_id for organization {org_id}: {children_ids}")
                    logging.error(f"Exception: {str(e)}")
                    raise
        
        # Log all missing entries
        if missing_entries:
            logging.warning(f"Found {len(missing_entries)} missing organizations referenced in the data: {', '.join(missing_entries)}")
        
        # Find root nodes (those without parents or with 'nan' as parent)
        root_nodes = []
        for org_id, org in org_lookup.items():
            # Skip organizations with missing parents
            if org_id in invalid_orgs:
                continue
                
            parent_id = str(org.get('parent_id', '')).strip()
            if not parent_id:
                logging.info(f"Organization {org_id} ('{org.get('title', 'Unknown')}') has no parent ID. Treating as root node.")
                root_nodes.append(org_id)
            elif parent_id.lower() == 'nan':
                logging.info(f"Organization {org_id} ('{org.get('title', 'Unknown')}') has 'nan' as parent ID. Treating as root node.")
                root_nodes.append(org_id)
        
        if not root_nodes:
            logging.error("No root nodes found in organization data. Hierarchy cannot be built.")
            raise ValueError("No root nodes found in organization data. Hierarchy cannot be built.")
        
        logging.info(f"Found {len(root_nodes)} root nodes")
        logging.info(f"Start construction of organizational structure - Not yet uploading...")

        # Dictionary to store organization titles by ID for lookup
        org_title_by_id = {org_id: org.get('title', '').strip() for org_id, org in org_lookup.items()}
        
        # Dictionary to track the path components for each organization
        # For root nodes, the path is just their title as a single component
        # For other nodes, it will be built recursively as a list of components
        # This avoids problems with slashes in organization names
        path_components_by_id = {}
        for root_id in root_nodes:
            path_components_by_id[root_id] = [org_title_by_id.get(root_id, '')]
        
        # Function to check if an organization or any of its ancestors is invalid
        def has_invalid_ancestor(org_id):
            if org_id in invalid_orgs:
                return True
                
            parent_id = str(org_lookup.get(org_id, {}).get('parent_id', '')).strip()
            if not parent_id or parent_id.lower() == 'nan' or parent_id not in org_lookup:
                return False
                
            return has_invalid_ancestor(parent_id)
        
        # Function to get or build the path components for an organization
        def get_path_components(org_id):
            # If path already calculated, return it
            if org_id in path_components_by_id:
                return path_components_by_id[org_id]
            
            # Get the organization's title
            title = org_title_by_id.get(org_id, '')
            if not title:
                logging.warning(f"Organization {org_id} has no title, using ID as title")
                title = f"Unknown-{org_id}"
            
            # Get the parent ID
            parent_id = str(org_lookup.get(org_id, {}).get('parent_id', '')).strip()
            if not parent_id or parent_id.lower() == 'nan' or parent_id not in org_lookup:
                # If no valid parent, this is effectively a root node
                path_components = [title]
                path_components_by_id[org_id] = path_components
                return path_components
            
            # Get the parent's path components recursively
            parent_path_components = get_path_components(parent_id)
            
            # Create the full path components by appending this org's title
            path_components = parent_path_components + [title]
            
            # Store the components in the mapping
            path_components_by_id[org_id] = path_components
            return path_components
        
        # Now construct hierarchical data for each organization
        all_units = []
        processed_ids = set()
        excluded_ids = set()  # Track excluded organizations
        
        # Recursively identify and exclude all descendants of invalid organizations
        def mark_descendants_as_invalid(org_id):
            if org_id in parent_child_map:
                for child_id in parent_child_map[org_id]:
                    invalid_orgs.add(child_id)
                    excluded_ids.add(child_id)
                    mark_descendants_as_invalid(child_id)
        
        # Mark all descendants of invalid organizations as invalid too
        for org_id in list(invalid_orgs):
            mark_descendants_as_invalid(org_id)
        
        if excluded_ids:
            logging.warning(f"Excluding {len(excluded_ids)} organizations with missing parents or ancestors: {', '.join(excluded_ids)}")
        
        # BFS traversal to build hierarchy level by level
        for depth in range(100):  # Safety limit to prevent infinite loops
            logging.info(f"Processing depth level {depth}")
            current_level = []
            
            # For depth 0, start with root nodes
            if depth == 0:
                current_level = root_nodes
            else:
                # Find all children of the previous level's nodes
                for parent_id in processed_ids:  # Check all processed nodes for children
                    if parent_id in parent_child_map:
                        for child_id in parent_child_map[parent_id]:
                            if child_id not in processed_ids and child_id not in invalid_orgs:
                                current_level.append(child_id)
            
            # If no more nodes at this level, we're done
            if not current_level:
                logging.info(f"No more nodes to process at depth {depth}. Hierarchy build complete.")
                break
            
            logging.info(f"Processing {len(current_level)} organizations at depth {depth}")
            
            # Process each node at this level
            for org_id in current_level:
                # Skip if already processed
                if org_id in processed_ids:
                    continue
                
                # Skip if this org or any of its ancestors is invalid
                if org_id in invalid_orgs:
                    continue
                
                # Mark as processed
                processed_ids.add(org_id)
                
                # Get organization data
                if org_id not in org_lookup:
                    logging.warning(f"Organization ID {org_id} referenced but not found in data")
                    continue
                
                org = org_lookup[org_id]
                title = org.get('title', '')
                if not title:
                    logging.warning(f"Organization {org_id} missing title, skipping")
                    continue
                
                # Get or validate URL
                url_website = self.get_validated_staatskalender_url(
                    title, 
                    org.get('url_website', ''), 
                    validate_url=validate_urls
                )
                
                # Create unit data
                unit_data = {
                    "_type": "Collection",
                    "label": title.strip(),
                    "stereotype": "Organisationseinheit",
                    "_hierarchy_depth": depth,
                    "id_im_staatskalender": org_id
                }
                
                # Add custom properties
                custom_properties = {}
                if url_website:
                    custom_properties["link_zum_staatskalender"] = url_website
                if org_id:
                    custom_properties["id_im_staatskalender"] = org_id
                if custom_properties:
                    unit_data["customProperties"] = custom_properties
                
                # Set parent relationship using hierarchical business key
                parent_id = str(org.get('parent_id', '')).strip()
                if parent_id and parent_id.lower() != 'nan' and parent_id in org_lookup:
                    # Get the parent's path components
                    parent_path_components = get_path_components(parent_id)
                    
                    if parent_path_components:
                        # Escape each path component individually
                        escaped_components = []
                        
                        for i, comp in enumerate(parent_path_components):
                            # Log before escaping for components with special characters
                            if any(char in comp for char in ['/', '.', '"']):
                                logging.debug(f"Component {i} before escaping: '{comp}'")
                            
                            # Escape special characters in this component
                            escaped_comp = escape_special_chars(comp)
                            
                            # Log after escaping for components with special characters
                            if any(char in comp for char in ['/', '.', '"']):
                                logging.debug(f"Component {i} after escaping: '{escaped_comp}'")
                                
                            escaped_components.append(escaped_comp)
                        
                        # Join the escaped components with slashes
                        escaped_parent_path = '/'.join(escaped_components)
                        
                        # For debugging, show original path components and final escaped path
                        original_path = '/'.join(parent_path_components)
                        logging.debug(f"Original path components: {parent_path_components}")
                        logging.debug(f"Escaped path: '{escaped_parent_path}'")
                        
                        unit_data["inCollection"] = escaped_parent_path
                        logging.debug(f"Setting inCollection for '{title}' to '{escaped_parent_path}'")
                
                # Add to the units list
                all_units.append(unit_data)
        
        # Calculate total invalid organizations (those with invalid ancestors)
        total_excluded = len(invalid_orgs) + len(excluded_ids)
        logging.info(f"Built hierarchy with {len(all_units)} organizational units (excluded {total_excluded} due to missing parents/ancestors)")
        
        # Check for any organizations not included in the hierarchy
        not_processed = set(org_lookup.keys()) - processed_ids - invalid_orgs - excluded_ids
        if not_processed:
            logging.warning(f"{len(not_processed)} organizations not included in the hierarchy due to circular references or other issues")
            
        return all_units

    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any], validate_urls: bool = False) -> dict:
        """
        Build organization hierarchy in the DNK based on data from ODS API using bulk upload.
        
        This method uses parent_id and children_id to build the hierarchy, not title_full.
        It processes the hierarchy level by level.
        
        Args:
            org_data (Dict[str, Any]): Dictionary containing organization data from ODS API
            validate_urls (bool): Whether to validate Staatskalender URLs (can be slow)
            
        Returns:
            dict: The response from the final bulk upload API call with status information
            
        Raises:
            ValueError: If organization data is invalid or no organizational units can be built
            HTTPError: If bulk upload API requests fail
        """
        logging.info("Building organization hierarchy using level-by-level bulk upload...")
        
        # Preload mappings from DNK to ensure we have the latest mapping data
        try:
            logging.info("Preloading Staatskalender ID to Dataspot mappings from DNK system")
            self._download_and_update_staatskalender_mappings()
        except Exception as e:
            logging.warning(f"Failed to preload organizational unit mappings, continuing with existing mappings: {str(e)}")
            
        # Transform organization data for bulk upload
        org_units = self.transform_organization_for_bulk_upload(org_data, validate_urls=validate_urls)
        
        if not org_units:
            logging.warning("No organizational units to upload")
            return {"status": "error", "message": "No organizational units to upload"}
        
        # Extract Staatskalender IDs from all units for mapping updates
        staatskalender_ids = []
        for unit in org_units:
            staatskalender_id = unit.get("id_im_staatskalender")
            if staatskalender_id:
                staatskalender_ids.append(staatskalender_id)
        
        # Group units by their depth in the hierarchy
        units_by_depth = {}
        for unit in org_units:
            # Get the hierarchy depth directly from the unit
            depth = unit.pop("_hierarchy_depth", 0)  # Remove the depth field and use its value
            
            # Add to the appropriate depth group
            if depth not in units_by_depth:
                units_by_depth[depth] = []
            
            units_by_depth[depth].append(unit)
        
        # Track uploaded units to handle failures
        level_result = {}
        upload_errors = []
        
        # Process each depth level in order
        for depth in sorted(units_by_depth.keys()):
            level_units = units_by_depth[depth]
            if not level_units:
                logging.info(f"No units to upload at depth level {depth}, skipping")
                continue
                
            logging.info(f"Uploading {len(level_units)} organizational units at depth level {depth}...")
            
            try:
                # Perform bulk upload for this level
                response = self.bulk_create_or_update_organizational_units(
                    organizational_units=level_units,
                    operation="ADD",
                    dry_run=False
                )
                
                # Store the result for this level
                level_result[depth] = response
                logging.info(f"Successfully uploaded {len(level_units)} units at depth level {depth}")
                
                # If this level has relationship errors, log them but continue
                if 'errors' in response and response.get('errors'):
                    error_count = len(response.get('errors', []))
                    logging.warning(f"Upload for level {depth} completed with {error_count} errors")
                    
                    # Log up to 10 errors in detail
                    for i, error in enumerate(response.get('errors', [])[:10]):
                        error_msg = error.get('error', str(error))
                        upload_errors.append(f"Level {depth} error: {error_msg}")
                        logging.warning(f"Level {depth} upload error {i+1}: {error_msg}")
                    
                    if error_count > 10:
                        logging.warning(f"... and {error_count - 10} more errors")
                
            except HTTPError as e:
                error_msg = f"HTTP error uploading level {depth}: {str(e)}"
                logging.error(error_msg)
                upload_errors.append(error_msg)
                
                # Try to extract details from the response for debugging
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_details = e.response.json()
                        # Log the first few errors
                        if 'errors' in error_details:
                            error_count = len(error_details.get('errors', []))
                            for i, err in enumerate(error_details.get('errors', [])[:10]):
                                logging.error(f"Error {i+1}: {err}")
                            if error_count > 10:
                                logging.error(f"...and {error_count - 10} more errors")
                    except:
                        logging.error(f"Error response: {e.response.text[:500]}")
                
            except Exception as e:
                error_msg = f"Error uploading level {depth}: {str(e)}"
                logging.error(error_msg)
                upload_errors.append(error_msg)
        
        # Update mappings for all uploaded organizations
        try:
            if staatskalender_ids:
                logging.info(f"Updating mappings for {len(staatskalender_ids)} organizational units")
                self.update_staatskalender_mappings_from_upload(staatskalender_ids)
        except Exception as e:
            logging.warning(f"Error updating organizational unit mappings: {str(e)}")
            # Add to errors but continue
            upload_errors.append(f"Error updating mappings: {str(e)}")
        
        logging.info("Organization hierarchy build completed")
        
        # Determine overall result
        if upload_errors:
            logging.warning(f"Upload completed with {len(upload_errors)} errors")
            
            # Sample up to 10 errors for the result
            sample_errors = upload_errors[:10]
            if len(upload_errors) > 10:
                sample_errors.append(f"... and {len(upload_errors) - 10} more errors")
                
            result = {
                "status": "partial", 
                "message": "Hierarchy build completed with errors",
                "errors": sample_errors,
                "total_errors": len(upload_errors),
                "levels_processed": len(level_result)
            }
        else:
            result = {
                "status": "success",
                "message": "Hierarchy build completed successfully",
                "levels_processed": len(level_result)
            }
            
        return result

    def _download_and_update_staatskalender_mappings(self, target_staatskalender_ids: List[str] = None) -> int:
        """
        Helper method to download organizational units and update Staatskalender ID to Dataspot UUID mappings.
        
        Args:
            target_staatskalender_ids (List[str], optional): If provided, only update mappings for these Staatskalender IDs.
                                                 If None, update all mappings found.
        
        Returns:
            int: Number of mappings successfully updated
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If the response format is unexpected or invalid
        """
        logging.info(f"Downloading organizational units from DNK scheme for mapping update")
        
        # Use the download API to retrieve collections from the scheme
        download_path = f"/api/{self.database_name}/schemes/{self.scheme_name}/download?format=JSON"
        full_url = url_join(self.base_url, download_path)
        
        logging.debug(f"Downloading collections from: {full_url}")
        response = requests_get(full_url, headers=self.auth.get_headers())
        response.raise_for_status()
        
        # Parse the JSON response
        all_items = response.json()
        
        # If we got a list directly, use it
        if isinstance(all_items, list):
            # Filter to only include collections with stereotype Organisationseinheit and id_im_staatskalender
            orgs = [item for item in all_items if 
                    item.get('_type') == 'Collection' and 
                    item.get('stereotype') == 'Organisationseinheit' and 
                    item.get('id_im_staatskalender')]
            
            # Sort by id_im_staatskalender
            orgs.sort(key=lambda x: x.get('id_im_staatskalender'))
            logging.info(f"Downloaded {len(orgs)} organizational units from scheme")
        else:
            # We might have received a job ID instead
            logging.error(f"Received unexpected response format from {full_url}. Expected a list of items.")
            logging.debug(f"Response content: {all_items}")
            raise ValueError(f"Unexpected response format from download API. Expected a list but got: {type(all_items)}")
        
        if not orgs:
            logging.warning(f"No organizational units with id_im_staatskalender found in {self.scheme_name}")
            return 0
        
        # Create a lookup dictionary for faster access
        org_by_staatskalender_id = {}
        for org in orgs:
            # Get the id_im_staatskalender field
            staatskalender_id_raw = org.get('id_im_staatskalender')
            if staatskalender_id_raw is not None: # Check if the ID exists
                # Ensure the key is always a string
                staatskalender_id_str = str(staatskalender_id_raw)
                org_by_staatskalender_id[staatskalender_id_str] = org
            else:
                # Log if an org unit is missing the ID, although the earlier filter should prevent this
                logging.warning(f"Found organizational unit missing 'id_im_staatskalender': {org.get('id', 'Unknown ID')}")
        
        # Process each org and update the mapping
        updated_count = 0
        
        # Check for orgs in mapping that are not in downloaded orgs
        if not target_staatskalender_ids:
            removed_count = 0
            for staatskalender_id, entry in list(self.org_mapping.mapping.items()):
                if staatskalender_id not in org_by_staatskalender_id:
                    logging.warning(f"Organizational unit {staatskalender_id} exists in local mapping but not in dataspot. Removing from local mapping.")
                    if len(entry) >= 2:
                        _type, uuid = entry[0], entry[1]
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}")
                    self.org_mapping.remove_entry(staatskalender_id)
                    removed_count += 1
            if removed_count > 0:
                logging.info(f"Found {removed_count} organizational units that exist locally but not in dataspot.")
        
        # If we have target IDs, prioritize those
        if target_staatskalender_ids and len(target_staatskalender_ids) > 0:
            total_targets = len(target_staatskalender_ids)
            target_staatskalender_ids.sort()
            for idx, staatskalender_id in enumerate(target_staatskalender_ids, 1):
                logging.debug(f"[{idx}/{total_targets}] Processing organizational unit with Staatskalender ID: {staatskalender_id}")
                
                org = org_by_staatskalender_id.get(staatskalender_id)
                
                # Skip if we couldn't find the organization
                if org is None:
                    logging.warning(f"Could not find organizational unit with Staatskalender ID {staatskalender_id} in downloaded data, skipping")
                    continue
                
                # Get the UUID and _type
                uuid = org.get('id')
                _type = org.get('_type')
                if not uuid:
                    logging.warning(f"Organizational unit with Staatskalender ID {staatskalender_id} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Organizational unit with Staatskalender ID {staatskalender_id} missing _type, skipping")
                    continue
                
                # Extract inCollection business key directly from the downloaded org
                inCollection_key = org.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.org_mapping.get_entry(staatskalender_id)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in organizational unit {staatskalender_id}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update organizational unit {staatskalender_id} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating organizational unit {staatskalender_id} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Organizational unit {staatskalender_id} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been removed from '{old_inCollection}'")
                        
                        logging.debug(f"    - old_type: {old_type}, old_uuid: {old_uuid}, old_inCollection: {old_inCollection}")
                        logging.debug(f"    - new_type: {_type}, new_uuid: {uuid}, new_inCollection: {inCollection_key}")
                        self.org_mapping.add_entry(staatskalender_id, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add organizational unit {staatskalender_id} with uuid {uuid}")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                        self.org_mapping.add_entry(staatskalender_id, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for organizational unit with Staatskalender ID: {staatskalender_id}")
        else:
            # No target IDs, process all orgs
            total_orgs = len(orgs)
            for idx, org in enumerate(orgs, 1):
                # Extract the ID
                staatskalender_id_raw = org.get('id_im_staatskalender')
                
                # Skip if this org doesn't have a Staatskalender ID (should already be filtered, but check anyway)
                if staatskalender_id_raw is None:
                    continue
                
                # Ensure we use the string version for lookup/processing
                staatskalender_id = str(staatskalender_id_raw)
                
                logging.debug(f"[{idx}/{total_orgs}] Processing organizational unit with Staatskalender ID: {staatskalender_id}")
                
                # Get the UUID and _type
                uuid = org.get('id')
                _type = org.get('_type') # Get the _type
                if not uuid:
                    logging.warning(f"Organizational unit with Staatskalender ID {staatskalender_id} missing UUID, skipping")
                    continue
                if not _type:
                    logging.warning(f"Organizational unit with Staatskalender ID {staatskalender_id} missing _type, skipping")
                    continue

                # Extract inCollection business key directly from the downloaded org
                inCollection_key = org.get('inCollection')
                
                if uuid and _type:
                    # Check if the mapping has changed before updating
                    existing_entry = self.org_mapping.get_entry(staatskalender_id)
                    # Existing entry tuple: (_type, uuid, inCollection)
                    if (existing_entry and
                            existing_entry[0] == _type and
                            existing_entry[1] == uuid and
                            existing_entry[2] == inCollection_key):
                        logging.debug(f"No changes in organizational unit {staatskalender_id}. Skipping")
                        logging.debug(f"    - _type: {_type}, uuid: {uuid}, inCollection: {inCollection_key}")
                    elif existing_entry:
                        old_type = existing_entry[0] if len(existing_entry) > 0 else None
                        old_uuid = existing_entry[1] if len(existing_entry) > 1 else None
                        old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None

                        # Only log UUID update warning if the UUID actually changed
                        if old_uuid != uuid:
                            logging.warning(f"Update organizational unit {staatskalender_id} uuid from {old_uuid} to {uuid}")
                        else:
                            logging.info(f"Updating organizational unit {staatskalender_id} metadata")

                        # Log changes in type if they occur
                        if old_type != _type:
                            logging.warning(f"Organizational unit {staatskalender_id} type changed from '{old_type}' to '{_type}'")

                        # Log a more meaningful message if inCollection has changed
                        if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                        elif not old_inCollection and inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been placed in '{inCollection_key}'")
                        elif old_inCollection and not inCollection_key:
                            logging.info(f"Organizational unit {staatskalender_id} has been removed from '{old_inCollection}'")
                        
                        self.org_mapping.add_entry(staatskalender_id, _type, uuid, inCollection_key)
                        updated_count += 1
                    else:
                        logging.debug(f"Add organizational unit {staatskalender_id} with uuid {uuid}")
                        self.org_mapping.add_entry(staatskalender_id, _type, uuid, inCollection_key)
                        updated_count += 1
                else:
                    logging.warning(f"Missing UUID or _type for organizational unit with Staatskalender ID: {staatskalender_id}")
        
        return updated_count
    
    def update_staatskalender_mappings_from_upload(self, staatskalender_ids: List[str]) -> None:
        """
        Updates the mapping between Staatskalender IDs and Dataspot UUIDs after uploading organizational units.
        Uses the download API to retrieve all organizational units and then updates the mapping for matching Staatskalender IDs.
        
        Args:
            staatskalender_ids (List[str]): List of Staatskalender IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve organizational unit information
        """
        logging.info(f"Updating mappings for {len(staatskalender_ids)} organizational units using download API")
        
        try:
            updated_count = self._download_and_update_staatskalender_mappings(staatskalender_ids)
            logging.info(f"Updated mappings for {updated_count} out of {len(staatskalender_ids)} organizational units")
            
            if updated_count < len(staatskalender_ids):
                missing_ids = [staatskalender_id for staatskalender_id in staatskalender_ids if not self.org_mapping.get_entry(staatskalender_id)]
                if missing_ids:
                    logging.warning(f"Could not find mappings for {len(missing_ids)} Staatskalender IDs: {missing_ids[:5]}" + 
                                   (f"... and {len(missing_ids)-5} more" if len(missing_ids) > 5 else ""))
        except Exception as e:
            logging.error(f"Error updating organizational unit mappings: {str(e)}")
            raise
