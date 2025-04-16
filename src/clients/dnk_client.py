import logging
from requests import RequestException, ConnectionError, Timeout
from typing import Dict, Any, List
from time import sleep

from requests import HTTPError

from src import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, generate_potential_staatskalender_url, get_uuid_and_href_from_response
from src.dataspot_dataset import Dataset
from src.common import requests_get # BUT DO NOT IMPORT THESE: requests_post, requests_put, requests_patch
from src.ods_dataspot_mapping import ODSDataspotMapping

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
        
        # Set up mapping
        self.mapping = ODSDataspotMapping(database_name=self.database_name)
        
    def _download_and_update_mappings(self, target_ods_ids: List[str] = None) -> int:
        """
        Helper method to download datasets and update ODS ID to Dataspot UUID/href mappings.
        
        Args:
            target_ods_ids (List[str], optional): If provided, only update mappings for these ODS IDs.
                                                 If None, update all mappings found.
        
        Returns:
            int: Number of mappings successfully updated
            
        Raises:
            HTTPError: If API requests fail
        """
        logging.info(f"Downloading datasets from DNK scheme for mapping update")
        
        # Use the download API to retrieve datasets from the scheme
        download_path = f"/api/{self.database_name}/schemes/{self.scheme_name}/download?format=JSON"
        full_url = url_join(self.base_url, download_path)
        
        logging.debug(f"Downloading datasets from: {full_url}")
        try:
            response = requests_get(full_url, headers=self.auth.get_headers())
            response.raise_for_status()
        except HTTPError as e:
            logging.error(f"Failed to download datasets from scheme: {str(e)}")
            raise
        
        # Parse the JSON response
        try:
            datasets = response.json()
            
            # If we got a list directly, use it
            if isinstance(datasets, list):
                # Filter to only include datasets
                datasets = [item for item in datasets if item.get('_type') == 'Dataset' and item.get('ODS_ID')]
                datasets.sort(key=lambda x: x.get('ODS_ID'))
                logging.info(f"Downloaded {len(datasets)} datasets from scheme")
            else:
                # We might have received a job ID instead
                logging.warning("Received unexpected response format. Expected a list of datasets.")
                logging.debug(f"Response: {datasets}")
                return 0
            
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
                            uuid, href = entry[0], entry[1]
                            logging.debug(f"    - uuid: {uuid}, href: {href}")
                        self.mapping.remove_entry(ods_id)
                        removed_count += 1
                if removed_count > 0:
                    logging.info(f"Found {removed_count} datasets that exist locally but not in dataspot.")
            
            # If we have target IDs, prioritize those
            if target_ods_ids and len(target_ods_ids) > 0:
                total_targets = len(target_ods_ids)
                target_ods_ids.sort()
                for idx, ods_id in enumerate(target_ods_ids, 1):
                    logging.info(f"[{idx}/{total_targets}] Processing dataset with ODS ID: {ods_id}")
                    
                    # Find this ODS ID in our downloaded datasets
                    dataset = dataset_by_ods_id.get(ods_id)
                    if not dataset:
                        logging.warning(f"Could not find dataset with ODS ID {ods_id} in downloaded data, skipping")
                        continue
                    
                    # Get the UUID
                    uuid = dataset.get('id')
                    if not uuid:
                        logging.warning(f"Dataset with ODS ID {ods_id} missing UUID, skipping")
                        continue
                    
                    # Build the href directly instead of fetching the full dataset
                    href = '/' + url_join('rest', self.database_name, 'datasets', uuid)
                    
                    # Extract inCollection business key directly from the downloaded dataset
                    inCollection_key = dataset.get('inCollection')
                    
                    if uuid and href:
                        # Check if the mapping has changed before updating
                        existing_entry = self.mapping.get_entry(ods_id)
                        if existing_entry and len(existing_entry) > 2 and existing_entry[0] == uuid and existing_entry[1] == href and existing_entry[2] == inCollection_key:
                            logging.info(f"No changes in dataset {ods_id}. Skipping")
                            logging.debug(f"    - uuid: {uuid}, href: {href}, inCollection: {inCollection_key}")
                        elif existing_entry:
                            old_uuid = existing_entry[0] if len(existing_entry) > 0 else None
                            old_href = existing_entry[1] if len(existing_entry) > 1 else None
                            old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None
                            
                            # Only log UUID update warning if the UUID actually changed
                            if old_uuid != uuid:
                                logging.warning(f"Update dataset {ods_id} uuid from {old_uuid} to {uuid}")
                            else:
                                logging.info(f"Updating dataset {ods_id} metadata")
                            
                            # Log a more meaningful message if inCollection has changed
                            if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                                logging.info(f"Dataset {ods_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                            elif not old_inCollection and inCollection_key:
                                logging.info(f"Dataset {ods_id} has been placed in '{inCollection_key}'")
                            elif old_inCollection and not inCollection_key:
                                logging.info(f"Dataset {ods_id} has been removed from '{old_inCollection}'")
                            
                            logging.debug(f"    - old_uuid: {old_uuid}, old_href: {old_href}, old_inCollection: {old_inCollection}")
                            logging.debug(f"    - new_uuid: {uuid}, new_href: {href}, new_inCollection: {inCollection_key}")
                            self.mapping.add_entry(ods_id, uuid, href, inCollection_key)
                            updated_count += 1
                        else:
                            logging.info(f"Add dataset {ods_id} with uuid {uuid}")
                            logging.debug(f"    - uuid: {uuid}, href: {href}, inCollection: {inCollection_key}")
                            self.mapping.add_entry(ods_id, uuid, href, inCollection_key)
                            updated_count += 1
                    else:
                        logging.warning(f"Missing UUID or href for dataset with ODS ID: {ods_id}")
            else:
                # No target IDs, process all datasets
                total_datasets = len(datasets)
                for idx, dataset in enumerate(datasets, 1):
                    # Extract the ID from customProperties
                    ods_id = dataset.get('ODS_ID')
                    
                    # Skip if this dataset doesn't have an ODS ID
                    if not ods_id:
                        continue
                    
                    logging.info(f"[{idx}/{total_datasets}] Processing dataset with ODS ID: {ods_id}")
                    
                    # Get the UUID
                    uuid = dataset.get('id')
                    if not uuid:
                        logging.warning(f"Dataset with ODS ID {ods_id} missing UUID, skipping")
                        continue
                    
                    # Build the href directly instead of fetching the full dataset
                    href = '/' + url_join('rest', self.database_name, 'datasets', uuid)
                    
                    # Extract inCollection business key directly from the downloaded dataset
                    inCollection_key = dataset.get('inCollection')
                    
                    if uuid and href:
                        # Check if the mapping has changed before updating
                        existing_entry = self.mapping.get_entry(ods_id)
                        if existing_entry and len(existing_entry) > 2 and existing_entry[0] == uuid and existing_entry[1] == href and existing_entry[2] == inCollection_key:
                            logging.info(f"No changes in dataset {ods_id}. Skipping")
                            logging.debug(f"    - uuid: {uuid}, href: {href}, inCollection: {inCollection_key}")
                        elif existing_entry:
                            old_uuid = existing_entry[0] if len(existing_entry) > 0 else None
                            old_href = existing_entry[1] if len(existing_entry) > 1 else None
                            old_inCollection = existing_entry[2] if len(existing_entry) > 2 else None
                            
                            # Only log UUID update warning if the UUID actually changed
                            if old_uuid != uuid:
                                logging.warning(f"Update dataset {ods_id} uuid from {old_uuid} to {uuid}")
                            else:
                                logging.info(f"Updating dataset {ods_id} metadata")
                            
                            # Log a more meaningful message if inCollection has changed
                            if old_inCollection != inCollection_key and old_inCollection and inCollection_key:
                                logging.info(f"Dataset {ods_id} has been moved from '{old_inCollection}' to '{inCollection_key}'")
                            elif not old_inCollection and inCollection_key:
                                logging.info(f"Dataset {ods_id} has been placed in '{inCollection_key}'")
                            elif old_inCollection and not inCollection_key:
                                logging.info(f"Dataset {ods_id} has been removed from '{old_inCollection}'")
                            
                            logging.debug(f"    - old_uuid: {old_uuid}, old_href: {old_href}, old_inCollection: {old_inCollection}")
                            logging.debug(f"    - new_uuid: {uuid}, new_href: {href}, new_inCollection: {inCollection_key}")
                            self.mapping.add_entry(ods_id, uuid, href, inCollection_key)
                            updated_count += 1
                        else:
                            logging.info(f"Add dataset {ods_id} with uuid {uuid}")
                            logging.debug(f"    - uuid: {uuid}, href: {href}, inCollection: {inCollection_key}")
                            self.mapping.add_entry(ods_id, uuid, href, inCollection_key)
                            updated_count += 1
                    else:
                        logging.warning(f"Missing UUID or href for dataset with ODS ID: {ods_id}")
            
            logging.info(f"Updated mappings for {updated_count} datasets. Did not update mappings for the other {len(datasets) - updated_count} datasets.")
            return updated_count
            
        except Exception as e:
            logging.error(f"Error processing download response: {str(e)}")
            raise

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
            uuid, href = existing_entry[0], existing_entry[1]
            logging.info(f"Dataset with ODS ID {ods_id} already exists (UUID: {uuid}). Use update_dataset method to update.")
            raise ValueError(f"Dataset with ODS ID {ods_id} already exists. Use update_dataset or create_or_update_dataset method.")
        
        # Read the dataset title
        title = dataset.to_json()['label']
        logging.info(f"Creating dataset: '{title}' with ODS ID: {ods_id}")
        
        # Ensure ODS-Imports collection exists
        logging.debug(f"Ensuring ODS-Imports collection exists")
        collection_data = self.ensure_ods_imports_collection_exists()
        logging.debug(f"Collection info: {collection_data}")

        # Get the collection UUID and href
        collection_uuid, collection_href = get_uuid_and_href_from_response(collection_data)

        if not collection_uuid or not collection_href:
            logging.error("Failed to get collection UUID or href")
            raise ValueError("Could not retrieve collection information required for dataset creation")

        logging.debug(f"Using collection UUID: {collection_uuid} and href: {collection_href}")
        
        # Create a new dataset
        dataset_creation_endpoint = url_join(collection_href, "datasets")
        
        response = self.create_resource(
            endpoint=dataset_creation_endpoint,
            data=dataset.to_json(),
            _type='Dataset'
        )
        
        # Store the mapping for future reference
        if ods_id:
            uuid, href = get_uuid_and_href_from_response(response)
            if uuid and href:
                # For newly created datasets, store the ODS-Imports collection name as the business key
                logging.debug(f"Adding mapping entry for ODS ID {ods_id} with UUID {uuid}, href {href}, and inCollection '{self.ods_imports_collection_name}'")
                self.mapping.add_entry(ods_id, uuid, href, self.ods_imports_collection_name)
            else:
                logging.warning(f"Could not extract UUID and href from response for dataset '{title}'")
        
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
            collection_uuid, collection_href = get_uuid_and_href_from_response(collection_data)

            if not collection_uuid or not collection_href:
                error_msg = "Failed to get collection UUID or href"
                logging.error(error_msg)
                raise ValueError(error_msg)

            logging.debug(f"Using collection UUID: {collection_uuid} and href: {collection_href}")
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
                    # Default to ODS-Imports collection
                    logging.debug(f"No inCollection found in mapping for ODS ID: {ods_id}, defaulting to {self.ods_imports_collection_name}")
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
                _type='Dataset',
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
        Updates the mapping between ODS IDs and Dataspot UUIDs/hrefs after uploading datasets.
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
            dataset_json['inCollection'] = self.ods_imports_collection_name
            logging.debug(f"No inCollection found in mapping, defaulting to '{self.ods_imports_collection_name}'")
        
        # Update the existing dataset
        logging.debug(f"Update method: {'PUT (replace)' if force_replace else 'PATCH (partial update)'}")
        response = self.update_resource(
            endpoint=href,
            data=dataset_json,
            replace=force_replace,
            _type='Dataset'
        )
        
        # Ensure the mapping is updated
        if ods_id:
            uuid, href = get_uuid_and_href_from_response(response)
            if uuid and href:
                logging.debug(f"Updating mapping for ODS ID {ods_id} with UUID {uuid}, href {href}, inCollection {inCollection}")
                self.mapping.add_entry(ods_id, uuid, href, inCollection)
            else:
                logging.warning(f"Could not extract UUID and href from response for dataset '{title}'")
        
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
            _, href = entry
            logging.debug(f"Found existing dataset in mapping with href: {href}")
            
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
                raise ValueError(f"Dataset with ODS ID '{ods_id}' does not exist")
            return False
        
        # Extract UUID and href from the mapping
        _, href = entry
        
        # Delete the dataset
        logging.info(f"Deleting dataset with ODS ID '{ods_id}' at {href}")
        self.delete_resource(href)
        
        # Remove entry from mapping
        self.mapping.remove_entry(ods_id)
        
        return True
    
    # TODO: Do NOT implement me
    def build_organization_hierarchy_from_ods(self, org_data: Dict[str, Any]):
        """
        Build organization hierarchy in the DNK based on organization data from ODS API.
        
        Args:
            org_data (Dict[str, Any]): Dictionary containing organization data from ODS API
            
        Raises:
            ValueError: If organization data is missing or invalid
        """
        
        if not org_data or 'results' not in org_data:
            raise ValueError("Invalid organization data format")
            
        # Build a lookup dictionary for quick access to organization by ID
        org_lookup = {org['id']: org for org in org_data['results']}
        
        # Keep track of processed organizations to avoid duplicates
        processed_orgs = set()
        failed_orgs = set()
        
        # Group organizations by their path depth
        depth_groups = {}
        for org_id, org in org_lookup.items():
            title_full = org.get('title_full', '')
            if not title_full:
                continue
                
            # Determine path depth (number of segments in title_full)
            path_depth = len(title_full.split('/'))
            if path_depth not in depth_groups:
                depth_groups[path_depth] = []
                
            depth_groups[path_depth].append(org_id)
        
        # Calculate total number of organizations to process
        total_orgs = sum(len(depth_groups[depth]) for depth in depth_groups)
        current_org = 0
        
        # Process organizations level by level (starting from the shallowest)
        for depth in sorted(depth_groups.keys()):
            logging.info(f"Processing organizations at depth level {depth}")
            
            # Use a queue for processing organizations at this depth level
            from collections import deque
            queue = deque(depth_groups[depth])
            
            while queue:
                org_id = queue.popleft()
                
                # Skip if already processed or failed before
                if org_id in processed_orgs or org_id in failed_orgs:
                    continue
                
                # Skip if not found in lookup
                if org_id not in org_lookup:
                    logging.warning(f"Organization with ID {org_id} not found in data, skipping")
                    failed_orgs.add(org_id)
                    continue
                
                org = org_lookup[org_id]

                # Extract organization details
                title = org.get('title')
                title_full = org.get('title_full', '')
                url_website = generate_potential_staatskalender_url(title_full)

                # Increment the counter
                current_org += 1

                # Check if the generated url is valid
                try:
                    response = requests_get(url_website)
                    if not response.status_code == 200:
                        logging.warning(f"Invalid URL for organization '{title}': {url_website}.")
                        url_website = org.get('url_website', '')
                        logging.info(f"Invalid URL for organization '{title}', falling back to {url_website}")
                except (RequestException, ConnectionError, Timeout) as e:
                    logging.warning(f"Invalid URL for organization '{title}': {url_website}")

                    # Note: We don't check whether the fallback url is valid, because we assume it is.
                    url_website = org.get('url_website', '')
                    logging.info(f"Invalid URL for organization '{title}', falling back to {url_website}")

                # Create custom properties for the organization
                custom_properties = {
                    "ID": org_id,
                    "Link_zum_Staatskalender": url_website
                }
                
                # Skip if organization is missing critical data
                if not title or not title_full:
                    logging.warning(f"Organization with ID {org_id} has missing data, skipping")
                    failed_orgs.add(org_id)
                    continue


                # Extract the path components from title_full
                path_components = title_full.split('/')

                logging.info(f"[{current_org}/{total_orgs}] Processing organization: {title} (ID: {org_id}, Path: {title_full})")

                try:
                    # TODO: Implement this (also don't forget to "Mark as processed" by processed_orgs.add(org_id)
                    # TODO: Actually build the structure based on path_components. Create Collections of stereotype "Organisationseinheiten" only.
                    pass


                    
                except Exception as e:
                    logging.error(f"Error processing organization {org_id}: {str(e)}")
                    failed_orgs.add(org_id)
        
        # Log summary statistics
        logging.info(f"Processed {len(processed_orgs)} organizations successfully")
        if failed_orgs:
            logging.warning(f"Failed to process {len(failed_orgs)} organizations")

    def require_scheme_exists(self) -> str:
        """
        Assert that the DNK scheme exists and return its href. Throw an error if it doesn't.

        Returns:
            str: The href of the DNK scheme (starting with /rest/...)

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
        Creates the collection if it does not exist, but requires the scheme to already exist.

        Returns:
            dict: The JSON response containing information about the ODS-Imports collection
            
        Raises:
            ValueError: If the DNK scheme does not exist
            HTTPError: If API requests fail
        """

        # Assert that the DNK scheme exists.
        self.require_scheme_exists()

        # Check if ODS-Imports collection exists and create if needed
        ods_imports_path = url_join('rest', self.database_name, 'schemes', self.scheme_name, 'collections',
                                    self.ods_imports_collection_name)

        # Check if ODS-Imports collection exists
        collection_response = self.get_resource_if_exists(ods_imports_path)
        if collection_response:
            logging.debug(f"ODS-Imports collection exists")
            return collection_response

        # ODS-Imports doesn't exist, create it
        logging.info("ODS-Imports collection doesn't exist, creating it")
        collections_endpoint = url_join('rest', self.database_name, 'schemes', self.scheme_name, 'collections')
        collection_data = {
            "label": self.ods_imports_collection_name
        }
        try:
            response_json = self.create_resource(endpoint=collections_endpoint, data=collection_data, _type="Collection")
            logging.info(f"Created ODS-Imports collection: {self.ods_imports_collection_name}")
            return response_json
        except HTTPError as create_error:
            logging.error(f"Failed to create ODS-Imports collection: {str(create_error)}")
            raise
