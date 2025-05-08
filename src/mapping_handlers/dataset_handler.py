import logging
from typing import List, Dict, Any

from requests import HTTPError

from src import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, get_uuid_from_response
from src.dataspot_dataset import Dataset
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping


class DatasetMapping(BaseDataspotMapping):
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


class DatasetHandler(BaseDataspotHandler):
    """
    Handler for dataset synchronization operations in Dataspot.
    Provides methods to sync datasets between ODS and Dataspot.
    """
    # Set configuration values for the base handler
    asset_id_field = 'ODS_ID'
    
    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the DatasetHandler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        # Call parent's __init__ method first
        super().__init__(client)
        
        # Initialize the dataset mapping
        self.mapping = DatasetMapping(database_name=client.database_name, scheme=client.scheme_name_short)
        
        # Store ODS imports collection name
        self.ods_imports_collection_name = client.ods_imports_collection_name
        
        # Set the asset type filter based on asset_id_field
        self.asset_type_filter = lambda asset: asset.get(self.asset_id_field) is not None

    def sync_datasets(self, datasets: List[Dataset]) -> Dict[str, Any]:
        """
        Synchronize datasets between ODS and Dataspot.
        This is the main public method for dataset synchronization.
        
        The method:
        1. Updates mappings before upload
        2. Uploads datasets using bulk_create_or_update_datasets
        3. Updates mappings after upload
        4. Saves mappings to CSV
        
        Args:
            datasets: List of Dataset objects to synchronize with Dataspot
            
        Returns:
            Dict[str, Any]: Summary of the synchronization process
        """
        if not datasets:
            logging.warning("No datasets provided for synchronization")
            return {
                "status": "error",
                "message": "No datasets provided for synchronization",
                "datasets_processed": 0
            }
        
        logging.info(f"Starting synchronization of {len(datasets)} datasets...")
        
        # Step 1: Update mappings before upload
        logging.info("Step 1: Updating mappings before upload...")
        try:
            self.update_mappings_before_upload()
        except Exception as e:
            logging.warning(f"Failed to update mappings before upload: {str(e)}")
            
        # Step 2: Extract ODS IDs for later mapping updates
        ods_ids = []
        for dataset in datasets:
            dataset_json = dataset.to_json()
            ods_id = dataset_json.get('customProperties', {}).get('ODS_ID')
            if ods_id:
                ods_ids.append(ods_id)
        
        # Step 3: Upload datasets using bulk_create_or_update_datasets
        logging.info(f"Step 3: Uploading {len(datasets)} datasets...")
        upload_result = self.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="ADD",
            dry_run=False
        )
        
        # Step 4: Update mappings after upload
        logging.info("Step 4: Updating mappings after upload...")
        if ods_ids:
            try:
                self.update_mappings_after_upload(ods_ids)
            except Exception as e:
                logging.error(f"Error updating mappings after upload: {str(e)}")
        
        # Step 5: Save mappings to CSV
        logging.info("Step 5: Saving mappings to CSV...")
        self.mapping.save_to_csv()
        
        # Generate result summary
        result = {
            "status": "success",
            "message": f"Successfully synchronized {len(datasets)} datasets",
            "datasets_processed": len(datasets),
            "upload_result": upload_result
        }
        
        logging.info(f"Dataset synchronization completed successfully")
        return result

    def update_mappings_after_upload(self, ods_ids: List[str]) -> None:
        """
        Updates the mapping between ODS IDs and Dataspot UUIDs after uploading datasets.
        Uses the download API to retrieve all datasets and then updates the mapping for matching ODS IDs.
        
        Args:
            ods_ids (List[str]): List of ODS IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve dataset information
        """
        # Call the base class method with our specific ID type
        super().update_mappings_after_upload(ods_ids)

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
            collection_data = self.client.ensure_ods_imports_collection_exists()
            
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
                    if self.client.ods_imports_collection_path:
                        # Build the full business key path for inCollection
                        # If path is ['A', 'B', 'C'], then inCollection should be "A/B/C/ODS-Imports"
                        full_path = f"{'/'.join(self.client.ods_imports_collection_path)}/{self.client.ods_imports_collection_name}"
                        
                        # Check for slashes in the path components which would require escaping
                        has_special_chars = any('/' in folder for folder in self.client.ods_imports_collection_path)

                        if has_special_chars:
                            # We need to escape the whole path with quotes since it contains slashes
                            from src.clients.helpers import escape_special_chars
                            full_path = escape_special_chars(full_path)  # FIXME: This is very likely not correct
                            
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
            response = self.client.bulk_create_or_update_assets(
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
                    self.update_mappings_after_upload(ods_ids)
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
        collection_data = self.client.ensure_ods_imports_collection_exists()
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
        collection_path = self.client.ods_imports_collection_path
        
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
        
        response = self.client._create_asset(
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
            collection_path = self.client.ods_imports_collection_path
            
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
        response = self.client._update_asset(
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
            asset_data = self.client._get_asset(href)
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
        self.client._delete_asset(href)
        
        # Remove entry from mapping
        self.mapping.remove_entry(ods_id)
        
        return True 