import logging
from typing import List, Dict, Any

from requests import HTTPError

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

        # Set the asset type filter based on asset_id_field
        self.asset_type_filter = lambda asset: asset.get(self.asset_id_field) is not None

        # Check for special characters in the default path and name
        if any('/' in folder for folder in self.client.ods_imports_collection_path) \
            or any('.' in folder for folder in self.client.ods_imports_collection_path) \
            or ('/' in self.client.ods_imports_collection_name) \
            or '.' in self.client.ods_imports_collection_name:
            # TODO: Implement escaping of special characters in the default dataset path
            logging.error("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")
            raise ValueError("The default path or name in config.py contains special characters ('/' or '.') that need escaping. This functionality is not yet supported and needs to be properly implemented as needed.")

        if self.client.ods_imports_collection_path:
            self.default_dataset_path_full = url_join(*self.client.ods_imports_collection_path, self.client.ods_imports_collection_name)
        else:
            self.default_dataset_path_full = self.client.ods_imports_collection_name

        logging.debug(f"Default dataset path: {self.default_dataset_path_full}")

    def sync_datasets(self, datasets: List[Dataset]) -> Dict[str, Any]:
        """
        Synchronize datasets between ODS and Dataspot.
        This is the main public method for dataset synchronization.
        
        The method:
        1. Updates mappings before upload
        2. Separates datasets into new and existing based on ODS_ID
        3. Updates existing datasets individually to preserve other fields
        4. Creates new datasets using bulk upload
        5. Updates mappings after all operations
        6. Saves mappings to CSV

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
        self.update_mappings_before_upload()

        # Step 2: Extract ODS IDs and separate datasets into new and existing
        logging.info("Step 2: Separating datasets into new and existing...")
        new_datasets = []
        existing_datasets = []
        ods_ids = []
        
        for dataset in datasets:
            dataset_json = dataset.to_json()
            ods_id = dataset_json.get('customProperties', {}).get('ODS_ID')
            if ods_id:
                ods_ids.append(ods_id)
                
                # Check if dataset exists in mapping
                existing_entry = self.mapping.get_entry(ods_id)
                if existing_entry:
                    existing_datasets.append((dataset, ods_id, existing_entry))
                else:
                    new_datasets.append(dataset)

        # Step 3: Process existing datasets with individual updates to preserve other fields
        updated_count = 0
        update_errors = 0
        
        logging.info(f"Step 3: Updating {len(existing_datasets)} existing datasets individually...")
        if existing_datasets:
            for dataset, ods_id, entry in existing_datasets:
                try:
                    # entry format is (_type, uuid, inCollection)
                    uuid = entry[1]
                    
                    # Get the endpoint for this dataset
                    endpoint = f"/rest/{self.client.database_name}/datasets/{uuid}"
                    
                    # Update only specified fields using PATCH to preserve other data
                    dataset_json = dataset.to_json()

                    # Ensure inCollection is preserved from the mapping
                    retrieved_asset = self.client._get_asset(endpoint=endpoint)
                    dataset_json['inCollection'] = retrieved_asset['inCollection']

                    # Use the client's update method with replace=False to do a PATCH
                    try:
                        self.client._update_asset(
                            endpoint=endpoint, 
                            data=dataset_json, 
                            replace=False
                        )
                        updated_count += 1
                        logging.info(f"Successfully updated dataset with ODS_ID {ods_id}")
                    except Exception as e:
                        logging.error(f"Error updating dataset with ODS_ID {ods_id}: {str(e)}")
                        update_errors += 1
                
                except Exception as e:
                    logging.error(f"Error processing update for dataset with ODS_ID {ods_id}: {str(e)}")
                    update_errors += 1

        # Step 4: Upload new datasets using bulk_create_or_update_datasets
        created_count = 0
        bulk_result = {}
        
        logging.info(f"Step 4: Creating {len(new_datasets)} new datasets with bulk upload...")
        if new_datasets:
            bulk_result = self.bulk_create_or_update_datasets(
                datasets=new_datasets,
                operation="ADD",
                dry_run=False
            )
            created_count = len(new_datasets)

        # Step 5: Update mappings after all operations
        logging.info("Step 5: Updating mappings after upload...")
        if ods_ids:
            self.update_mappings_after_upload(ods_ids)

        # Step 6: Save mappings to CSV
        logging.info("Step 6: Saving mappings to CSV...")
        self.mapping.save_to_csv()

        # Generate result summary
        result = {
            "status": "success",
            "message": f"Synchronized {len(datasets)} datasets: {updated_count} updated, {created_count} created, {update_errors} errors",
            "datasets_processed": len(datasets),
            "updated": updated_count,
            "created": created_count,
            "errors": update_errors,
            "bulk_upload_result": bulk_result if new_datasets else {}
        }

        logging.info(f"Dataset synchronization completed: {updated_count} updated, {created_count} created, {update_errors} errors")
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

        # Ensure ODS-Imports collection exists and get its UUID
        logging.debug("Ensuring ODS-Imports collection exists")
        collection_data = self.client.ensure_ods_imports_collection_exists()
        
        # Get the collection UUID
        collection_uuid = get_uuid_from_response(collection_data)

        if not collection_uuid:
            error_msg = "Failed to get collection UUID"
            logging.error(error_msg)
            raise ValueError(error_msg)

        logging.debug(f"Using collection UUID: {collection_uuid}")
        
        # Validate and transform datasets
        dataset_jsons = []
        ods_ids = []
        validation_errors = []
        
        for i, dataset in enumerate(datasets):
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
                # Use the centralized default dataset path
                logging.debug(f"Using default dataset path: '{self.default_dataset_path_full}' for dataset with ODS ID: {ods_id}")
                dataset_json['inCollection'] = self.default_dataset_path_full
            
            dataset_jsons.append(dataset_json)
        
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
        response = self.client.bulk_create_or_update_assets(
            scheme_name=self.scheme_name,
            data=dataset_jsons,
            operation=operation,
            dry_run=dry_run
        )

        logging.info(f"Bulk creation complete")

        # Update mapping for each dataset (only for non-dry runs)
        if not dry_run:
            # After bulk upload, retrieve the datasets and update mapping
            self.update_mappings_after_upload(ods_ids)
        
        logging.info(f"Bulk dataset creation completed successfully")
        return response

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
            # Use the centralized default dataset path
            logging.debug(f"Using default dataset path: '{self.default_dataset_path_full}'")
            dataset_json['inCollection'] = self.default_dataset_path_full
        else:
            # No path, just use the collection name directly
            logging.debug(f"Using default inCollection: '{self.client.ods_imports_collection_name}'")
            dataset_json['inCollection'] = self.client.ods_imports_collection_name
        
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
                logging.debug(f"Adding mapping entry for ODS ID {ods_id} with Type 'Dataset', UUID {uuid}, and inCollection '{self.client.ods_imports_collection_name}'")
                self.mapping.add_entry(ods_id, "Dataset", uuid, self.client.ods_imports_collection_name)
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
                # Use the centralized default dataset path
                logging.debug(f"Using default dataset path: '{self.default_dataset_path_full}'")
                dataset_json['inCollection'] = self.default_dataset_path_full
            else:
                # No path, just use the collection name directly
                logging.debug(f"Using default inCollection: '{self.client.ods_imports_collection_name}'")
                dataset_json['inCollection'] = self.client.ods_imports_collection_name
        
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
        Delete a dataset from the DNK or mark it for deletion review.
        
        Args:
            ods_id (str): The ODS ID of the dataset to delete
            fail_if_not_exists (bool): Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted or marked for deletion, or if it didn't exist but tracking was updated.
                 False if it didn't exist in the mapping and fail_if_not_exists is False.
            
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
        
        # Check if the dataset still exists in Dataspot
        asset_exists = self.client._get_asset(href) is not None
        
        if asset_exists:
            # Dataset exists, mark it for deletion
            logging.info(f"Marking dataset with ODS ID '{ods_id}' (UUID: {uuid}) for deletion review at {href}")
            self.client._mark_asset_for_deletion(href)
        else:
            # Dataset already deleted in Dataspot, just log it
            logging.info(f"Dataset with ODS ID '{ods_id}' (UUID: {uuid}) already deleted in Dataspot, updating local mapping only")
        
        # Remove entry from mapping in both cases
        self.mapping.remove_entry(ods_id)
        
        return True
