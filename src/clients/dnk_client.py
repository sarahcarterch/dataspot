from typing import Dict, Any, List

from src import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.dataset_handler import DatasetHandler
from src.dataspot_dataset import Dataset

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
                         ods_imports_collection_name=config.ods_imports_collection_name,
                         ods_imports_collection_path=config.ods_imports_collection_path)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.dataset_handler = DatasetHandler(self)

    # Direct API operations for datasets
    def create_dataset(self, dataset: Dataset) -> dict:
        """
        Create a new dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        
        Args:
            dataset: The dataset instance to be uploaded
            
        Returns:
            dict: The JSON response containing the dataset data
        """
        # Ensure ODS-Imports collection exists
        collection_data = self.ensure_ods_imports_collection_exists()
        
        # Create dataset endpoint
        collection_uuid = collection_data.get('id')
        if not collection_uuid:
            raise ValueError("Failed to get collection UUID")
            
        # Prepare dataset for upload with proper inCollection value
        dataset_json = dataset.to_json()
        # TODO: Inspect this, it should potentially be: dataset_json['inCollection'] = collection_data.get('label')
        dataset_json['inCollection'] = self.ods_imports_collection_name
        
        # Create the dataset directly
        endpoint = f"/rest/{self.database_name}/datasets/{collection_uuid}/datasets"
        return self._create_asset(endpoint=endpoint, data=dataset_json)
    
    def update_dataset(self, dataset: Dataset, uuid: str, force_replace: bool = False) -> dict:
        """
        Update an existing dataset in the DNK.
        
        Args:
            dataset: The dataset instance with updated data
            uuid: The UUID of the dataset to update
            force_replace: Whether to completely replace the dataset
            
        Returns:
            dict: The JSON response containing the updated dataset data
        """
        endpoint = f"/rest/{self.database_name}/datasets/{uuid}"
        return self._update_asset(endpoint=endpoint, data=dataset.to_json(), replace=force_replace)
    
    def delete_dataset(self, uuid: str) -> bool:
        """
        Delete a dataset from the DNK.
        
        Args:
            uuid: The UUID of the dataset to delete
            
        Returns:
            bool: True if the dataset was deleted
        """
        endpoint = f"/rest/{self.database_name}/datasets/{uuid}"
        self._delete_asset(endpoint)
        return True
    
    def bulk_create_or_update_datasets(self, datasets: List[Dataset],
                                      operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple datasets in bulk in Dataspot.
        
        Args:
            datasets: List of dataset instances to be uploaded
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            
        Returns:
            dict: The JSON response containing the upload results
        """
        # Create full path to the ODS-Imports collection
        full_collection_path = f"{'/'.join(self.ods_imports_collection_path)}/{self.ods_imports_collection_name}"
        
        dataset_jsons = [dataset.to_json() for dataset in datasets]
        
        # Set inCollection for each dataset using the full path
        for dataset_json in dataset_jsons:
            dataset_json['inCollection'] = full_collection_path
            
        return self.bulk_create_or_update_assets(
            scheme_name=self.scheme_name,
            data=dataset_jsons,
            operation=operation,
            dry_run=dry_run
        )
    
    # Direct API operations for org units
    def bulk_create_or_update_organizational_units(self, organizational_units: List[Dict[str, Any]], 
                                                operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple organizational units in bulk in Dataspot.
        
        Args:
            organizational_units: List of organizational unit data to upload
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            
        Returns:
            dict: The JSON response containing the upload results
        """
        return self.bulk_create_or_update_assets(
            scheme_name=self.scheme_name,
            data=organizational_units,
            operation=operation,
            dry_run=dry_run
        )
    
    # Synchronization methods delegated to handlers
    def sync_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            validate_urls: Whether to validate Staatskalender URLs
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.org_handler.sync_org_units(org_data, validate_urls)
    
    def sync_datasets(self, datasets: List[Dataset]) -> Dict[str, Any]:
        """
        Synchronize datasets between ODS and Dataspot.
        
        Args:
            datasets: List of Dataset objects to synchronize
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.dataset_handler.sync_datasets(datasets)
