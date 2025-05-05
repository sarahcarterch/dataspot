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
                         ods_imports_collection_name=config.ods_imports_collection_name)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)
        self.dataset_handler = DatasetHandler(self)
        
        # Provide direct access to mappings if needed
        self.org_mapping = self.org_handler.mapping
        self.ods_dataset_mapping = self.dataset_handler.mapping

    # Organization structure methods
    def sync_staatskalender_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            validate_urls: Whether to validate Staatskalender URLs
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.org_handler.sync_staatskalender_org_units(org_data, validate_urls)
    
    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any], validate_urls: bool = False) -> dict:
        """
        Build organization hierarchy in the DNK based on data from ODS API using bulk upload.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            validate_urls: Whether to validate Staatskalender URLs
            
        Returns:
            dict: The response from the final bulk upload with status information
        """
        return self.org_handler.build_organization_hierarchy_from_ods_bulk(org_data, validate_urls)

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
        return self.org_handler.bulk_create_or_update_organizational_units(organizational_units, operation, dry_run)

    def get_all_staatskalender_ids(self) -> List[str]:
        """
        Get a list of all Staatskalender IDs in the mapping.
        
        Returns:
            List[str]: A list of all Staatskalender IDs
        """
        return self.org_handler.get_all_staatskalender_ids()

    # Dataset methods
    def update_dataset_mappings(self, target_ods_ids: List[str] = None) -> int:
        """
        Download datasets and update ODS ID to Dataspot UUID mappings.
        
        Args:
            target_ods_ids: If provided, only update mappings for these ODS IDs
            
        Returns:
            int: Number of mappings successfully updated
        """
        return self.dataset_handler._download_and_update_mappings(target_ods_ids)
    
    def create_dataset(self, dataset: Dataset) -> dict:
        """
        Create a new dataset in the 'Datennutzungskatalog/ODS-Imports' in Dataspot.
        
        Args:
            dataset: The dataset instance to be uploaded
            
        Returns:
            dict: The JSON response containing the dataset data
        """
        return self.dataset_handler.create_dataset(dataset)
    
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
        return self.dataset_handler.bulk_create_or_update_datasets(datasets, operation, dry_run)
    
    def update_dataset_mappings_from_upload(self, ods_ids: List[str]) -> None:
        """
        Update the mapping between ODS IDs and Dataspot UUIDs after uploading datasets.
        
        Args:
            ods_ids: List of ODS IDs to update in the mapping
        """
        return self.dataset_handler.update_mappings_from_upload(ods_ids)
    
    def update_dataset(self, dataset: Dataset, href: str, force_replace: bool = False) -> dict:
        """
        Update an existing dataset in the DNK.
        
        Args:
            dataset: The dataset instance with updated data
            href: The href of the dataset to update
            force_replace: Whether to completely replace the dataset
            
        Returns:
            dict: The JSON response containing the updated dataset data
        """
        return self.dataset_handler.update_dataset(dataset, href, force_replace)
    
    def create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update',
                                force_replace: bool = False) -> dict:
        """
        Create a new dataset or update an existing dataset in Dataspot.
        
        Args:
            dataset: The dataset instance to be uploaded
            update_strategy: Strategy for handling dataset existence
            force_replace: Whether to completely replace an existing dataset
            
        Returns:
            dict: The JSON response containing the dataset data
        """
        return self.dataset_handler.create_or_update_dataset(dataset, update_strategy, force_replace)
    
    def delete_dataset(self, ods_id: str, fail_if_not_exists: bool = False) -> bool:
        """
        Delete a dataset from the DNK.
        
        Args:
            ods_id: The ODS ID of the dataset to delete
            fail_if_not_exists: Whether to raise an error if the dataset doesn't exist
            
        Returns:
            bool: True if the dataset was deleted, False if it didn't exist
        """
        return self.dataset_handler.delete_dataset(ods_id, fail_if_not_exists)
    
    def get_all_ods_ids(self) -> List[str]:
        """
        Get a list of all ODS IDs in the mapping.
        
        Returns:
            List[str]: A list of all ODS IDs
        """
        return self.dataset_handler.get_all_ods_ids()
