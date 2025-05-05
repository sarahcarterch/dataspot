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
        self.org_mapping = self.org_handler.org_mapping
        self.ods_dataset_mapping = self.dataset_handler.ods_dataset_mapping

    # TODO (Renato): Clean these up
    # Organization methods - delegate to org_handler
    def sync_staatskalender_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """Delegate to org_handler"""
        return self.org_handler.sync_staatskalender_org_units(org_data, validate_urls)
    
    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any], validate_urls: bool = False) -> dict:
        """Delegate to org_handler"""
        return self.org_handler.build_organization_hierarchy_from_ods_bulk(org_data, validate_urls)

    def bulk_create_or_update_organizational_units(self, organizational_units: List[Dict[str, Any]], 
                                                 operation: str = "ADD", dry_run: bool = False) -> dict:
        """Delegate to org_handler"""
        return self.org_handler.bulk_create_or_update_organizational_units(organizational_units, operation, dry_run)

    # TODO (Renato): Clean these up
    # Dataset methods - delegate to dataset_handler
    def _download_and_update_mappings(self, target_ods_ids: List[str] = None) -> int:
        """Delegate to dataset_handler"""
        return self.dataset_handler._download_and_update_mappings(target_ods_ids)
    
    def create_dataset(self, dataset: Dataset) -> dict:
        """Delegate to dataset_handler"""
        return self.dataset_handler.create_dataset(dataset)
    
    def bulk_create_or_update_datasets(self, datasets: List[Dataset],
                                      operation: str = "ADD", dry_run: bool = False) -> dict:
        """Delegate to dataset_handler"""
        return self.dataset_handler.bulk_create_or_update_datasets(datasets, operation, dry_run)
    
    def update_mappings_from_upload(self, ods_ids: List[str]) -> None:
        """Delegate to dataset_handler"""
        return self.dataset_handler.update_mappings_from_upload(ods_ids)
    
    def update_dataset(self, dataset: Dataset, href: str, force_replace: bool = False) -> dict:
        """Delegate to dataset_handler"""
        return self.dataset_handler.update_dataset(dataset, href, force_replace)
    
    def create_or_update_dataset(self, dataset: Dataset, update_strategy: str = 'create_or_update',
                                force_replace: bool = False) -> dict:
        """Delegate to dataset_handler"""
        return self.dataset_handler.create_or_update_dataset(dataset, update_strategy, force_replace)
    
    def delete_dataset(self, ods_id: str, fail_if_not_exists: bool = False) -> bool:
        """Delegate to dataset_handler"""
        return self.dataset_handler.delete_dataset(ods_id, fail_if_not_exists)
