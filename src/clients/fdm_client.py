from typing import Dict, Any

from src import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler

class FDMClient(BaseDataspotClient):
    """Client for interacting with your new data scheme."""
    
    def __init__(self):
        """
        Initialize the new client.
        """
        super().__init__(base_url=config.base_url,
                         database_name=config.database_name,
                         scheme_name=config.fdm_scheme_name,
                         scheme_name_short=config.fdm_scheme_name_short,
                         ods_imports_collection_name=config.ods_imports_collection_name,
                         ods_imports_collection_path=config.ods_imports_collection_path)
        
        # Initialize the handlers
        self.org_handler = OrgStructureHandler(self)

    # Synchronization methods
    def sync_org_units(self, org_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot.
        
        Args:
            org_data: Dictionary containing organization data
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self.org_handler.sync_org_units(org_data)
