import logging
from typing import List, Dict, Any, Optional, Tuple

from src import config
from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping
from src.clients.helpers import url_join, get_uuid_from_response


class BaseDataspotHandler:
    """
    Base class for Dataspot handlers that manage different types of assets.
    This class provides common functionality for dataset and organizational unit handlers.
    """
    # mapping: BaseDataspotMapping # TODO: Probably use this, and remove the init method.
    
    # TODO (large language model): Remove the mapping parameter and use the mapping attribute instead. Implement the entire init in the subclasses.
    def __init__(self, client: BaseDataspotClient, mapping: BaseDataspotMapping):
        """
        Initialize the base handler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
            mapping: BaseDataspotMapping instance to use for ID-to-UUID mappings
        """
        self.client = client
        self.mapping = mapping
        self.logger = logging.getLogger(__name__)
        
        # Load common properties from client
        self.database_name = client.database_name
        self.scheme_name = client.scheme_name
        self.scheme_name_short = client.scheme_name_short
    
    # TODO (Renato): Is it possible to implement this here?
    def _download_and_update_mappings(self, target_ids: List[str] = None) -> int:
        """
        Base method for downloading assets and updating mappings.
        
        Args:
            target_ids: If provided, only update mappings for these IDs
            
        Returns:
            int: Number of mappings successfully updated
            
        Note:
            This method should be overridden by subclasses to implement
            asset-specific download and mapping updates.
        """
        raise NotImplementedError("Subclasses must implement _download_and_update_mappings")
    
    def update_mappings_from_upload(self, ids: List[str]) -> None:
        """
        Updates the mapping between external IDs and Dataspot UUIDs after uploading assets.
        
        Args:
            ids: List of external IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve asset information
        """
        self.logger.info(f"Updating mappings for {len(ids)} assets using download API")
        
        try:
            updated_count = self._download_and_update_mappings(ids)
            self.logger.info(f"Updated mappings for {updated_count} out of {len(ids)} assets")
            
            if updated_count < len(ids):
                missing_ids = [id_value for id_value in ids if not self.mapping.get_entry(id_value)]
                if missing_ids:
                    self.logger.warning(f"Could not find mappings for {len(missing_ids)} IDs: {missing_ids[:5]}" + 
                                      (f"... and {len(missing_ids)-5} more" if len(missing_ids) > 5 else ""))
        except Exception as e:
            self.logger.error(f"Error updating mappings: {str(e)}")
            raise
    
    def bulk_create_or_update_assets(self, assets: List[Dict[str, Any]], 
                                     operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple assets in bulk in Dataspot.
        
        Args:
            assets: List of asset data to upload
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If no assets are provided
            HTTPError: If API requests fail
        """
        # Verify we have assets to process
        if not assets:
            self.logger.warning("No assets provided for bulk upload")
            return {"status": "error", "message": "No assets provided"}
        
        # Count of assets
        num_assets = len(assets)
        self.logger.info(f"Bulk creating {num_assets} assets (operation: {operation}, dry_run: {dry_run})...")
        
        # Bulk create assets using the scheme name
        try:
            response = self.client.bulk_create_or_update_assets(
                scheme_name=self.scheme_name,
                data=assets,
                operation=operation,
                dry_run=dry_run
            )

            self.logger.info(f"Bulk creation complete")
            return response
            
        except Exception as e:
            self.logger.error(f"Unexpected error during bulk upload: {str(e)}")
            raise
    
    def get_all_external_ids(self) -> List[str]:
        """
        Get a list of all external IDs in the mapping.
        
        Returns:
            List[str]: A list of all external IDs
        """
        return self.mapping.get_all_ids() 