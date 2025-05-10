import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.base_dataspot_mapping import BaseDataspotMapping
from src.mapping_handlers.org_structure_helpers.org_structure_transformer import OrgStructureTransformer
from src.mapping_handlers.org_structure_helpers.org_structure_comparer import OrgStructureComparer
from src.mapping_handlers.org_structure_helpers.org_structure_updater import OrgStructureUpdater


class OrgStructureMapping(BaseDataspotMapping):
    """
    A lookup table that maps Staatskalender IDs to Dataspot asset type, UUID, and optionally inCollection.
    Stores the mapping in a CSV file for persistence. Handles organizational units.
    The REST endpoint is constructed dynamically.
    """

    def __init__(self, database_name: str, scheme: str):
        """
        Initialize the mapping table for organizational units.
        The CSV filename is derived from the database_name and scheme.

        Args:
            database_name (str): Name of the database to use for file naming.
                                 Example: "feature-staatskalender_DNK_staatskalender-dataspot-mapping.csv"
            scheme (str): Name of the scheme (e.g., 'DNK', 'TDM')
        """
        super().__init__(database_name, "staatskalender_id", "staatskalender-dataspot", scheme)


@dataclass
class OrgUnitChange:
    """Class to track changes to organizational units"""
    staatskalender_id: str
    title: str
    change_type: str  # "create", "update", "delete"
    details: Dict[str, Any]  # Details about the change


class OrgStructureHandler(BaseDataspotHandler):
    """
    Handler for organizational structure operations in Dataspot.
    Coordinates the transformation, comparison, and updating of organizational units.
    """
    
    # Set configuration values for the base handler
    asset_id_field = 'id_im_staatskalender'
    
    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the OrgStructureHandler.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        # Call parent's __init__ method first
        super().__init__(client)
        
        # Initialize the organization mapping
        self.mapping = OrgStructureMapping(database_name=client.database_name, scheme=client.scheme_name_short)
        
        # Set the asset type filter based on asset_id_field and stereotype
        self.asset_type_filter = lambda asset: (
            asset.get('_type') == 'Collection' and 
            asset.get('stereotype') == 'Organisationseinheit' and 
            asset.get(self.asset_id_field) is not None
        )
        
        # Create component instances
        self.updater = OrgStructureUpdater(client)
    
    def bulk_create_or_update_organizational_units(
        self, 
        organizational_units: List[Dict[str, Any]], 
        operation: str = "ADD", 
        dry_run: bool = False
    ) -> dict:
        """
        Create multiple organizational units in bulk in Dataspot.
        
        Args:
            organizational_units: List of organizational unit data to upload
            operation: Upload operation mode (ADD, REPLACE, FULL_LOAD)
            dry_run: Whether to perform a test run without changing data
            
        Returns:
            dict: The JSON response from the API containing the upload results
        """
        if not organizational_units:
            logging.warning("No organizational units provided for bulk upload")
            return {"status": "error", "message": "No organizational units provided"}
            
        # Call the base class method with our specific asset type
        return self.bulk_create_or_update_assets(organizational_units, operation, dry_run)
    
    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any]) -> dict:
        """
        Build organization hierarchy in the DNK based on data from ODS API using bulk upload.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            
        Returns:
            dict: The response with status information
        """
        logging.info("Building organization hierarchy using bulk upload...")
        
        # Transform organization data for bulk upload
        org_units = OrgStructureTransformer.transform_for_bulk_upload(org_data)
        
        if not org_units:
            logging.warning("No organizational units to upload")
            return {"status": "error", "message": "No organizational units to upload"}
        
        # Extract Staatskalender IDs for mapping updates
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
        level_results = {}
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
                level_results[depth] = response
                logging.info(f"Successfully uploaded {len(level_units)} units at depth level {depth}")
                
                # If this level has errors, log them but continue
                if isinstance(response, list):
                    errors = [msg for msg in response if isinstance(msg, dict) and msg.get('level') == 'ERROR']
                    if errors:
                        error_count = len(errors)
                        logging.warning(f"Upload for level {depth} completed with {error_count} errors")
                        
                        # Log up to 5 errors in detail
                        for i, error in enumerate(errors[:5]):
                            error_msg = error.get('message', str(error))
                            upload_errors.append(f"Level {depth} error: {error_msg}")
                            logging.warning(f"Level {depth} upload error {i+1}: {error_msg}")
                        
                        if error_count > 5:
                            logging.warning(f"... and {error_count - 5} more errors")
            except Exception as e:
                error_msg = f"Error uploading level {depth}: {str(e)}"
                logging.error(error_msg)
                upload_errors.append(error_msg)
        
        # Update mappings after upload
        self.update_mappings_after_upload(staatskalender_ids)
        self.mapping.save_to_csv()
        
        # Determine overall result
        if upload_errors:
            logging.warning(f"Upload completed with {len(upload_errors)} errors")
            result = {
                "status": "partial", 
                "message": "Hierarchy build completed with errors",
                "errors": upload_errors[:10],  # First 10 errors
                "total_errors": len(upload_errors),
                "levels_processed": len(level_results)
            }
        else:
            result = {
                "status": "success",
                "message": "Hierarchy build completed successfully",
                "levels_processed": len(level_results)
            }
            
        return result
    
    def sync_org_units(self, org_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            
        Returns:
            Dict[str, Any]: Summary of the synchronization process
        """
        logging.info("Starting synchronization of organizational units...")
        
        # Fetch current org data from Dataspot
        dataspot_units = self._fetch_current_org_units()

        # Update mappings before making changes
        self.update_mappings_before_upload()
        
        # Check if this is an initial run (no org units in Dataspot)
        is_initial_run = len(dataspot_units) == 0
        
        # If this is an initial run, perform bulk upload
        if is_initial_run:
            logging.info("No organizational units found in Dataspot. Performing initial bulk upload...")
            result = self.build_organization_hierarchy_from_ods_bulk(org_data)
            
            # Save updated mappings
            source_ids = [str(org_unit['id']) for org_unit in org_data.get('results', [])]
            self.update_mappings_after_upload(source_ids)
            self.mapping.save_to_csv()
            
            return {
                "status": result.get("status", "unknown"),
                "message": "Performed initial bulk upload as no existing organizational units were found",
                "details": result
            }
        
        # For incremental sync, transform both source and current data for comparison
        # Transform source org data to layers
        source_units_by_layer = OrgStructureTransformer.transform_to_layered_structure(org_data)
        
        # Index Dataspot units by Staatskalender ID
        dataspot_units_by_id = {
            str(unit.get(self.asset_id_field)): unit 
            for unit in dataspot_units
        }
        
        # Compare structures and identify changes
        changes = OrgStructureComparer.compare_structures(
            source_units_by_layer, dataspot_units_by_id)
        
        # If there are no changes, return early
        if not changes:
            return {
                "status": "no_changes",
                "message": "No changes detected between source and current organizational structures",
                "counts": {"total": 0, "created": 0, "updated": 0, "deleted": 0}
            }
        
        # Apply changes
        self.updater.apply_changes(changes, is_initial_run=False)
        
        # Update mappings after changes
        if changes:
            staatskalender_ids = [change.staatskalender_id for change in changes]
            self.update_mappings_after_upload(staatskalender_ids)
            self.mapping.save_to_csv()
        
        # Generate summary
        summary = OrgStructureComparer.generate_sync_summary(changes)
        
        return summary
    
    def _fetch_current_org_units(self) -> List[Dict[str, Any]]:
        """
        Fetch current organizational units from Dataspot.
        
        Returns:
            List of organizational units currently in Dataspot
        """
        logging.info("Fetching current organizational units from Dataspot...")
        return self.client.get_all_assets_from_scheme(self.asset_type_filter)
    
    def update_mappings_after_upload(self, staatskalender_ids: Optional[List[str]] = None) -> int:
        """
        Update local mappings after uploading organizational units.
        
        Args:
            staatskalender_ids: List of Staatskalender IDs to update mappings for
            
        Returns:
            int: Number of mappings successfully updated
        """
        # Use the base implementation to update mappings
        return self._download_and_update_mappings(staatskalender_ids)
    
    def update_mappings_before_upload(self) -> int:
        """
        Update local mappings before uploading organizational units.
        
        Returns:
            int: Number of mappings successfully updated
        """
        # Update all mappings without filtering by ID
        return self._download_and_update_mappings()
