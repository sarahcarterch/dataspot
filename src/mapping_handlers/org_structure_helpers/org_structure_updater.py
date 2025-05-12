import logging
from typing import Dict, Any, List

from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join
from src.mapping_handlers.org_structure_helpers.org_structure_comparer import OrgUnitChange


class OrgStructureUpdater:
    """
    Handles applying changes to organizational units in Dataspot.
    Responsible for creations, updates, and deletions of org units.
    """

    def __init__(self, client: BaseDataspotClient):
        """
        Initialize the OrgStructureUpdater.
        
        Args:
            client: BaseDataspotClient instance to use for API operations
        """
        self.client = client
        self.database_name = client.database_name
    
    def apply_changes(self, changes: List[OrgUnitChange], is_initial_run: bool = False) -> Dict[str, int]:
        """
        Apply the identified changes to the system.
        
        Args:
            changes: List of changes to apply
            is_initial_run: Whether this is an initial run with no existing org units
            
        Returns:
            Dict[str, int]: Statistics about applied changes
        """
        if not changes:
            logging.info("No changes to apply")
            return {"created": 0, "updated": 0, "deleted": 0, "errors": 0}
            
        logging.info(f"Applying {len(changes)} changes...")
        
        stats = {
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "errors": 0
        }
        
        # Group changes by type for clearer processing
        changes_by_type = {
            "create": [c for c in changes if c.change_type == "create"],
            "update": [c for c in changes if c.change_type == "update"],
            "delete": [c for c in changes if c.change_type == "delete"]
        }
        
        # First, handle deletions
        self._process_deletions(changes_by_type["delete"], stats)
        
        # Then handle updates
        self._process_updates(changes_by_type["update"], is_initial_run, stats)
        
        # Finally handle creations
        self._process_creations(changes_by_type["create"], stats)
        
        logging.info(f"Change application complete: {stats['created']} created, {stats['updated']} updated, "
                     f"{stats['deleted']} deleted, {stats['errors']} errors")
        
        return stats
    
    def _process_deletions(self, deletion_changes: List[OrgUnitChange], stats: Dict[str, int]) -> None:
        """
        Process deletion changes.
        
        Args:
            deletion_changes: List of deletion changes
            stats: Statistics dictionary to update
        """
        for change in deletion_changes:
            uuid = change.details.get("uuid")
            if not uuid:
                logging.warning(f"Cannot delete org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                stats["errors"] += 1
                continue
            
            # Construct endpoint for deletion
            endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
            logging.info(f"Deleting org unit '{change.title}' (ID: {change.staatskalender_id}) at {endpoint}")
            
            try:
                # Delete the asset
                self.client._delete_asset(endpoint)
                stats["deleted"] += 1
            except Exception as e:
                logging.error(f"Error deleting org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                stats["errors"] += 1
    
    def _process_updates(self, update_changes: List[OrgUnitChange], is_initial_run: bool, stats: Dict[str, int]) -> None:
        """
        Process update changes.
        
        Args:
            update_changes: List of update changes
            is_initial_run: Whether this is an initial run with no existing org units
            stats: Statistics dictionary to update
        """
        # For updates, we need to process them one at a time to handle interdependencies
        for change in update_changes:
            uuid = change.details.get("uuid")
            if not uuid:
                logging.warning(f"Cannot update org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                stats["errors"] += 1
                continue
            
            # Construct endpoint for update
            endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
            
            logging.info(f"Updating org unit '{change.title}' (ID: {change.staatskalender_id})")
            
            # Check if there are any changes to apply
            if not change.details.get("changes"):
                logging.debug(f"No changes needed for org unit '{change.title}' (ID: {change.staatskalender_id}), skipping update")
                continue
            
            # Create update data with only necessary fields
            update_data = self._create_update_data(change)
            
            # If nothing changed (only _type and stereotype is in update_data), skip the update
            if len(update_data) <= 2:  # Just _type and stereotype
                logging.debug(f"No actual changes for org unit '{change.title}' after filtering, skipping update")
                continue
            
            try:
                # Update the asset
                self.client._update_asset(endpoint, update_data, replace=False)
                stats["updated"] += 1
            except Exception as e:
                logging.error(f"Error updating org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                stats["errors"] += 1
    
    def _create_update_data(self, change: OrgUnitChange) -> Dict[str, Any]:
        """
        Create update data with only the necessary fields to change.
        
        Args:
            change: The change to create update data for
            
        Returns:
            Dict[str, Any]: The update data
        """
        # Base required fields
        update_data = {
            "_type": "Collection",
            "stereotype": "Organisationseinheit"
        }
        
        # Apply changes
        for field, change_info in change.details.get("changes", {}).items():
            if field == "customProperties":
                # For customProperties, only include what's changed
                if "customProperties" not in update_data:
                    update_data["customProperties"] = {}
                
                for prop, prop_change in change_info.items():
                    update_data["customProperties"][prop] = prop_change["new"]
            elif field == "inCollection" and change_info.get("parent_moved", False):
                # Special handling for moved collections - we need to set inCollection to parent UUID
                # When a collection is moved, we need the UUID of the parent collection, not the path
                try:
                    # Extract the parent path from the inCollection value
                    parent_path = change_info["new"]
                    
                    # Get the parent collection from Dataspot using the path
                    if parent_path:
                        # Build the endpoint using scheme name, path components, and asset type
                        # We need to convert path to endpoint format: /rest/{db}/schemes/{scheme}/collections/{path}
                        components = ["rest", self.database_name, "schemes", self.client.scheme_name]
                        
                        # Add each path component as a collection
                        path_parts = parent_path.split('/')
                        for part in path_parts:
                            components.append("collections")
                            components.append(part)
                        
                        # Get the parent collection details
                        parent_endpoint = '/'.join(components)
                        logging.info(f"Looking up parent collection at: {parent_endpoint}")
                        
                        # Get the parent's UUID
                        parent_collection = self.client._get_asset(parent_endpoint)
                        if parent_collection and "id" in parent_collection:
                            parent_uuid = parent_collection["id"]
                            logging.info(f"Found parent UUID: {parent_uuid} for path: {parent_path}")
                            
                            # Set the inCollection to the parent's UUID
                            update_data["inCollection"] = parent_uuid
                            logging.info(f"Collection '{change.title}' will be moved using inCollection UUID: {parent_uuid}")
                        else:
                            logging.error(f"Failed to find parent collection at path: {parent_path}")
                except Exception as e:
                    logging.error(f"Error finding parent collection for path {change_info.get('new', '')}: {str(e)}")
            elif field != "inCollection":  # Skip inCollection except for parent moved case
                # For simple fields, use the new value
                update_data[field] = change_info["new"]
        
        # If we have an empty customProperties after filtering, remove it
        if "customProperties" in update_data and not update_data["customProperties"]:
            del update_data["customProperties"]
        
        # Critical fix: Always include id_im_staatskalender in customProperties for PATCH requests
        # This ensures correct placement for the update operation
        if "id_im_staatskalender" not in update_data.get("customProperties", {}):
            if "customProperties" not in update_data:
                update_data["customProperties"] = {}
            update_data["customProperties"]["id_im_staatskalender"] = change.staatskalender_id
        
        # Log the update data we're creating
        if "inCollection" in update_data:
            if isinstance(update_data["inCollection"], str) and not update_data["inCollection"].startswith("/"):
                logging.info(f"Collection '{change.title}' will be moved using inCollection UUID: {update_data['inCollection']}")
        elif "label" in update_data:
            logging.info(f"Collection '{change.title}' will be renamed to: {update_data['label']}")
        
        return update_data
    
    def _process_creations(self, creation_changes: List[OrgUnitChange], stats: Dict[str, int]) -> None:
        """
        Process creation changes.
        
        Args:
            creation_changes: List of creation changes
            stats: Statistics dictionary to update
        """
        if not creation_changes:
            return
            
        # Group create changes by their inCollection value (parent path)
        create_by_parent = {}
        for change in creation_changes:
            source_unit = change.details.get("source_unit", {})
            parent_path = source_unit.get("inCollection", "")
            
            if parent_path not in create_by_parent:
                create_by_parent[parent_path] = []
            
            # Add this unit to its parent group
            create_by_parent[parent_path].append(source_unit)
        
        # Process each parent group
        for parent_path, units in create_by_parent.items():
            logging.info(f"Creating {len(units)} org units under parent path '{parent_path}'")
            
            try:
                # Bulk upload these units
                response = self.client.bulk_create_or_update_assets(
                    scheme_name=self.client.scheme_name,
                    data=units,
                    operation="ADD",
                    dry_run=False
                )
                
                # Check for errors
                errors = [message for message in response if isinstance(message, dict) and message.get('level') == 'ERROR']
                if errors:
                    logging.warning(f"Bulk creation completed with {len(errors)} errors")
                    stats["errors"] += len(errors)
                    stats["created"] += len(units) - len(errors)
                    for error in errors[:5]:  # Log first 5 errors
                        logging.error(f"  - {error.get('message', 'Unknown error')}")
                else:
                    stats["created"] += len(units)
                    logging.info(f"Successfully created {len(units)} units")
            except Exception as e:
                logging.error(f"Error during bulk creation of units under '{parent_path}': {str(e)}")
                stats["errors"] += len(units)
