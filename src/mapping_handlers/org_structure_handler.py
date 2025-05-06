import logging
from typing import Dict, Any, List, Callable
from dataclasses import dataclass

from src import config
from src.clients.base_client import BaseDataspotClient
from src.clients.helpers import url_join, escape_special_chars
from src.mapping_handlers.base_dataspot_handler import BaseDataspotHandler
from src.mapping_handlers.org_structure_mapping import OrgStructureMapping
from src.common import requests_get


@dataclass
class OrgUnitChange:
    """Class to track changes to organizational units"""
    staatskalender_id: str
    title: str
    change_type: str  # "create", "update", "delete"
    details: Dict[str, Any]  # Details about the change


class OrgStructureHandler(BaseDataspotHandler):
    """Handler for organizational structure operations in Dataspot."""
    
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
    
    def get_validated_staatskalender_url(self, title: str, url_website: str, validate_url: bool = False) -> str:
        """
        Validate a Staatskalender URL for an organization or use the provided URL.
        
        Args:
            title (str): The organization title
            url_website (str): The URL provided in the data
            validate_url (bool): Whether to validate the URL by making an HTTP request
            
        Returns:
            str: The validated URL for the organization, or empty string if invalid or validation fails
            
        Note:
            If validation fails or no URL is provided, an empty string is returned.
            No exceptions are raised from this method, validation errors are logged.
        """
        # If URL is already provided, optionally validate it
        if url_website:
            if not validate_url:
                return url_website
                
            # Validate the provided URL
            try:
                response = requests_get(url_website)
                if response.status_code == 200:
                    return url_website
                logging.warning(f"Invalid provided URL for organization '{title}': {url_website}")
            except Exception as e:
                logging.warning(f"Error validating URL for organization '{title}': {url_website}")
                
        # If no URL or validation failed, return empty string
        return ""
    
    def update_staatskalender_mappings_from_upload(self, staatskalender_ids: List[str]) -> None:
        """
        Updates the mapping between Staatskalender IDs and Dataspot UUIDs after uploading organizational units.
        Uses the download API to retrieve all organizational units and then updates the mapping for matching Staatskalender IDs.
        
        Args:
            staatskalender_ids (List[str]): List of Staatskalender IDs to update in the mapping
            
        Raises:
            HTTPError: If API requests fail
            ValueError: If unable to retrieve organizational unit information
        """
        # Call the base class method with our specific ID type
        self.update_mappings_from_upload(staatskalender_ids)
    
    def bulk_create_or_update_organizational_units(self, organizational_units: List[Dict[str, Any]], 
                                        operation: str = "ADD", dry_run: bool = False) -> dict:
        """
        Create multiple organizational units in bulk in the Datennutzungskatalog scheme in Dataspot.
        
        Args:
            organizational_units (List[Dict[str, Any]]): List of organizational unit data to upload
            operation (str, optional): Upload operation mode. Defaults to "ADD".
                                      "ADD": Add or update only. Existing units not in the upload remain unchanged.
                                      "REPLACE": Reconcile elements. Units not in the upload are considered obsolete.
                                      "FULL_LOAD": Reconcile model. Completely replaces with the uploaded units.
            dry_run (bool, optional): Whether to perform a test run without changing data. Defaults to False.
            
        Returns:
            dict: The JSON response from the API containing the upload results
            
        Raises:
            ValueError: If no organizational units are provided or if the DNK scheme doesn't exist
            HTTPError: If API requests fail
        """
        # Call the base class method with our specific asset type
        return self.bulk_create_or_update_assets(organizational_units, operation, dry_run)
    
    def get_all_staatskalender_ids(self) -> List[str]:
        """
        Get a list of all Staatskalender IDs in the mapping.
        
        Returns:
            List[str]: A list of all Staatskalender IDs
        """
        # Call the base class method
        return self.get_all_external_ids()

    def transform_organization_for_bulk_upload(self, org_data: Dict[str, Any], validate_urls: bool = False) -> List[Dict[str, Any]]:
        """
        Build organization hierarchy from flat data using parent_id and children_id fields.
        
        Args:
            org_data (Dict[str, Any]): Organization data from ODS API
            validate_urls (bool): Whether to validate Staatskalender URLs
            
        Returns:
            List[Dict[str, Any]]: List of organizational units with hierarchy info
            
        Raises:
            ValueError: If organization data is invalid, missing 'results' key, or no root nodes found
            Exception: If there's an error processing children_id fields
        """
        if not org_data or 'results' not in org_data:
            logging.error("Invalid organization data format. Data must contain a 'results' key.")
            raise ValueError("Invalid organization data format. Data must contain a 'results' key.")
        
        # Build a lookup dictionary for quick access to organization by ID
        org_lookup : Dict[str, Dict[str, Any]] = {str(org['id']): org for org in org_data['results']} # Assume that the id is a string
        logging.info(f"Processing {len(org_lookup)} organizations from input data")
        
        # Create a dictionary to track parent-child relationships
        parent_child_map = {}
        
        # Track missing entries that are referenced
        missing_entries = set()
        
        # Track organizations that reference missing parents - these will be excluded
        invalid_orgs = set()
        
        # Process each organization to build parent-child relationships
        for org_id, org in org_lookup.items():
            # Get parent ID
            parent_id = str(org.get('parent_id', '')).strip()
            if parent_id and parent_id.lower() != 'nan':
                # Check if parent exists
                if parent_id not in org_lookup:
                    missing_entries.add(parent_id)
                    logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing parent {parent_id}. Skipping...")
                    # Mark this organization as invalid (has missing parent)
                    invalid_orgs.add(org_id)
                    continue
                
                # Add this org as a child of its parent
                if parent_id not in parent_child_map:
                    parent_child_map[parent_id] = []
                parent_child_map[parent_id].append(org_id)
            
            # Check children IDs for consistency
            children_ids = org.get('children_id', '')
            if children_ids:
                # Parse the children IDs - they might be in various formats
                try:
                    if isinstance(children_ids, str):
                        # Try to split by comma if it's a string
                        children_list = [id.strip() for id in children_ids.split(',')]
                    elif isinstance(children_ids, list):
                        children_list = [str(id).strip() for id in children_ids]
                    else:
                        children_list = [str(children_ids).strip()]
                    
                    # Add these children to the map if not already added by parent_id
                    if org_id not in parent_child_map:
                        parent_child_map[org_id] = []
                    
                    # Check each child
                    for child_id in children_list:
                        if child_id and child_id.lower() != 'nan':
                            if child_id not in org_lookup:
                                missing_entries.add(child_id)
                                logging.warning(f"Organization {org_id} ('{org.get('title', 'Unknown')}') references missing child {child_id}")
                            else:
                                # Only add if not already in the list
                                if child_id not in parent_child_map[org_id]:
                                    parent_child_map[org_id].append(child_id)
                except Exception as e:
                    logging.error(f"Error processing children_id for organization {org_id}: {children_ids}")
                    logging.error(f"Exception: {str(e)}")
                    raise
        
        # Log all missing entries
        if missing_entries:
            logging.warning(f"Found {len(missing_entries)} missing organizations referenced in the data: {', '.join(missing_entries)}")
        
        # Find root nodes (those without parents or with 'nan' as parent)
        root_nodes = []
        for org_id, org in org_lookup.items():
            # Skip organizations with missing parents
            if org_id in invalid_orgs:
                continue
                
            parent_id = str(org.get('parent_id', '')).strip()
            if not parent_id:
                logging.info(f"Organization {org_id} ('{org.get('title', 'Unknown')}') has no parent ID. Treating as root node.")
                root_nodes.append(org_id)
            elif parent_id.lower() == 'nan':
                logging.info(f"Organization {org_id} ('{org.get('title', 'Unknown')}') has 'nan' as parent ID. Treating as root node.")
                root_nodes.append(org_id)
        
        if not root_nodes:
            logging.error("No root nodes found in organization data. Hierarchy cannot be built.")
            raise ValueError("No root nodes found in organization data. Hierarchy cannot be built.")
        
        logging.info(f"Found {len(root_nodes)} root nodes")
        logging.info(f"Start construction of organizational structure - Not yet uploading...")

        # Dictionary to store organization titles by ID for lookup
        org_title_by_id = {org_id: org.get('title', '').strip() for org_id, org in org_lookup.items()}
        
        # Dictionary to track the path components for each organization
        # For root nodes, the path is just their title as a single component
        # For other nodes, it will be built recursively as a list of components
        # This avoids problems with slashes in organization names
        path_components_by_id = {}
        for root_id in root_nodes:
            path_components_by_id[root_id] = [org_title_by_id.get(root_id, '')]
        
        # Function to check if an organization or any of its ancestors is invalid
        def has_invalid_ancestor(org_id):
            if org_id in invalid_orgs:
                return True
                
            parent_id = str(org_lookup.get(org_id, {}).get('parent_id', '')).strip()
            if not parent_id or parent_id.lower() == 'nan' or parent_id not in org_lookup:
                return False
                
            return has_invalid_ancestor(parent_id)
        
        # Function to get or build the path components for an organization
        def get_path_components(org_id):
            # If path already calculated, return it
            if org_id in path_components_by_id:
                return path_components_by_id[org_id]
            
            # Get the organization's title
            title = org_title_by_id.get(org_id, '')
            if not title:
                logging.warning(f"Organization {org_id} has no title, using ID as title")
                title = f"Unknown-{org_id}"
            
            # Get the parent ID
            parent_id = str(org_lookup.get(org_id, {}).get('parent_id', '')).strip()
            if not parent_id or parent_id.lower() == 'nan' or parent_id not in org_lookup:
                # If no valid parent, this is effectively a root node
                path_components = [title]
                path_components_by_id[org_id] = path_components
                return path_components
            
            # Get the parent's path components recursively
            parent_path_components = get_path_components(parent_id)
            
            # Create the full path components by appending this org's title
            path_components = parent_path_components + [title]
            
            # Store the components in the mapping
            path_components_by_id[org_id] = path_components
            return path_components
        
        # Now construct hierarchical data for each organization
        all_units = []
        processed_ids = set()
        excluded_ids = set()  # Track excluded organizations
        
        # Recursively identify and exclude all descendants of invalid organizations
        def mark_descendants_as_invalid(org_id):
            if org_id in parent_child_map:
                for child_id in parent_child_map[org_id]:
                    invalid_orgs.add(child_id)
                    excluded_ids.add(child_id)
                    mark_descendants_as_invalid(child_id)
        
        # Mark all descendants of invalid organizations as invalid too
        for org_id in list(invalid_orgs):
            mark_descendants_as_invalid(org_id)
        
        if excluded_ids:
            logging.warning(f"Excluding {len(excluded_ids)} organizations with missing parents or ancestors: {', '.join(excluded_ids)}")
        
        # BFS traversal to build hierarchy level by level
        for depth in range(100):  # Safety limit to prevent infinite loops
            logging.info(f"Processing depth level {depth}")
            current_level = []
            
            # For depth 0, start with root nodes
            if depth == 0:
                current_level = root_nodes
            else:
                # Find all children of the previous level's nodes
                for parent_id in processed_ids:  # Check all processed nodes for children
                    if parent_id in parent_child_map:
                        for child_id in parent_child_map[parent_id]:
                            if child_id not in processed_ids and child_id not in invalid_orgs:
                                current_level.append(child_id)
            
            # If no more nodes at this level, we're done
            if not current_level:
                logging.info(f"No more nodes to process at depth {depth}. Hierarchy build complete.")
                break
            
            logging.info(f"Processing {len(current_level)} organizations at depth {depth}")
            
            # Process each node at this level
            for org_id in current_level:
                # Skip if already processed
                if org_id in processed_ids:
                    continue
                
                # Skip if this org or any of its ancestors is invalid
                if org_id in invalid_orgs:
                    continue
                
                # Mark as processed
                processed_ids.add(org_id)
                
                # Get organization data
                if org_id not in org_lookup:
                    logging.warning(f"Organization ID {org_id} referenced but not found in data")
                    continue
                
                org = org_lookup[org_id]
                title = org.get('title', '')
                if not title:
                    logging.warning(f"Organization {org_id} missing title, skipping")
                    continue
                
                # Get or validate URL
                url_website = self.get_validated_staatskalender_url(
                    title, 
                    org.get('url_website', ''), 
                    validate_url=validate_urls
                )
                
                # Create unit data
                unit_data = {
                    "_type": "Collection",
                    "label": title.strip(),
                    "stereotype": "Organisationseinheit",
                    "_hierarchy_depth": depth,
                    "id_im_staatskalender": org_id
                }
                
                # Add custom properties
                custom_properties = {}
                if url_website:
                    custom_properties["link_zum_staatskalender"] = url_website
                if org_id:
                    custom_properties["id_im_staatskalender"] = org_id
                if custom_properties:
                    unit_data["customProperties"] = custom_properties
                
                # Set parent relationship using hierarchical business key
                parent_id = str(org.get('parent_id', '')).strip()
                if parent_id and parent_id.lower() != 'nan' and parent_id in org_lookup:
                    # Get the parent's path components
                    parent_path_components = get_path_components(parent_id)
                    
                    if parent_path_components:
                        # Escape each path component individually
                        escaped_components = []
                        
                        for i, comp in enumerate(parent_path_components):
                            # Log before escaping for components with special characters
                            if any(char in comp for char in ['/', '.', '"']):
                                logging.debug(f"Component {i} before escaping: '{comp}'")
                            
                            # Escape special characters in this component
                            escaped_comp = escape_special_chars(comp)
                            
                            # Log after escaping for components with special characters
                            if any(char in comp for char in ['/', '.', '"']):
                                logging.debug(f"Component {i} after escaping: '{escaped_comp}'")
                                
                            escaped_components.append(escaped_comp)
                        
                        # Join the escaped components with slashes
                        escaped_parent_path = '/'.join(escaped_components)
                        
                        # For debugging, show original path components and final escaped path
                        original_path = '/'.join(parent_path_components)
                        logging.debug(f"Original path components: {parent_path_components}")
                        logging.debug(f"Escaped path: '{escaped_parent_path}'")
                        
                        unit_data["inCollection"] = escaped_parent_path
                        logging.debug(f"Setting inCollection for '{title}' to '{escaped_parent_path}'")
                
                # Add to the units list
                all_units.append(unit_data)
        
        # Calculate total invalid organizations (those with invalid ancestors)
        total_excluded = len(invalid_orgs) + len(excluded_ids)
        logging.info(f"Built hierarchy with {len(all_units)} organizational units (excluded {total_excluded} due to missing parents/ancestors)")
        
        # Check for any organizations not included in the hierarchy
        not_processed = set(org_lookup.keys()) - processed_ids - invalid_orgs - excluded_ids
        if not_processed:
            logging.warning(f"{len(not_processed)} organizations not included in the hierarchy due to circular references or other issues")
            
        return all_units
    
    def build_organization_hierarchy_from_ods_bulk(self, org_data: Dict[str, Any], validate_urls: bool = False) -> dict:
        """
        Build organization hierarchy in the DNK based on data from ODS API using bulk upload.
        
        This method uses parent_id and children_id to build the hierarchy, not title_full.
        It processes the hierarchy level by level.
        
        Args:
            org_data (Dict[str, Any]): Dictionary containing organization data from ODS API
            validate_urls (bool): Whether to validate Staatskalender URLs (can be slow)
            
        Returns:
            dict: The response from the final bulk upload API call with status information
            
        Raises:
            ValueError: If organization data is invalid or no organizational units can be built
            HTTPError: If bulk upload API requests fail
        """
        logging.info("Building organization hierarchy using level-by-level bulk upload...")
        
        # Preload mappings from DNK to ensure we have the latest mapping data
        try:
            logging.info("Preloading Staatskalender ID to Dataspot mappings from DNK system")
            self._download_and_update_mappings()
        except Exception as e:
            logging.warning(f"Failed to preload organizational unit mappings, continuing with existing mappings: {str(e)}")
            
        # Transform organization data for bulk upload
        org_units = self.transform_organization_for_bulk_upload(org_data, validate_urls=validate_urls)
        
        if not org_units:
            logging.warning("No organizational units to upload")
            return {"status": "error", "message": "No organizational units to upload"}
        
        # Extract Staatskalender IDs from all units for mapping updates
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
        level_result = {}
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
                level_result[depth] = response
                logging.info(f"Successfully uploaded {len(level_units)} units at depth level {depth}")
                
                # If this level has relationship errors, log them but continue
                if 'errors' in response and response.get('errors'):
                    error_count = len(response.get('errors', []))
                    logging.warning(f"Upload for level {depth} completed with {error_count} errors")
                    
                    # Log up to 10 errors in detail
                    for i, error in enumerate(response.get('errors', [])[:10]):
                        error_msg = error.get('error', str(error))
                        upload_errors.append(f"Level {depth} error: {error_msg}")
                        logging.warning(f"Level {depth} upload error {i+1}: {error_msg}")
                    
                    if error_count > 10:
                        logging.warning(f"... and {error_count - 10} more errors")
                
            except Exception as e:
                error_msg = f"Error uploading level {depth}: {str(e)}"
                logging.error(error_msg)
                upload_errors.append(error_msg)
        
        # Update mappings for all uploaded organizations
        try:
            if staatskalender_ids:
                logging.info(f"Updating mappings for {len(staatskalender_ids)} organizational units")
                self.update_staatskalender_mappings_from_upload(staatskalender_ids)
        except Exception as e:
            logging.warning(f"Error updating organizational unit mappings: {str(e)}")
            # Add to errors but continue
            upload_errors.append(f"Error updating mappings: {str(e)}")
        
        logging.info("Organization hierarchy build completed")
        
        # Determine overall result
        if upload_errors:
            logging.warning(f"Upload completed with {len(upload_errors)} errors")
            
            # Sample up to 10 errors for the result
            sample_errors = upload_errors[:10]
            if len(upload_errors) > 10:
                sample_errors.append(f"... and {len(upload_errors) - 10} more errors")
                
            result = {
                "status": "partial", 
                "message": "Hierarchy build completed with errors",
                "errors": sample_errors,
                "total_errors": len(upload_errors),
                "levels_processed": len(level_result)
            }
        else:
            result = {
                "status": "success",
                "message": "Hierarchy build completed successfully",
                "levels_processed": len(level_result)
            }
            
        return result

    def sync_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """
        Synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        This is the main public method for organizational unit synchronization.
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            validate_urls: Whether to validate Staatskalender URLs
            
        Returns:
            Dict: Summary of the synchronization process
        """
        return self._sync_staatskalender_org_units(org_data, validate_urls)
    
    def _sync_staatskalender_org_units(self, org_data: Dict[str, Any], validate_urls: bool = False) -> Dict[str, Any]:
        """
        Internal implementation to synchronize organizational units in Dataspot with data from the Staatskalender ODS API.
        
        This method implements the following algorithm:
        1. Fetch source org data from ODS (Staatskalender)
        2. Transform the data to a tree structure by layer
        3. Fetch current org data from Dataspot via Download API
        4. Compare the two trees
        5. Apply changes to Dataspot via REST API
        6. Update mappings after changes
        7. Generate a summary of changes
        
        Args:
            org_data: Dictionary containing organization data from ODS API
            validate_urls: Whether to validate Staatskalender URLs (can be slow)
            
        Returns:
            Dict[str, Any]: Summary of the synchronization process
        """
        logging.info("Starting synchronization of organizational units...")
        
        # Step 1 & 2: Transform source org data to a tree structure by layer
        source_units_by_layer = self._transform_org_data_to_layers(org_data)
        logging.info(f"Transformed source data into {len(source_units_by_layer)} layers")
        
        # Step 3: Fetch current org data from Dataspot
        dataspot_units = self._fetch_current_org_units()
        
        # Check if we have any org units in Dataspot
        if not dataspot_units:
            logging.info("No organizational units found in Dataspot. Performing bulk upload...")
            result = self.build_organization_hierarchy_from_ods_bulk(org_data, validate_urls=validate_urls)
            return {
                "status": result.get("status", "unknown"),
                "message": "Performed bulk upload as no existing organizational units were found",
                "details": result
            }
        
        # Step 4: Build a similar tree structure from Dataspot data
        dataspot_units_by_id = {
            str(unit.get("id_im_staatskalender")): unit 
            for unit in dataspot_units
        }
        
        # Step 5: Compare trees and identify changes
        changes = self._compare_org_structures(source_units_by_layer, dataspot_units_by_id)
        
        # Step 6: Apply changes
        self._apply_org_unit_changes(changes, validate_urls)
        
        # Step 7: Update mappings after changes
        if changes:
            staatskalender_ids = [change.staatskalender_id for change in changes]
            logging.info(f"Updating mappings for {len(staatskalender_ids)} changed organizational units")
            try:
                self._download_and_update_mappings(staatskalender_ids)
            except Exception as e:
                logging.error(f"Error updating mappings: {str(e)}")
        
        # Generate summary
        summary = self._generate_sync_summary(changes)
        
        return summary
    
    def _transform_org_data_to_layers(self, org_data: Dict[str, Any]) -> Dict[int, List[Dict[str, Any]]]:
        """
        Transform organization data into a layer-based structure.
        
        Args:
            org_data (Dict[str, Any]): Organization data from ODS API
            
        Returns:
            Dict[int, List[Dict[str, Any]]]: Dictionary with depth as key and list of org units at that depth
        """
        logging.info("Transforming organization data to layer structure...")
        
        # Use transform_organization_for_bulk_upload as it already builds the hierarchy
        org_units = self.transform_organization_for_bulk_upload(org_data, validate_urls=False)
        
        # Group units by depth
        units_by_depth = {}
        
        for unit in org_units:
            # Get the hierarchy depth 
            depth = unit.get("_hierarchy_depth", 0)
            
            # Add to the appropriate depth group
            if depth not in units_by_depth:
                units_by_depth[depth] = []
            
            # Add a cleaned copy of the unit without the depth field
            clean_unit = unit.copy()
            if "_hierarchy_depth" in clean_unit:
                del clean_unit["_hierarchy_depth"]
                
            units_by_depth[depth].append(clean_unit)
        
        logging.info(f"Organized {len(org_units)} units into {len(units_by_depth)} depth layers")
        return units_by_depth
    
    def _fetch_current_org_units(self) -> List[Dict[str, Any]]:
        """
        Fetch current organizational units from Dataspot using the Download API.
        
        Returns:
            List[Dict[str, Any]]: List of organizational units currently in Dataspot
        """
        logging.info("Fetching current organizational units from Dataspot...")
        
        # Use the download API to retrieve collections from the scheme
        download_path = f"/api/{self.database_name}/schemes/{self.scheme_name}/download?format=JSON"
        full_url = url_join(self.client.base_url, download_path)
        
        logging.debug(f"Downloading collections from: {full_url}")
        try:
            response = requests_get(full_url, headers=self.client.auth.get_headers())
            response.raise_for_status()
            
            # Parse the JSON response
            all_items = response.json()
            
            # If we got a list directly, use it
            if isinstance(all_items, list):
                # Filter to only include collections with stereotype Organisationseinheit and id_im_staatskalender
                org_units = [item for item in all_items if 
                        item.get('_type') == 'Collection' and 
                        item.get('stereotype') == 'Organisationseinheit' and 
                        item.get('id_im_staatskalender')]
                
                logging.info(f"Found {len(org_units)} organizational units in Dataspot")
                return org_units
            else:
                logging.error(f"Received unexpected response format from {full_url}. Expected a list of items.")
                logging.debug(f"Response content: {all_items}")
                raise ValueError(f"Unexpected response format from download API. Expected a list but got: {type(all_items)}")
        except Exception as e:
            logging.error(f"Error fetching organizational units: {str(e)}")
            return []
    
    def _compare_org_structures(
        self, 
        source_units_by_layer: Dict[int, List[Dict[str, Any]]], 
        dataspot_units_by_id: Dict[str, Dict[str, Any]]
    ) -> List[OrgUnitChange]:
        """
        Compare the source organizational structure with the current structure in Dataspot.
        
        Args:
            source_units_by_layer: Organization units from source (ODS) organized by depth layer
            dataspot_units_by_id: Organization units from Dataspot indexed by Staatskalender ID
            
        Returns:
            List[OrgUnitChange]: List of changes to be applied
        """
        logging.info("Comparing source and current organizational structures...")
        
        changes = []
        
        # Process each layer in the source structure
        for layer, units in source_units_by_layer.items():
            logging.info(f"Comparing layer {layer} with {len(units)} units")
            
            for unit in units:
                staatskalender_id = str(unit.get("id_im_staatskalender", ""))
                if not staatskalender_id:
                    logging.warning("Found unit without Staatskalender ID, skipping")
                    continue
                
                title = unit.get("label", "")
                
                # Check if this unit exists in Dataspot
                if staatskalender_id in dataspot_units_by_id:
                    # Unit exists, check for changes
                    dataspot_unit = dataspot_units_by_id[staatskalender_id]
                    changes_needed = self._check_for_unit_changes(unit, dataspot_unit)
                    
                    if changes_needed:
                        changes.append(OrgUnitChange(
                            staatskalender_id=staatskalender_id,
                            title=title,
                            change_type="update",
                            details={
                                "uuid": dataspot_unit.get("id"),
                                "changes": changes_needed,
                                "source_unit": unit,
                                "current_unit": dataspot_unit
                            }
                        ))
                else:
                    # Unit doesn't exist, mark for creation
                    changes.append(OrgUnitChange(
                        staatskalender_id=staatskalender_id,
                        title=title,
                        change_type="create",
                        details={
                            "source_unit": unit
                        }
                    ))
        
        # Check for units in Dataspot that are not in the source (to be deleted)
        source_ids = set()
        for layer_units in source_units_by_layer.values():
            for unit in layer_units:
                staatskalender_id = str(unit.get("id_im_staatskalender", ""))
                if staatskalender_id:
                    source_ids.add(staatskalender_id)
        
        for staatskalender_id, unit in dataspot_units_by_id.items():
            if staatskalender_id not in source_ids:
                title = unit.get("label", f"Unknown ({staatskalender_id})")
                changes.append(OrgUnitChange(
                    staatskalender_id=staatskalender_id,
                    title=title,
                    change_type="delete",
                    details={
                        "uuid": unit.get("id"),
                        "current_unit": unit
                    }
                ))
        
        logging.info(f"Identified {len(changes)} changes needed: "
                    f"{sum(1 for c in changes if c.change_type == 'create')} creations, "
                    f"{sum(1 for c in changes if c.change_type == 'update')} updates, "
                    f"{sum(1 for c in changes if c.change_type == 'delete')} deletions")
        
        return changes
    
    def _check_for_unit_changes(self, source_unit: Dict[str, Any], dataspot_unit: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if there are differences between source and Dataspot versions of an org unit.
        
        Args:
            source_unit: Organization unit data from source (ODS)
            dataspot_unit: Organization unit data from Dataspot
            
        Returns:
            Dict[str, Any]: Dictionary of changes needed (empty if no changes)
        """
        changes = {}
        
        # Check label (title)
        if source_unit.get("label") != dataspot_unit.get("label"):
            changes["label"] = {
                "old": dataspot_unit.get("label"),
                "new": source_unit.get("label")
            }
        
        # Check inCollection path (parent relationship)
        if source_unit.get("inCollection") != dataspot_unit.get("inCollection"):
            changes["inCollection"] = {
                "old": dataspot_unit.get("inCollection"),
                "new": source_unit.get("inCollection")
            }
        
        # Check custom properties - specifically link_zum_staatskalender
        source_url = source_unit.get("customProperties", {}).get("link_zum_staatskalender", "")
        dataspot_url = dataspot_unit.get("customProperties", {}).get("link_zum_staatskalender", "")
        
        if source_url != dataspot_url:
            if "customProperties" not in changes:
                changes["customProperties"] = {}
            
            changes["customProperties"]["link_zum_staatskalender"] = {
                "old": dataspot_url,
                "new": source_url
            }
        
        return changes
    
    def _apply_org_unit_changes(self, changes: List[OrgUnitChange], validate_urls: bool = False) -> Dict[str, int]:
        """
        Apply the identified changes to Dataspot.
        
        Args:
            changes: List of changes to apply
            validate_urls: Whether to validate URLs when creating or updating org units
            
        Returns:
            Dict[str, int]: Statistics about applied changes
        """
        logging.info(f"Applying {len(changes)} changes to Dataspot...")
        
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
        for change in changes_by_type["delete"]:
            try:
                uuid = change.details.get("uuid")
                if not uuid:
                    logging.warning(f"Cannot delete org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                    stats["errors"] += 1
                    continue
                
                # Construct endpoint for deletion
                endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
                logging.info(f"Deleting org unit '{change.title}' (ID: {change.staatskalender_id}) at {endpoint}")
                
                # Delete the asset
                self.client._delete_asset(endpoint)
                stats["deleted"] += 1
                
                # Remove from mapping
                self.mapping.remove_entry(change.staatskalender_id)
                
            except Exception as e:
                logging.error(f"Error deleting org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                stats["errors"] += 1
        
        # Next, handle updates
        for change in changes_by_type["update"]:
            try:
                uuid = change.details.get("uuid")
                if not uuid:
                    logging.warning(f"Cannot update org unit '{change.title}' (ID: {change.staatskalender_id}) - missing UUID")
                    stats["errors"] += 1
                    continue
                
                # Construct endpoint for update
                endpoint = url_join('rest', self.database_name, 'collections', uuid, leading_slash=True)
                
                logging.info(f"Updating org unit '{change.title}' (ID: {change.staatskalender_id})")
                
                # Prepare update data
                update_data = change.details.get("current_unit", {}).copy()
                
                # Apply changes
                for field, change_info in change.details.get("changes", {}).items():
                    if field == "customProperties":
                        # For customProperties, we need to merge with existing
                        if "customProperties" not in update_data:
                            update_data["customProperties"] = {}
                        
                        for prop, prop_change in change_info.items():
                            update_data["customProperties"][prop] = prop_change["new"]
                    else:
                        # For simple fields, just update directly
                        update_data[field] = change_info["new"]
                
                # If we're updating the URL, validate it if requested
                if validate_urls and "customProperties" in update_data and "link_zum_staatskalender" in update_data["customProperties"]:
                    url = update_data["customProperties"]["link_zum_staatskalender"]
                    validated_url = self.get_validated_staatskalender_url(change.title, url, validate_url=True)
                    update_data["customProperties"]["link_zum_staatskalender"] = validated_url
                
                # Update the asset
                self.client._update_asset(endpoint, update_data, replace=True)
                stats["updated"] += 1
                
            except Exception as e:
                logging.error(f"Error updating org unit '{change.title}' (ID: {change.staatskalender_id}): {str(e)}")
                stats["errors"] += 1
        
        # Finally, handle bulk creation of new units
        # This is more complex because we need to create them in the right order (by layer)
        # and we need to use the bulk upload API
        
        # Group create changes by their inCollection value (parent path)
        create_by_parent = {}
        for change in changes_by_type["create"]:
            source_unit = change.details.get("source_unit", {})
            parent_path = source_unit.get("inCollection", "")
            
            if parent_path not in create_by_parent:
                create_by_parent[parent_path] = []
            
            # Add this unit to its parent group
            create_by_parent[parent_path].append(source_unit)
        
        # Process each parent group
        for parent_path, units in create_by_parent.items():
            try:
                logging.info(f"Creating {len(units)} org units under parent path '{parent_path}'")
                
                # Prepare units for bulk upload
                prepared_units = []
                
                for unit in units:
                    # If we're validating URLs, do that now
                    if validate_urls and "customProperties" in unit and "link_zum_staatskalender" in unit["customProperties"]:
                        url = unit["customProperties"]["link_zum_staatskalender"]
                        title = unit.get("label", "")
                        validated_url = self.get_validated_staatskalender_url(title, url, validate_url=True)
                        unit["customProperties"]["link_zum_staatskalender"] = validated_url
                    
                    prepared_units.append(unit)
                
                # Bulk upload these units
                response = self.bulk_create_or_update_organizational_units(
                    organizational_units=prepared_units,
                    operation="ADD",
                    dry_run=False
                )
                
                # Check for errors
                errors = response.get("errors", [])
                if errors:
                    logging.warning(f"Bulk creation completed with {len(errors)} errors")
                    stats["errors"] += len(errors)
                    stats["created"] += len(prepared_units) - len(errors)
                else:
                    stats["created"] += len(prepared_units)
                
            except Exception as e:
                logging.error(f"Error creating org units under parent '{parent_path}': {str(e)}")
                stats["errors"] += len(units)
        
        logging.info(f"Change application complete: {stats['created']} created, {stats['updated']} updated, "
                     f"{stats['deleted']} deleted, {stats['errors']} errors")
        
        return stats
    
    def _generate_sync_summary(self, changes: List[OrgUnitChange]) -> Dict[str, Any]:
        """
        Generate a human-readable summary of the synchronization.
        
        Args:
            changes: List of changes that were identified
            
        Returns:
            Dict[str, Any]: Summary dictionary with statistics and details
        """
        # Count changes by type
        counts = {
            "total": len(changes),
            "created": sum(1 for c in changes if c.change_type == "create"),
            "updated": sum(1 for c in changes if c.change_type == "update"),
            "deleted": sum(1 for c in changes if c.change_type == "delete")
        }
        
        # Generate summary text
        summary_text = f"Organizational structure synchronization completed with {counts['total']} changes: " \
                      f"{counts['created']} creations, {counts['updated']} updates, {counts['deleted']} deletions."
        
        # Prepare sample details for each change type (limited to 5 of each)
        details = {}
        
        if counts["created"] > 0:
            creations = [c for c in changes if c.change_type == "create"][:5]
            details["sample_creations"] = [
                f"'{c.title}' (ID: {c.staatskalender_id})" for c in creations
            ]
            if counts["created"] > 5:
                details["sample_creations"].append(f"... and {counts['created'] - 5} more")
        
        if counts["updated"] > 0:
            updates = [c for c in changes if c.change_type == "update"][:5]
            details["sample_updates"] = []
            for c in updates:
                change_fields = list(c.details.get("changes", {}).keys())
                details["sample_updates"].append(
                    f"'{c.title}' (ID: {c.staatskalender_id}) - changed: {', '.join(change_fields)}"
                )
            if counts["updated"] > 5:
                details["sample_updates"].append(f"... and {counts['updated'] - 5} more")
        
        if counts["deleted"] > 0:
            deletions = [c for c in changes if c.change_type == "delete"][:5]
            details["sample_deletions"] = [
                f"'{c.title}' (ID: {c.staatskalender_id})" for c in deletions
            ]
            if counts["deleted"] > 5:
                details["sample_deletions"].append(f"... and {counts['deleted'] - 5} more")
        
        # Create the full summary
        summary = {
            "status": "success" if len(changes) > 0 else "no_changes",
            "message": summary_text,
            "counts": counts,
            "details": details
        }
        
        # Log the summary
        logging.info(summary_text)
        for change_type in ["creations", "updates", "deletions"]:
            sample_key = f"sample_{change_type}"
            if sample_key in details:
                for item in details[sample_key]:
                    logging.info(f"  {change_type.rstrip('s').capitalize()}: {item}")
        
        return summary
