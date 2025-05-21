import logging
from time import sleep
import json
import os
import datetime

from src.ods_client import ODSClient
from src.clients.dnk_client import DNKClient
from src.dataspot_dataset import OGDDataset
import ods_utils_py as ods_utils

from src.dataset_transformer import transform_ods_to_dnk


def main_X():
    dataspot_client = DNKClient()
    ods_client = ODSClient()

    #ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
    #ods_ids = ods_ids[:10]
    #ods_ids.append('100034') # Includes a ,
    #ods_ids.append('100236') # Includes a /
    ods_ids = ['100003', '100034', '100236']

    for ods_id in ods_ids:
        logging.info(f"Processing dataset {ods_id}...")

        ods_metadata = ods_utils.get_dataset_metadata(dataset_id = ods_id)
        dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)

        #dataspot_client. ...
    pass

def main_1_test_require_scheme_exists():
    dataspot_client = DNKClient()

    dataspot_client.require_scheme_exists()

def main_2_test_ensure_ods_imports_collection_exists():
    dataspot_client = DNKClient()
    ods_imports_collection = dataspot_client.ensure_ods_imports_collection_exists()
    print(f"ODS-Imports collection: {ods_imports_collection}")

def main_3_test_create_or_update_dataset():
    """
    Test the create_or_update_dataset method of the DNKClient.
    
    This function:
    1. Creates a test OGDDataset
    2. Attempts to create it in Dataspot
    3. Modifies the dataset and updates it
    4. Tests different update strategies
    5. Deletes the dataset
    
    The results can be manually verified in the Dataspot UI.
    """
    logging.info("Testing create_or_update_dataset method...")
    
    
    # Initialize client
    dataspot_client = DNKClient()

    ods_ids = ['100003', '100034', '100236']

    # Create a test dataset
    test_dataset = OGDDataset(
        name="Test Dataset",
        beschreibung="This dataset was created to test the DNKClient's create_or_update_dataset method",
        schluesselwoerter=["test", "dataset", "api"],
        datenportal_identifikation="test-dataset-001",  # Required for ODS ID
        # Add required custom properties for OGD stereotype
        aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
        tags=["test", "api", "documentation"]
    )
    
    # Test 1: Create a new dataset
    logging.info("Test 1: Creating a new dataset...")
    create_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='create_only'
    )
    logging.info(f"Dataset created with UUID: {create_response.get('id')}")
    logging.info(f"Dataset href: {create_response.get('href')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 2: Update the dataset
    logging.info("Test 2: Updating the dataset...")
    test_dataset.beschreibung = "Updated test dataset description"
    test_dataset.tags = ["updated", "test"]
    
    update_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='update_only'
    )
    logging.info(f"Dataset updated with UUID: {update_response.get('id')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 3: Create or update with force_replace
    logging.info("Test 3: Create or update with complete replacement...")
    test_dataset.beschreibung = "Complete replacement of the dataset"
    
    replace_response = dataspot_client.create_or_update_dataset(
        dataset=test_dataset,
        update_strategy='create_or_update',
        force_replace=True
    )
    logging.info(f"Dataset replaced with UUID: {replace_response.get('id')}")
    
    # Wait briefly to allow the server to process
    sleep(1)
    
    # Test 4: Delete the dataset
    logging.info("Test 4: Deleting the dataset...")
    delete_success = dataspot_client.delete_dataset(
        ods_id=test_dataset.datenportal_identifikation,
        fail_if_not_exists=True
    )
    logging.info(f"Dataset deletion successful: {delete_success}")
    
    logging.info("Tests completed. Check the Dataspot UI to verify all operations were performed correctly.")
    
    # Return the dataset ID for potential manual cleanup
    return test_dataset.datenportal_identifikation

def main_4_test_bulk_create_dataset():
    """
    Test the bulk_create_dataset method of the DNKClient.
    
    This function:
    1. Creates multiple test datasets
    2. Uploads them in a bulk operation to the scheme level, placing them in the ODS-Imports collection
    3. Tests different options (operations, dry run)
    4. Cleans up by deleting the datasets
    
    The results can be manually verified in the Dataspot UI.
    """
    logging.info("Testing bulk_create_dataset method...")
    
    # Initialize client
    dataspot_client = DNKClient()
    
    # Create test datasets
    test_datasets = []
    dataset_ids = []
    
    # Create 5 test datasets
    for i in range(1, 6):
        dataset_id = f"test-bulk-dataset-{i:03d}"
        dataset_ids.append(dataset_id)
        
        test_dataset = OGDDataset(
            name=f"Test Bulk Dataset {i}",
            beschreibung=f"This is test dataset #{i} for bulk upload testing",
            schluesselwoerter=["test", "bulk", "api"],
            datenportal_identifikation=dataset_id,  # Required for ODS ID
            # Add required custom properties for OGD stereotype
            aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
            tags=["test", "bulk", f"dataset-{i}"]
        )
        test_datasets.append(test_dataset)
    
    try:
        # Test 1: Dry run with operation (ADD)
        logging.info("Test 1: Performing dry run with operation (ADD)...")
        dry_run_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=True
        )
        logging.info(f"Dry run completed. Response: {dry_run_response}")
        
        # Wait briefly to allow the server to process
        sleep(2)
        
        # Test 2: Actual creation with operation (ADD)
        logging.info("Test 2: Bulk creating datasets with operation (ADD)...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk creation completed. Response summary: {create_response}")
        
        # Wait briefly to allow the server to process
        sleep(2)
        
        # Test 3: Update with operation (REPLACE)
        # Modify some dataset properties
        for dataset in test_datasets:
            dataset.beschreibung = f"{dataset.beschreibung} - UPDATED"
            dataset.tags.append("updated")
        
        logging.info("Test 3: Updating datasets with operation (REPLACE)...")
        update_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="REPLACE",
            dry_run=False
        )
        logging.info(f"Bulk update completed. Response summary: {update_response}")
        
    finally:
        # Clean up: Delete all test datasets
        logging.info("Cleaning up: Deleting test datasets...")
        for dataset_id in dataset_ids:
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
            except Exception as e:
                logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")
        
    logging.info("Bulk dataset tests completed. Check the Dataspot UI to verify all operations were performed correctly.")
    
    return dataset_ids

def main_5_test_mapping_update():
    """
    Test the ability to update mappings after bulk dataset upload.

    This function:
    1. Creates multiple test datasets
    2. Uploads them using the bulk_create_dataset method
    3. Verifies that mappings are correctly updated
    4. Cleans up by deleting the test datasets
    """
    logging.info("Testing mapping update after bulk upload...")

    # Initialize client
    dataspot_client = DNKClient()

    # Create test datasets
    test_datasets = []
    dataset_ids = []

    # Create 3 test datasets with unique IDs
    for i in range(1, 4):
        dataset_id = f"test-mapping-{i:03d}"
        dataset_ids.append(dataset_id)

        test_dataset = OGDDataset(
            name=f"Test Mapping Dataset {i}",
            beschreibung=f"This is test dataset #{i} for mapping update testing",
            schluesselwoerter=["test", "mapping", "api"],
            datenportal_identifikation=dataset_id,  # Required for ODS ID
            aktualisierungszyklus="http://publications.europa.eu/resource/authority/frequency/DAILY",
            tags=["test", "mapping", f"dataset-{i}"]
        )
        test_datasets.append(test_dataset)

    try:
        # Upload the datasets in bulk
        logging.info("Creating datasets in bulk...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=test_datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk creation completed. Response: {create_response}")

        # Check if mappings were updated
        logging.info("Checking if mappings were updated...")
        mappings_updated = 0

        for dataset_id in dataset_ids:
            entry = dataspot_client.ods_dataset_mapping.get_entry(dataset_id)
            if entry:
                uuid, href = entry
                logging.info(f"Mapping for {dataset_id}: UUID={uuid}, href={href}")
                mappings_updated += 1
            else:
                logging.warning(f"No mapping found for dataset ID: {dataset_id}")

        logging.info(f"Found mappings for {mappings_updated} out of {len(dataset_ids)} datasets")

    finally:
        # Clean up: Delete all test datasets
        logging.info("Cleaning up: Deleting test datasets...")
        for dataset_id in dataset_ids:
            try:
                delete_success = dataspot_client.delete_dataset(
                    ods_id=dataset_id,
                    fail_if_not_exists=False
                )
                logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
            except Exception as e:
                logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")

    logging.info("Mapping update test completed")
    return dataset_ids

def main_6_analyze_ods_metadata():
    """
    Analyzes and displays the structure of ODS metadata for a sample dataset.
    
    This function:
    1. Retrieves metadata for a specific dataset from ODS
    2. Prints the full metadata structure for analysis
    3. Tests the transform_ods_to_dnk function
    4. Displays the resulting OGDDataset
    """
    logging.info("Starting ODS metadata analysis...")

    dataset_id = '100397'
    
    # Get metadata from ODS
    logging.info(f"Retrieving metadata for dataset {dataset_id}...")
    ods_metadata = ods_utils.get_dataset_metadata(dataset_id=dataset_id)
    
    # Print the metadata structure
    logging.info("Full ODS metadata structure:")
    print(json.dumps(ods_metadata, indent=2))
    
    # Transform to Dataspot dataset
    logging.info("Transforming ODS metadata to Dataspot dataset...")
    dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=dataset_id)
    
    # Print the resulting dataset
    logging.info("Resulting OGDDataset:")
    print(f"Dataset name: {dataset.name}")
    print(f"Beschreibung: {dataset.beschreibung}")
    print(f"Schlüsselwörter: {dataset.schluesselwoerter}")
    print(f"Aktualisierungszyklus: {dataset.aktualisierungszyklus}")
    print(f"Publikationsdatum: {dataset.publikationsdatum}")
    print(f"Geographische Dimension: {dataset.geographische_dimension}")
    print(f"Datenportal-ID: {dataset.datenportal_identifikation}")
    print(f"Tags: {dataset.tags}")
    
    # Convert to JSON representation
    logging.info("Dataset JSON representation:")
    dataset_json = dataset.to_json()
    print(json.dumps(dataset_json, indent=2))
    
    logging.info("ODS metadata analysis completed")
    return dataset

def main_7_test_bulk_few_ods_datasets_upload(cleanup_after_test=True):
    """
    Test the bulk upload of specific ODS datasets to Dataspot.
    
    This function:
    1. Retrieves metadata for specific ODS dataset IDs
    2. Transforms each dataset using transform_ods_to_dnk
    3. Creates a list of transformed datasets
    4. Calls bulk_create_or_update_datasets with this list
    5. Includes sleep statements for manual verification
    6. Cleans up datasets at the end if cleanup_after_test is True

    Args:
        cleanup_after_test (bool, optional): Whether to delete datasets after testing. Defaults to True.
    
    Returns:
        List[str]: The list of dataset IDs that were uploaded
    """
    logging.info("Testing bulk upload of specific ODS datasets...")
    
    # Initialize clients
    dataspot_client = DNKClient()
    
    # Specific ODS dataset IDs to test
    ods_ids = ['100003', '100034', '100236', '100397']
    datasets = []
    
    # Retrieve and transform each dataset
    logging.info(f"Retrieving and transforming {len(ods_ids)} datasets...")
    for ods_id in ods_ids:
        try:
            logging.info(f"Processing dataset {ods_id}...")
            
            # Get metadata from ODS
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            
            # Transform to Dataspot dataset
            dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
            
            # Add to list
            datasets.append(dataset)
            logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
            
        except Exception as e:
            logging.error(f"Error processing dataset {ods_id}: {str(e)}")
    
    if not datasets:
        logging.error("No datasets were successfully transformed. Aborting test.")
        return []
    
    dataset_ids = [dataset.datenportal_identifikation for dataset in datasets]
    logging.info(f"Successfully transformed {len(datasets)} datasets: {dataset_ids}")
    
    try:
        # Test 1: Dry run to check for any issues
        logging.info("Performing dry run of bulk upload...")
        dry_run_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="ADD",
            dry_run=True
        )
        logging.info(f"Dry run completed. Response: {dry_run_response}")
        
        # Wait for manual verification
        sleep(2)
        
        # Test 2: Actual creation with operation (ADD)
        logging.info("Bulk uploading datasets...")
        create_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="ADD",
            dry_run=False
        )
        logging.info(f"Bulk upload completed. Response summary: {create_response}")
        
        # Wait for manual verification
        sleep(3)
        
        # Test 3: Update with operation (REPLACE)
        # Modify some dataset properties for testing updates
        for dataset in datasets:
            dataset.beschreibung = f"{dataset.beschreibung} - UPDATED VIA BULK API"
            if dataset.tags:
                dataset.tags.append("bulk-updated")
            else:
                dataset.tags = ["bulk-updated"]
        
        logging.info("Updating datasets with operation (REPLACE)...")
        update_response = dataspot_client.bulk_create_or_update_datasets(
            datasets=datasets,
            operation="REPLACE",
            dry_run=False
        )
        logging.info(f"Bulk update completed. Response summary: {update_response}")
        
        # Wait for manual verification
        sleep(3)
        
        return dataset_ids
        
    finally:
        if cleanup_after_test:
            # Clean up: Delete all test datasets
            logging.info("Cleaning up: Deleting uploaded datasets...")
            for dataset_id in dataset_ids:
                try:
                    delete_success = dataspot_client.delete_dataset(
                        ods_id=dataset_id,
                        fail_if_not_exists=False
                    )
                    logging.info(f"Deleted dataset {dataset_id}: {delete_success}")
                except Exception as e:
                    logging.warning(f"Failed to delete dataset {dataset_id}: {str(e)}")
            logging.info("Cleanup completed")

def main_8_test_bulk_ods_datasets_upload_and_delete(max_datasets: int = None):
    """
    Test the bulk upload of all public ODS datasets to Dataspot.
    
    This function:
    1. Retrieves all public dataset IDs from ODS
    2. Transforms each dataset using transform_ods_to_dnk
    3. Collects all datasets and uploads them at once
    4. Includes progress reporting with counters
    5. Provides options to limit the number of datasets processed

    Args:
        max_datasets (int, optional): Maximum number of datasets to process (set to None for all). Defaults to None.
    
    Returns:
        List[str]: The list of dataset IDs that were uploaded
    """
    logging.info("Testing bulk upload of public ODS datasets...")
    
    # Initialize clients
    dataspot_client = DNKClient()
    
    # Get all public dataset IDs
    logging.info(f"Step 1: Retrieving {max_datasets or 'all'} public dataset IDs from ODS...")
    ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False, max_datasets=max_datasets)

    logging.info(f"Found {len(ods_ids)} datasets to process")
    
    # Process all datasets
    logging.info(f"Step 2: Processing the retrieved dataset: Downloading and transforming the ODS datasets to dataspot "
                 f"datasets.")
    total_processed = 0
    total_successful = 0
    total_failed = 0
    processed_ids = []
    all_datasets = []
    
    # Process each dataset
    for idx, ods_id in enumerate(ods_ids):
        try:
            logging.info(f"[{idx+1}/{len(ods_ids)}] Processing dataset {ods_id}...")
            
            # Get metadata from ODS
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            
            # Transform to Dataspot dataset
            dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
            
            # Add to collection
            all_datasets.append(dataset)
            processed_ids.append(ods_id)
            
            logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
            total_successful += 1
            
        except Exception as e:
            logging.error(f"Error processing dataset {ods_id}: {str(e)}")
            total_failed += 1
        
        total_processed += 1

    logging.info(f"Completed processing {total_processed} datasets: {total_successful} successful, {total_failed} failed")

    # Upload all datasets at once if we have any
    if all_datasets:
        try:
            # Bulk create all datasets
            logging.info(f"Step 3: Bulk uploading all {len(all_datasets)} datasets to dataspot...") # Comment: This line is confusing, as it we do not upload right here and there, but before that there is more setup.

            dataspot_client.ensure_ods_imports_collection_exists()

            creation_summary = dataspot_client.sync_datasets(datasets=all_datasets)

            logging.info(f"Bulk upload completed. Response summary: {creation_summary}")
            
        except Exception as e:
            logging.error(f"Error during bulk upload: {str(e)}")

    return processed_ids

def main_10_sync_organization_structure():
    """
    Synchronize organizational structure in Dataspot with the latest data from ODS API.
    This method:
    1. Retrieves organization data from the ODS API
    2. Validates that no duplicate id_im_staatskalender values exist in ODS data (throws an error if duplicates are found)
    3. Fetches existing organizational units from Dataspot 
    4. Validates that no duplicate id_im_staatskalender values exist in Dataspot (throws an error if duplicates are found)
    5. Compares with existing organization data in Dataspot
    6. Updates only the changed organizations
    7. Provides a summary of changes
    """
    logging.info("Starting organization structure synchronization...")

    # Initialize clients
    ods_client = ODSClient()
    dataspot_client = DNKClient()

    # Fetch organization data
    logging.info("Fetching organization data from ODS API...")
    all_organizations = ods_client.get_all_organization_data(batch_size=100)
    logging.info(f"Total organizations retrieved: {len(all_organizations['results'])} (out of {all_organizations['total_count']})")

    # Synchronize organization data
    logging.info("Synchronizing organization data with Dataspot...")
    try:
        # Use the sync method with URL validation feature flag
        sync_result = dataspot_client.sync_org_units(
            all_organizations
        )
        
        # Get the base URL and database name for asset links
        base_url = dataspot_client.base_url
        database_name = dataspot_client.database_name
        
        # Display sync results
        logging.info(f"Synchronization {sync_result['status']}!")
        logging.info(f"Message: {sync_result['message']}")
        
        # Display details if available
        if 'counts' in sync_result:
            counts = sync_result['counts']
            logging.info(f"Changes: {counts['total']} total - {counts['created']} created, "
                         f"{counts['updated']} updated, {counts['deleted']} deleted")
        
        # Show detailed information for each change type
        details = sync_result.get('details', {})

        # Process creations
        if 'creations' in details:
            creations = details['creations'].get('items', [])
            logging.info(f"\n=== CREATED UNITS ({len(creations)}) ===")
            for i, creation in enumerate(creations, 1):
                title = creation.get('title', '(Unknown)')
                staatskalender_id = creation.get('staatskalender_id', '(Unknown)')
                uuid = creation.get('uuid', '')  # UUID might be missing for newly created items
                
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id})")
                
                # Show asset link if UUID is available
                if uuid:
                    asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
                    logging.info(f"   - Link: {asset_link}")
                
                # Show properties
                props = creation.get('properties', {})
                if props:
                    for key, value in props.items():
                        if value:  # Only show non-empty values
                            logging.info(f"   - {key}: '{value}'")

        # Process updates - show field changes with old and new values
        if 'updates' in details:
            updates = details['updates'].get('items', [])
            logging.info(f"\n=== UPDATED UNITS ({len(updates)}) ===")
            for i, update in enumerate(updates, 1):
                title = update.get('title', '(Unknown)')
                staatskalender_id = update.get('staatskalender_id', '(Unknown)')
                uuid = update.get('uuid', '(Unknown)')

                # Create asset link
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"

                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, UUID: {uuid})")
                logging.info(f"   - Link: {asset_link}")

                # Show each changed field
                for field_name, changes in update.get('changed_fields', {}).items():
                    old_value = changes.get('old_value', '')
                    new_value = changes.get('new_value', '')
                    logging.info(f"   - {field_name}: '{old_value}' → '{new_value}'")

        # Process deletions
        if 'deletions' in details:
            deletions = details['deletions'].get('items', [])
            logging.info(f"\n=== DELETED UNITS ({len(deletions)}) ===")
            for i, deletion in enumerate(deletions, 1):
                title = deletion.get('title', '(Unknown)')
                staatskalender_id = deletion.get('staatskalender_id', '(Unknown)')
                uuid = deletion.get('uuid', '(Unknown)')
                
                # Create asset link
                asset_link = f"{base_url}/web/{database_name}/collections/{uuid}"
                
                logging.info(f"{i}. '{title}' (ID: {staatskalender_id}, UUID: {uuid})")
                logging.info(f"   - Link: {asset_link}")
                logging.info(f"   - Path: '{deletion.get('inCollection', '')}'")
        
        # Write detailed report to file for email/reference purposes
        try:
            # Get project root directory (one level up from src)
            current_file_path = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(current_file_path))
            
            # Define reports directory in project root
            reports_dir = os.path.join(project_root, "reports")
            
            # Create reports directory if it doesn't exist
            os.makedirs(reports_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(reports_dir, f"org_sync_report_{timestamp}.json")
            
            # Write report to file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(sync_result, f, indent=2, ensure_ascii=False)
                
            logging.info(f"\nDetailed report saved to {filename}")
        except Exception as e:
            logging.error(f"Failed to save detailed report to file: {str(e)}")
        
    except ValueError as e:
        if "Duplicate id_im_staatskalender values detected in Dataspot" in str(e):
            logging.error("============================================================")
            logging.error("ERROR: SYNCHRONIZATION ABORTED - DUPLICATE IDs IN DATASPOT")
            logging.error("------------------------------------------------------------")
            logging.error(str(e))
            logging.error("------------------------------------------------------------")
            logging.error("Please fix the duplicate IDs in Dataspot before continuing.")
            logging.error("You may need to manually delete one of the duplicate collections.")
            logging.error("============================================================")
            return  # Exit the function
        elif "Duplicate id_im_staatskalender values detected" in str(e):
            logging.error("============================================================")
            logging.error("ERROR: SYNCHRONIZATION ABORTED - DUPLICATE IDs IN ODS DATA")
            logging.error("------------------------------------------------------------")
            logging.error(str(e))
            logging.error("------------------------------------------------------------")
            logging.error("Please fix the duplicate IDs in the ODS source data before continuing.")
            logging.error("============================================================")
            return  # Exit the function
        else:
            # Re-raise other ValueError exceptions
            raise
    except Exception as e:
        logging.error(f"Error synchronizing organization structure: {str(e)}")
        
    logging.info("Organization structure synchronization process finished")
    logging.info("===============================================")

def main_11_sync_datasets(max_datasets: int = None):
    """
    Sync existing datasets with updated titles without creating duplicates.
    
    This function:
    1. Retrieves public dataset IDs from ODS
    2. For each dataset, retrieves metadata and transforms it (which adds "(OGD)" to titles)
    3. Uses the sync_datasets method that's been modified to properly update existing datasets
    
    Args:
        max_datasets (int, optional): Maximum number of datasets to process. Defaults to None.
    
    Returns:
        List[str]: The list of dataset IDs that were processed
    """
    logging.info("Syncing existing datasets with updated titles...")
    
    # Initialize client
    dataspot_client = DNKClient()
    
    # Get all public dataset IDs
    logging.info(f"Step 1: Retrieving {max_datasets or 'all'} public dataset IDs from ODS...")
    ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False, max_datasets=max_datasets)
    logging.info(f"Found {len(ods_ids)} datasets to process")
    
    # Process datasets
    logging.info("Step 2: Processing datasets - downloading metadata and transforming...")
    total_processed = 0
    total_successful = 0
    total_failed = 0
    processed_ids = []
    all_datasets = []
    
    for idx, ods_id in enumerate(ods_ids):
        try:
            logging.info(f"[{idx+1}/{len(ods_ids)}] Processing dataset {ods_id}...")
            
            # Get metadata from ODS
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            
            # Transform to Dataspot dataset
            dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
            
            # Add to collection
            all_datasets.append(dataset)
            processed_ids.append(ods_id)
            
            logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
            total_successful += 1
            
        except Exception as e:
            logging.error(f"Error processing dataset {ods_id}: {str(e)}")
            total_failed += 1
        
        total_processed += 1
        
        # Process in smaller batches to avoid memory issues
        if len(all_datasets) >= 50 or idx == len(ods_ids) - 1:
            if all_datasets:
                try:
                    logging.info(f"Step 3: Syncing batch of {len(all_datasets)} datasets...")
                    
                    # Ensure ODS-Imports collection exists
                    dataspot_client.ensure_ods_imports_collection_exists()
                    
                    # Sync datasets - the method has been modified to handle updates properly
                    sync_summary = dataspot_client.sync_datasets(datasets=all_datasets)
                    
                    logging.info(f"Batch sync completed. Response summary: {sync_summary}")
                    
                    # Clear the batch for the next iteration
                    all_datasets = []
                    
                except Exception as e:
                    logging.error(f"Error during batch sync: {str(e)}")

    logging.info(f"Completed processing {total_processed} datasets: {total_successful} successful, {total_failed} failed")
    return processed_ids


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f'Executing {__file__}...')

    import config
    logging.info(f"Running script on database: {config.database_name}")

    if config.database_name == config.database_name_prod:
        answer = input("Are you sure you want to run this script in the prod environment (y/[n])? ")
        if answer != 'y':
            exit("Aborting run...")

    #main_10_sync_organization_structure()
    main_11_sync_datasets(max_datasets=None)

    logging.info('Job successful!')
