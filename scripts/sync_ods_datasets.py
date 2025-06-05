import logging
import json
import os
import datetime

import config
from src.clients.dnk_client import DNKClient
from src.common import email_helpers as email_helpers
import ods_utils_py as ods_utils
from src.dataset_transformer import transform_ods_to_dnk


def main():
    logging.info(f"Running sync_ods_datasets on {config.database_name}")
    sync_ods_datasets()


def sync_ods_datasets(max_datasets: int = None, batch_size: int = 50):
    """
    Synchronize ODS datasets with Dataspot using DNKClient.
    
    This method:
    1. Creates a DNKClient instance for ODS dataset synchronization
    2. Retrieves public dataset IDs from ODS
    3. For each dataset, retrieves metadata and transforms it
    4. Processes datasets in batches to avoid memory issues
    5. Uses the sync_datasets method to properly update existing datasets
    6. Provides a summary of changes
    
    Args:
        max_datasets (int, optional): Maximum number of datasets to process. Defaults to None (all datasets).
        batch_size (int, optional): Number of datasets to process in each batch. Defaults to 50.
    """
    logging.info("Starting ODS datasets synchronization...")

    # Initialize Dataspot client
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
    
    # Store sync results for reporting
    sync_results = {
        'status': 'pending',
        'message': '',
        'counts': {
            'total': 0,
            'created': 0,
            'updated': 0,
            'deleted': 0,
            'errors': 0,
            'processed': 0
        },
        'details': {
            'creations': {
                'count': 0,
                'items': []
            },
            'updates': {
                'count': 0,
                'items': []
            },
            'deletions': {
                'count': 0,
                'items': []
            },
            'errors': {
                'count': 0,
                'items': []
            }
        },
        '_batches': [] # Internal tracking, not part of final report
    }
    
    for idx, ods_id in enumerate(ods_ids):
        logging.info(f"[{idx+1}/{len(ods_ids)}] Processing dataset {ods_id}...")
        
        # Get metadata from ODS and transform to Dataspot dataset
        # Following fail-fast principle - no try/catch here to make debugging easier
        ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
        dataset = transform_ods_to_dnk(ods_metadata=ods_metadata, ods_dataset_id=ods_id)
        
        # Add to collection
        all_datasets.append(dataset)
        processed_ids.append(ods_id)
        
        logging.info(f"Successfully transformed dataset {ods_id}: {dataset.name}")
        total_successful += 1
        total_processed += 1
        
        # Process in smaller batches to avoid memory issues
        if len(all_datasets) >= batch_size or idx == len(ods_ids) - 1:
            if all_datasets:
                batch_num = len(sync_results['_batches']) + 1
                logging.info(f"Step 3: Syncing batch {batch_num} of {len(all_datasets)} datasets...")
                
                # Ensure ODS-Imports collection exists
                dataspot_client.ensure_ods_imports_collection_exists()
                
                # Sync datasets - the method handles updates properly
                # Following fail-fast principle - no try/catch here to make debugging easier
                sync_summary = dataspot_client.sync_datasets(datasets=all_datasets)
                
                logging.info(f"Batch sync completed. Response summary: {sync_summary}")
                
                # Store batch results for reporting
                sync_results['_batches'].append({
                    'batch_number': batch_num,
                    'batch_size': len(all_datasets),
                    'summary': sync_summary
                })
                
                # Update overall counts
                sync_results['counts']['created'] += sync_summary.get('created', 0)
                sync_results['counts']['updated'] += sync_summary.get('updated', 0)
                sync_results['counts']['deleted'] += sync_summary.get('deleted', 0)
                sync_results['counts']['errors'] += sync_summary.get('errors', 0)
                sync_results['counts']['total'] += (
                    sync_summary.get('created', 0) + 
                    sync_summary.get('updated', 0) + 
                    sync_summary.get('deleted', 0)
                )
                
                # Clear the batch for the next iteration
                all_datasets = []

    # Update final report status and message
    sync_results['status'] = 'success'
    sync_results['message'] = (
        f"ODS datasets synchronization completed with {sync_results['counts']['total']} changes: "
        f"{sync_results['counts']['created']} created, {sync_results['counts']['updated']} updated, "
        f"{sync_results['counts']['deleted']} deleted."
    )
    
    # Update final counts (processed may differ from total changes)
    sync_results['counts']['processed'] = total_processed
    
    # Log final summary
    logging.info(f"Completed processing {total_processed} datasets: {total_successful} successful, {total_failed} failed")
    
    # Write detailed report to file for email/reference purposes
    # Following fail-fast principle - no try/catch here to make debugging easier
    # Get project root directory (one level up from scripts)
    current_file_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_file_path))
    
    # Define reports directory in project root
    reports_dir = os.path.join(project_root, "reports")
    
    # Create reports directory if it doesn't exist
    os.makedirs(reports_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(reports_dir, f"ods_datasets_sync_report_{timestamp}.json")
    
    # Write report to file
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(sync_results, f, indent=2, ensure_ascii=False)
        
    logging.info(f"\nDetailed report saved to {report_filename}")
    
    # Create email content
    email_subject, email_content, should_send = create_email_content(
        sync_results=sync_results,
        scheme_name_short=dataspot_client.database_name
    )
    
    # Print a report to the logs, similar to the email content
    if total_processed > 0:
        logging.info("===== ODS DATASETS SYNC SUMMARY =====")
        logging.info(f"Status: {sync_results['status']}")
        logging.info(f"Message: {sync_results['message']}")
        logging.info(f"Database: {dataspot_client.database_name}")
        
        # Show counts
        counts = sync_results['counts']
        logging.info(f"Changes: {counts['total']} total - {counts['created']} created, "
                    f"{counts['updated']} updated, {counts['deleted']} deleted")
        logging.info(f"Total datasets processed: {counts['processed']}")
        
        # Log summary of batch processing
        if sync_results['_batches']:
            logging.info(f"Batches processed: {len(sync_results['_batches'])}")
            
            for batch in sync_results['_batches']:
                batch_number = batch['batch_number']
                batch_size = batch['batch_size']
                summary = batch['summary']
                
                created = summary.get('created', 0)
                updated = summary.get('updated', 0)
                deleted = summary.get('deleted', 0)
                errors = summary.get('errors', 0)
                
                if created > 0 or updated > 0 or deleted > 0 or errors > 0:
                    logging.info(f"Batch {batch_number} ({batch_size} datasets): "
                                f"{created} created, {updated} updated, {deleted} deleted, {errors} errors")
        
        logging.info("====================================")
    
    # Clean up the final report to match org_sync_report format
    # Remove the internal _batches tracking before writing to file
    if '_batches' in sync_results:
        del sync_results['_batches']
    
    # Send email if there were datasets processed
    if should_send:
        try:
            # Create and send email
            attachment = report_filename if os.path.exists(report_filename) else None
            msg = email_helpers.create_email_msg(
                subject=email_subject,
                text=email_content,
                attachment=attachment
            )
            email_helpers.send_email(msg)
            logging.info("Email notification sent successfully")
        except Exception as e:
            # Log error but continue execution
            logging.error(f"Failed to send email notification: {str(e)}")
            logging.info("Continuing execution despite email failure")
    else:
        logging.info("No datasets were processed - email notification not sent")
    
    logging.info("ODS datasets synchronization process finished")
    logging.info("===============================================")
    
    return processed_ids


def create_email_content(sync_results, scheme_name_short):
    """
    Create email content based on synchronization results.

    Args:
        sync_results (dict): Synchronization result data
        scheme_name_short (str): Short name of the scheme (database name)

    Returns:
        tuple: (email_subject, email_text, should_send)
    """
    counts = sync_results['counts']
    total_changes = counts['total']
    
    # Only create email if there were changes
    if total_changes == 0:
        return None, None, False
    
    # Create email subject with summary following the requested format
    email_subject = f"[{scheme_name_short}] ODS Datasets: {counts['created']} created, {counts['updated']} updated, {counts['deleted']} deleted"
    
    email_text = f"Hi there,\n\n"
    email_text += f"I've just synchronized ODS datasets with Dataspot.\n"
    email_text += f"Here's a summary of the synchronization:\n\n"
    
    # Add summary counts
    email_text += f"Changes: {counts['total']} total\n"
    email_text += f"- Created: {counts['created']} datasets\n"
    email_text += f"- Updated: {counts['updated']} datasets\n"
    email_text += f"- Deleted: {counts['deleted']} datasets\n"
    if counts.get('errors', 0) > 0:
        email_text += f"- Errors: {counts['errors']}\n"
    email_text += f"\nTotal datasets processed: {counts['processed']}\n\n"
    
    # Add a note about batches but don't include detailed batch information
    # as we're moving to a more consolidated report like org_sync_report
    if '_batches' in sync_results and sync_results['_batches']:
        email_text += f"The synchronization was performed in {len(sync_results['_batches'])} batches.\n\n"
    
    email_text += "Please review the synchronization results in Dataspot.\n\n"
    email_text += "Best regards,\n"
    email_text += "Your Dataspot ODS Datasets Sync Assistant"
    
    return email_subject, email_text, True


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:[%(filename)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    logging.info(f'Executing {__file__}...')
    main() 