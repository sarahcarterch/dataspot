#!/usr/bin/env python
"""
Script to build the organization structure in Dataspot's DNK scheme.

This script fetches organization data from the ODS API and builds
the organization hierarchy in Dataspot using bulk upload.
"""

import logging
import argparse
from time import sleep

from src.clients.dnk_client import DNKClient
from src.ods_client import ODSClient


def build_organization_structure_in_dnk(validate_urls=False, max_batches=None, batch_size=100):
    """
    Build the organization structure in Dataspot's DNK scheme based on data from the ODS API.
    Uses the bulk upload approach.

    Args:
        validate_urls (bool): Whether to validate Staatskalender URLs (can be slow)
        max_batches (int, optional): Maximum number of batches to retrieve (set to None for all)
        batch_size (int): Number of records to retrieve in each API call

    Returns:
        dict: Response from the bulk upload operation
    """
    logging.info("Starting organization structure build...")

    # Initialize clients
    ods_client = ODSClient()
    dataspot_client = DNKClient()

    # Collection for organization data
    all_organizations = {"results": []}

    # Fetch organization data in batches
    logging.info("Fetching organization data from ODS API...")
    batch_count = 0
    total_retrieved = 0

    try:
        while True:
            # Get the next batch of organization data
            offset = batch_count * batch_size
            batch_data = ods_client.get_organization_data(limit=batch_size, offset=offset)

            # Check if we received any results
            batch_results = batch_data.get('results', [])
            num_results = len(batch_results)

            if num_results == 0:
                # No more results, break out of the loop
                break

            # Add the batch results to our collected data
            all_organizations['results'].extend(batch_results)
            total_retrieved += num_results

            logging.info(
                f"Retrieved batch {batch_count + 1} with {num_results} organizations (total: {total_retrieved})")

            # Check if we've reached our batch limit
            batch_count += 1
            if max_batches is not None and batch_count >= max_batches:
                logging.info(f"Reached the maximum number of batches ({max_batches})")
                break

            # Small delay to avoid overwhelming the API
            sleep(0.1)

        # Set the total count in the combined data
        all_organizations['total_count'] = batch_data.get('total_count', total_retrieved)
        logging.info(f"Total organizations retrieved: {total_retrieved} (out of {all_organizations['total_count']})")

        # Build the organization hierarchy in Dataspot using bulk upload
        logging.info(f"Building organization hierarchy in Dataspot using bulk upload (validate_urls={validate_urls})...")
        try:
            # Use the bulk upload method with URL validation feature flag
            upload_response = dataspot_client.build_organization_hierarchy_from_ods_bulk(
                all_organizations, 
                validate_urls=validate_urls
            )
            
            logging.info(f"Organization structure bulk upload complete. Response summary: {upload_response}")
            return upload_response
            
        except Exception as e:
            logging.error(f"Error building organization hierarchy: {str(e)}")
            logging.info("Organization structure build failed")
            return {"status": "error", "message": str(e)}

    except KeyboardInterrupt:
        logging.info("Operation was interrupted by user")
        return {"status": "interrupted", "message": "Operation interrupted by user"}
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return {"status": "error", "message": str(e)}


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Build organization hierarchy in Dataspot")
    parser.add_argument(
        "--validate-urls", 
        action="store_true", 
        help="Validate Staatskalender URLs (can be slow)"
    )
    parser.add_argument(
        "--max-batches", 
        type=int, 
        default=None, 
        help="Maximum number of batches to retrieve (None for all)"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=100, 
        help="Number of records to retrieve in each API call"
    )
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Run the build function
    try:
        logging.info(f"Starting organization hierarchy build script...")
        result = build_organization_structure_in_dnk(
            validate_urls=args.validate_urls,
            max_batches=args.max_batches,
            batch_size=args.batch_size
        )
        
        # Print final result summary
        if result and result.get("status") == "success":
            logging.info("Organization hierarchy build completed successfully!")
            logging.info(f"Processed {result.get('levels_processed', 0)} organization levels")
        elif result and result.get("status") == "partial":
            logging.warning("Organization hierarchy build completed with errors")
            logging.warning(f"Total errors: {result.get('total_errors', 0)}")
            logging.warning(f"Processed {result.get('levels_processed', 0)} organization levels")
            if "errors" in result:
                for error in result.get("errors", [])[:5]:
                    logging.warning(f"Error: {error}")
        else:
            logging.error("Organization hierarchy build failed")
            if result and "message" in result:
                logging.error(f"Error message: {result.get('message')}")
                
        logging.info("Job completed!")
        
    except Exception as e:
        logging.error(f"Unhandled exception: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())