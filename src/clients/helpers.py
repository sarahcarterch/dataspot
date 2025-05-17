"""
Dataspot API Client Helpers

This module provides utility functions and helpers for Dataspot API clients.
"""

import logging


def url_join(*parts: str, leading_slash: bool = False) -> str:
    """
    Join URL parts ensuring proper formatting with slashes.
    
    Args:
        *parts: URL parts to be joined.
        leading_slash: Whether to add a leading slash to the URL.
        
    Returns:
        str: A properly formatted URL.
    """
    if leading_slash:
        return '/' + url_join(*parts, leading_slash=False)

    return "/".join([part.strip("/") for part in parts])

def get_uuid_from_response(response: dict) -> str | None:
    """
    Extract UUID from a Dataspot API response.
    
    The UUID is at response['id'].
    
    Args:
        response (dict): The JSON response from Dataspot API
        
    Returns:
        str: UUID or None if not found
    """
    return response.get('id')

def escape_special_chars(name: str) -> str:
    '''
    Escape special characters in asset names for Dataspot API according to the business key rules. Also remove leading
    and trailing spaces.
    
    According to Dataspot documentation, special characters need to be properly escaped in business keys:
    
    1. If a name contains / or ., it should be enclosed in double quotes
       Example: INPUT/OUTPUT → "INPUT/OUTPUT"
       Example: dataspot. → "dataspot."
    
    2. If a name contains double quotes ("), each double quote should be doubled ("") and 
       the entire name should be enclosed in double quotes
       Example: 28" City Bike → "28"" City Bike"
       Example: Project "Zeus" → "Project ""Zeus"""
    
    Args:
        name (str): The name of the asset (dataset, organizational unit, etc.)
        
    Returns:
        str: The escaped name suitable for use in Dataspot API business keys
    '''
    
    if name is None:
        logging.warning(f"Trying to escape special characters for None")

    if not name:
        return name
    
    # Check if the name contains any characters that need special handling
    needs_quoting = False

    orig_name = name
    
    # Remove leading and trailing spaces
    name = name.strip()
    
    # Names containing '/' or '.' need to be quoted
    if '/' in name or '.' in name:
        needs_quoting = True
    

    # Names containing double quotes need special handling
    has_quotes = '"' in name
    if has_quotes:
        needs_quoting = True
        # Double each quote in the name
        name = "".join('""' if char == '"' else char for char in name)
    
    # Enclose the name in quotes if needed
    if needs_quoting:
        logging.debug(f"Escaped organization title from '{orig_name}' to '{name}'")
        return f'"{name}"'
    
    logging.debug(f"No need to escape special characters for '{orig_name}'")
    return name
