import logging

from src.common import requests_get

# This is deprecated code. Maybe we'll need this when updating the script that fills the staatskalender ods dataset,
# but I don't think this will be anytime soon...

def generate_potential_staatskalender_url(path: str) -> str:
    """
    Generate a URL for the Basel Staatskalender based on an organization path string.

    This function transforms an organization path into a standardized URL format
    by applying these transformations:
    - Convert to lowercase
    - Replace spaces with hyphens
    - Convert German umlauts (ö→oe, ä→ae, ü→ue)
    - Remove all characters except letters, hyphens, and forward slashes
    - Replace double hyphens with single ones
    - Remove trailing slashes

    Args:
        path (str): The organization path string to transform

    Returns:
        str: A formatted URL pointing to the organization in the Basel Staatskalender

    Example:
        >>> generate_potential_staatskalender_url("Präsidialdepartement/Kantons- und Stadtentwicklung")
        "https://staatskalender.bs.ch/organization/praesidialdepartement/kantons-und-stadtentwicklung"
    """
    # Make path string lowercase, and replace ö -> oe, ä -> ae, ü -> ue, é -> e, etc., and space -> '-', and remove trailing slashes
    new_path = path.lower()
    new_path = new_path.replace(' ', '-')
    new_path = new_path.replace('ö', 'oe')
    new_path = new_path.replace('ä', 'ae')
    new_path = new_path.replace('ü', 'ue')
    new_path = new_path.replace('é', 'e')
    new_path = new_path.replace('ê', 'e')
    new_path = new_path.replace('è', 'e')
    new_path = new_path.replace('à', 'a')
    # Keep only letters a-z and hyphens, remove all other characters
    new_path = ''.join(c for c in new_path if c.isalpha() or c in '-/')
    new_path = new_path.replace('--', '-')
    new_path = new_path.rstrip('/')
    return f"https://staatskalender.bs.ch/organization/{new_path}"


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


