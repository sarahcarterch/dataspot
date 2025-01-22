from dataspot_auth import DataspotAuth
from src.common import requests_get
import json

class DataspotClient:
    """Client for interacting with the Dataspot API."""
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.auth = DataspotAuth()
    
    def download_from_dataspot(self, relative_path, params: dict[str, str] = None):
        """
        Download data from Dataspot API.
        
        Args:
            relative_path (str): The relative path for the API endpoint including query parameters
            params (dict[str, str]): The parameters that should be passed in the url, i.e. everything after the ? in the url
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the request fails
            json.JSONDecodeError: If the response is not valid JSON
        """
        endpoint = f"{self.base_url}{relative_path}"
        headers = self.auth.get_headers()
        
        response = requests_get(endpoint, headers=headers, params=params)
        
        try:
            return response.json()
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to decode JSON response from {endpoint}: {str(e)}", 
                e.doc, 
                e.pos
            )
