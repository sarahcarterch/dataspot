import os
from dotenv import load_dotenv
from dataspot_client import DataspotClient
import json

def main():
    load_dotenv()
    
    # Get base URL from environment variables
    base_url = os.getenv("DATASPOT_API_BASE_URL")
    if not base_url:
        raise ValueError("DATASPOT_API_BASE_URL environment variable is not set")
    
    # Initialize client
    client = DataspotClient(base_url)

    # Settings for download
    relative_path = '/datasets/e190b4a9-5b56-4ad0-ad04-82b990c91fb6?language=de&format=json'

    # Debug: Print headers and URL
    headers = client.auth.get_headers()
    full_url = f"{base_url}{relative_path}"
    print("Making request to:", full_url)
    print("Headers:", {k: v if k != 'Authorization' else 'Bearer [REDACTED]' for k, v in headers.items()})

    # Test download
    try:
        data = client.download_from_dataspot(relative_path)
        print("Successfully downloaded data:")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
    