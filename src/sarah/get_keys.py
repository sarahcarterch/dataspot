from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get, requests_put

import json
import logging
from time import sleep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()

base_url = "https://bs.dataspot.io"
project_uuid = "bdb3fbbe-3ab0-4a08-bb9e-ed8c19b42924"
endpoint = f"/rest/test-sarah-1/projects/{project_uuid}"
url = f"{base_url}{endpoint}"

ogd_client = OGDClient()

projekt = ogd_client._get_asset(endpoint=endpoint)
print(projekt.keys())
