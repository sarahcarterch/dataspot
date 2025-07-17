import json
import logging
from dotenv import load_dotenv

from src.dataspot_auth import DataspotAuth
from src.common import requests_patch, requests_get

# Optionales Debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

API_BASE = "https://bs.dataspot.io/rest/test-sarah-1"

# 🔑 Projekt-ID und Zielstatus hier eintragen:
project_id = "12178ec7-3e63-412b-9015-78554401c20d"
new_status = "REVIEW3DS"  # oder z. B. "PLANNED", "DONE"

url = f"{API_BASE}/projects/{project_id}"
headers = DataspotAuth().get_headers()
payload = {"status": new_status}

logger.info(f"PATCH {url} → {new_status}")
logger.info(f"Projekt-ID: {project_id}")
logger.info(f"URL: {url}")
logger.info(f"Payload: {json.dumps(payload)}")
logger.info(f"Headers: {headers}")

response = requests_get(url, headers=headers, verify=False)
print("PRINT:","Statuscode:", response.status_code)
print("PRINT:","Antworttext:", response.text)
logger.info(f"Statuscode: {response.status_code}")
logger.info(f"Antwort: {response.text}")

logger.info(f"Statuscode: {response.status_code}")
try:
    logger.info(f"Antwort: {response.json()}")
except Exception:
    logger.info(f"Antworttext: {response.text}")
