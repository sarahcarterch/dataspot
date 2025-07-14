"""
Script to .python ..
"""
import os
import logging
import argparse
from time import sleep
import requests

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get, requests_put, requests_patch, requests_post, requests_delete

from dotenv import load_dotenv

from src.sarah.uuid import *

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

endpoint = f"{base_path}{working_project_uuid}"
url = f"{base_url}{endpoint}"

def main():
    logger.info(f"Sende DELETE-Request an: {url}")
    # Projekt mit DELETE löschen
    response = requests_delete(
    url,
    headers=auth.get_headers(),
    verify=False
    )

    if response.status_code == 204:
        logger.info("Projekt erfolgreich gelöscht.")
    elif response.status_code == 404:
        logger.warning("Projekt nicht gefunden.")
    else:
        logger.error(f"Fehler beim Löschen: {response.status_code} – {response.text}")

if __name__ == "__main__":
    main()
