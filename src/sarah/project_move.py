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
from src.common import requests_get, requests_put, requests_patch, requests_post

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

base_url = "https://bs.dataspot.io"
base_path = "/rest/test-sarah-1/"
project_uuid = "projects/bd4aa6b8-09e1-4348-99a7-bf6cda88e725"
endpoint = f"/rest/test-sarah-1/{project_uuid}"
url = f"{base_url}{endpoint}"

new_parent_uuid = "8accbced-0622-403f-a7b7-e4553407be10"

def main():
# Aktuelle Projektdaten holen (optional)
    # projekt = ogd_client._get_asset(endpoint=endpoint)
    data = {
        "_type": "Project",
        "subprojectOf": None,
        "inCollection": new_parent_uuid
    }
# subprojektOf-Feld entfernen
    #if "subprojectOf" in projekt:
        #del projekt["subprojectOf"]
# Neuen Eltern-Knoten setzen
    #projekt["parentId"] = new_parent_uuid
# Status auf DRAFT setzen
    #projekt["inCollection"] = new_parent_uuid
    #projekt["_links"]["inCollection"] = {
        #"href": f"{base_path}collection/{new_parent_uuid}"}
    #projekt["_links"]["attributedTo"] = {
        #"href": f"{base_path}{project_uuid}/attributedTo"}
    #del projekt["_links"]["subprojectOf"]

# Eingabe überprüfen
    print(json.dumps(data, indent=2))
# Projekt mit PATCH updaten
    response = requests_patch(
    url,
    headers=auth.get_headers(),
    json=data
    )
    print(response.status_code, response.json())

    projekt_neu = ogd_client._get_asset(endpoint=endpoint)
    print(projekt_neu.get("parentId"))

if __name__ == "__main__":
    main()
