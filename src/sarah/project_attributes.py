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

from src.sarah.uuid import *

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

endpoint = f"/rest/test-sarah-1/attributions"
existing = ogd_client._get_asset(endpoint)

bereits_zugewiesen = set()
if existing and "_embedded" in existing and "attributedTo" in existing["_embedded"]:
    for eintrag in existing["_embedded"]["attributedTo"]:
        person = eintrag.get("attributedTo")
        rolle = eintrag.get("attributedAs")
        if person and rolle:
            bereits_zugewiesen.add((person, rolle))

url = f"{base_url}{endpoint}"

new_attributionFor = new_attributionFor # Projekt-ID kommt von uuid.py
new_type = "Attribution"     # "_type" (wird offenbar nicht gebraucht)

def main():
    rollenzuweisungen = [
        {
            "rollenname": "Data Owner",
            "rollen_uuid": data_owner,
            "person_uuid": lm,
        },
        {
            "rollenname": "Data Steward",
            "rollen_uuid": data_steward,
            "person_uuid": ug,
        }
    ]

    for eintrag in rollenzuweisungen:

        data = {

            "attributionFor": new_attributionFor, # UUID vom Projekt
            "attributedAs": eintrag["rollen_uuid"], # UUID von Rolle, Wert kommt von uuid.py
            "attributedTo": eintrag["person_uuid"], # UUID vom Posten oder von Person, , Wert kommt von uuid.py
            }
    
        print(f"\nSende {eintrag['rollenname']}:")
        print(json.dumps(data, indent=2))

    # Eingabe überprüfen
        print("Sende folgende Daten an API:")
        print(json.dumps(data, indent=2))

        kandidat = (eintrag["person_uuid"], eintrag["rollen_uuid"])
    
        if kandidat in bereits_zugewiesen:
            print(f"Überspringe {eintrag['rollenname']} – bereits zugewiesen.")
            continue  # nicht posten
    # Projekt mit POST updaten
        response = requests_post(
        url,
        headers=auth.get_headers(),
        json=data,
        verify=False
        )

        print("Status:", response.status_code)
        try:
            print("Antwort:", response.json())
        except Exception:
            print("Antwort (Text):", response.text)

if __name__ == "__main__":
    main()
