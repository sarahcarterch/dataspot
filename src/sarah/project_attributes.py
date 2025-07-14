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

project_uuid = f"{directory_uuid}/projects"      # dann: {base_path}/projects/["id"]/attributedTo
# endpoint = f"/rest/test-sarah-1/{project_uuid}"
# endpoint = f"projects/bdb3fbbe-3ab0-4a08-bb9e-ed8c19b42924/attributedTo" # Test1

endpoint = "https://bs.dataspot.io/rest/test-sarah-1/attributions"
url = endpoint
# url = f"{base_url}{endpoint}"

new_id = None
new_tenantId = None
new_modelId = new_modelId
new_attributionFor = new_attributionFor
new_attributedTo = new_attributedTo # ID der gewünschten Person einfügen
new_attributedAs_do = data_owner
new_attributedAs_ds = data_steward
new_type = "Attribution"     # "_type"

def main():
    rollenzuweisungen = [
        {
            "rollenname": "Data Owner",
            "rollen_uuid": new_attributedAs_do,
            "person_uuid": lm,
        },
        {
            "rollenname": "Data Steward",
            "rollen_uuid": new_attributedAs_ds,
            "person_uuid": ug,
        }
    ]
# Aktuelle Projektdaten holen (optional)
    # projekt = ogd_client._get_asset(endpoint=endpoint)
    for eintrag in rollenzuweisungen:
        data = {
            "_embedded": {
                "attributedTo": [
                    {
                        # "id": None,
                        "tenantId": "bf6a14c1-ed03-4d8a-beea-c6db320502fd",
                        "modelId": new_modelId,
                        "attributionFor": new_attributionFor,
                        "attributedTo": eintrag["person_uuid"],
                        "attributedAs": eintrag["rollen_uuid"],
                        "_type": new_type,
                        #"_links": {
                            #"self": {
                                #"href": None
                            #}
                        #}
                    }
                ]
            }
        }
    
    print(f"\nSende {eintrag['rollenname']}:")
    print(json.dumps(data, indent=2))

# Eingabe überprüfen
    print("Sende folgende Daten an API:")
    print(json.dumps(data, indent=2))
# Projekt mit PATCH updaten
    response = requests_put(
    url,
    headers=auth.get_headers(),
    json=data,
    # verify=False
    )

    print("Status:", response.status_code)
    try:
        print("Antwort:", response.json())
    except Exception:
        print("Antwort (Text):", response.text)

if __name__ == "__main__":
    main()
