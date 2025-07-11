import streamlit as st
import pandas as pd
import numpy as np
from collections import defaultdict

import os
import logging
import argparse
from time import sleep

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get

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

# Die Projekte der Sammlung auflisten
response = ogd_client._get_asset(endpoint=f"{base_path}{directory_uuid}/projects")
projects = response["_embedded"]["projects"]

# Für jedes Projekt: attributedTo auflösen

print("Gefundene Projekte mit zugewiesenen Personen:")

all_projects = []

for project in projects:
    project_name = project.get("label", "Kein Name")
    project_title = project.get("title", "Kein Titel")
    project_uuid = project.get("id")
    
    print(f"\nProjekt: {project_name}")

    # Personen abrufen, die diesem Projekt zugewiesen sind
    endpoint = f"{base_path}projects/{project_uuid}/attributedTo"
    response = ogd_client._get_asset(endpoint=endpoint)

    personen = []
    if response is not None and "_embedded" in response and "attributedTo" in response["_embedded"]:
        for attribution in response["_embedded"]["attributedTo"]:
            person_uuid = attribution.get("attributedTo")
            if person_uuid:
                # Personen-Details holen
                person_endpoint = f"{base_path}persons/{person_uuid}"
                person_response = ogd_client._get_asset(endpoint=person_endpoint)
                person_label = person_response.get("label", "Unbekannte Person")
                personen.append(person_label)
                print(" -", person_label)
            else:
                print(" - Keine gültige Person gefunden")
    else:
        print(f"⚠️ Keine attributedTo-Einträge für Projekt {project_name} gefunden.")

    # Struktur für spätere Anzeige speichern
    all_projects.append({
        "projekt": project_name,
        "titel": project_title,
        "personen": personen
    })

st.title("Alle Projekte und ihre zugewiesenen Personen")

for eintrag in all_projects:
    st.write(eintrag["projekt"])
    for person in eintrag["personen"]:
        st.write("- ", person)
