import streamlit as st
import pandas as pd
import numpy as np

import os
import logging
import argparse
from time import sleep

from ogd_client import OGDClient
from dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()

def main():
    base_url = "https://bs.dataspot.io"
    link = "https://bs.dataspot.io/rest/test-sarah-1/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d"
    href1 = "_links"
    href2 = "projects"

    response = requests_get(link, headers=auth.get_headers())
    print(response)

    pass

    ogd_client = OGDClient()
    endpoint = "/rest/test-sarah-1/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d"
    response = ogd_client._get_asset(endpoint=endpoint)
    print(response[href1][href2]['href'])
    projects_url = response[href1][href2]['href']
    print(projects_url)
    full_url = base_url + projects_url

    projects_response = requests_get(full_url, headers=auth.get_headers())
    projects_data = projects_response.json()

    print("Gefundene Projekte")
    for project in projects_data["_embedded"]["projects"]:
        name = project.get("label", "Kein Name")
        print("-", name)

    return projects_data["_embedded"]["projects"]

if __name__ == "__main__":
    main()

# Streamlit UI
st.title("Projektübersicht")

projects = main()

# Als einfache Liste ausgeben
st.subheader("Gefundene Projekte:")
for proj in projects:
    st.markdown(f"- **{proj.get('label', 'Kein Label')}**")

# Optional: Als Tabelle anzeigen
import pandas as pd
df = pd.DataFrame(projects)
if not df.empty:
    st.subheader("Projekte als Tabelle")
    st.dataframe(df[["label", "_type"]])  # oder weitere Felder, je nach API
else:
    st.info("Keine Projekte gefunden.")

