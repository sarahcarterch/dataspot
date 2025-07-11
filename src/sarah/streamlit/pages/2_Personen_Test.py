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

# ID paths

directory_url = f"{base_url}{base_path}{directory_uuid}"
attributedTo_url = f"{base_url}{base_path}{directory_uuid}/attributedTo"
endpoint = f"{base_path}{directory_uuid}/attributedTo"

response = ogd_client._get_asset(endpoint=endpoint)
print(response)

attributedObjects = response["_embedded"]["attributedTo"]
attributedPerson = attributedObjects[0]
Person = attributedPerson["attributedTo"]
print(Person)

personDetails_url = f"{base_url}{base_path}persons/{Person}"
endpoint = f"{base_path}persons/{Person}"
response = ogd_client._get_asset(endpoint=endpoint)
print(response["label"])
name = response["label"]

endpoint = f"{base_path}persons/{Person}/attributionFor"
response = ogd_client._get_asset(endpoint=endpoint)
print(response)

attributedFor = response["_embedded"]["attributionFor"]
attributedFor = attributedFor[0]
Object = attributedFor["attributionFor"]
print(Object)

objectDetails_url = f"{base_url}{base_path}persons/{Object}"
endpoint = f"{base_path}schemes/{Object}"
response = ogd_client._get_asset(endpoint=endpoint)
print(response["label"])
attributions = response["label"]

#def main():

    #print("Gefundene Personen und Projekte")
    #for person in data["_embedded"]["projects"]:
        #name = person.get("label", "Kein Name")
        #print("-", name)

    #return data["_embedded"]["projects"]

#if __name__ == "__main__":
    #main()

# Streamlit UI
st.title("Projektübersicht")

# Als einfache Liste ausgeben
st.subheader("Gefundene Personen:")
col1, col2 = st.columns(2)

with col1:
    st.header("Personen")
    st.write(name)

with col2: 
    st.header("Attribution")
    st.write(attributions)