import streamlit as st
from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_put, requests_patch

auth = DataspotAuth()
ogd_client = OGDClient()

freigaben_uuid = "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d" # = directory_uuid ohne schemes/

st.set_page_config(page_title="Attribute anpassen", layout="wide")
st.header("Projekt-Attribute verwalten")

def lade_projekte():
    endpoint = f"https://bs.dataspot.io/rest/test-sarah-1/assets/{freigaben_uuid}/projects"
    response = requests_get(endpoint, headers=auth.get_headers())
    return response.get("_embedded", {}).get("projects", [])

def lade_attribute(projekt_id):
    endpoint = f"https://bs.dataspot.io/rest/test-sarah-1/projects/{projekt_id}"
    return requests_get(endpoint, headers=auth.get_headers())

def speichere_attribute(projekt_id, attribute):
    endpoint = f"https://bs.dataspot.io/rest/test-sarah-1/projects/{projekt_id}"
    return requests_patch(endpoint, headers=auth.get_headers(), json=attribute)

# Projekte laden
projekte = lade_projekte()
projekt_auswahl = {projekt["name"]: projekt["id"] for projekt in projekte}
gewaehltes_projekt = st.selectbox("Projekt auswählen", projekt_auswahl.keys())

if gewaehltes_projekt:
    projekt_id = projekt_auswahl[gewaehltes_projekt]
    projekt_details = lade_attribute(projekt_id)

    with st.form("attribute_form"):
        st.subheader("Projektattribute bearbeiten")
        name = st.text_input("Projektname", value=projekt_details.get("name", ""))
        beschreibung = st.text_area("Beschreibung", value=projekt_details.get("description", ""))
        status = st.selectbox("Status", ["aktiv", "inaktiv", "archiviert"], index=0)

        submitted = st.form_submit_button("Speichern")
        
        if submitted:
            update_data = {
                "name": name,
                "description": beschreibung,
                "status": status
            }
            response = speichere_attribute(projekt_id, update_data)
            if response.ok:
                st.success("Attribute erfolgreich gespeichert!")
            else:
                st.error(f"Fehler beim Speichern: {response.status_code}")

