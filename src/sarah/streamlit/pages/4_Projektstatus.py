import streamlit as st
import json
import os

st.set_page_config(page_title="Projektstatus", layout="wide")
st.title("Status pro Projekt")

# JSON-Datei laden
json_path = "attributions.json"
if not os.path.exists(json_path):
    st.error("Datei attributions.json nicht gefunden.")
    st.stop()

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Projekt-Mapping
projekt_mapping = data.get("projects", {})

# Status aus Attributionsdaten extrahieren (Status aus dritten Eintrag je Projekt)
attributions = data.get("attributions", {})

status_mapping = {
    projekt_id: attributions[projekt_id]["status"]
    for projekt_id in projekt_mapping
    if projekt_id in attributions
}

# Projekt-Auswahl
projekt_namen = sorted(projekt_mapping.values())
projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}
gewähltes_projekt = st.selectbox("Projekt auswählen", projekt_namen)

if gewähltes_projekt:
    projekt_id = projekt_id_lookup[gewähltes_projekt]
    status_code = status_mapping[projekt_id]

    # Lesbare Labels
    status_labels = {
        "WORKING": "In Entwurf",
        "REVIEW3DS": "Entwurfsprüfung Data Steward",
        "PUBLISHEDDCC2": "Veröffentlicht",
        "REJECTED": "Abgelehnt",
        "MUTATION_NV": "Mutation mit Abhängigkeiten"
    }

    label = status_labels.get(status_code, status_code)

    st.markdown(f"### Aktueller Status: **{label}** (`{status_code}`)")

    # Fortschrittsanzeige
    status_keys = list(status_labels.keys())
    if status_code in status_keys:
        index = status_keys.index(status_code)
        progress = (index + 1) / len(status_keys)
        st.progress(progress)
    else:
        st.info("Status ist definiert, aber nicht im Fortschrittsmapping enthalten.")
