import streamlit as st
import json
import os
import subprocess

st.set_page_config(page_title="Projektstatus", layout="wide")
st.title("Status pro Projekt")

# === Dateien laden ===
json_path = "attributions.json"
workflow_path = "workflow_ogd_stati.json"

if not os.path.exists(json_path) or not os.path.exists(workflow_path):
    st.error("Benötigte Datei fehlt.")
    st.stop()

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

with open(workflow_path, "r", encoding="utf-8") as f:
    status_liste = json.load(f)

# Mapping für schnellen Zugriff: {"WORKING": "In Entwurf", ...}
status_labels = {entry["status"]: entry["label"] for entry in status_liste}

# Reine Statuswerte für Dropdown
status_options = [entry["status"] for entry in status_liste]

# === Projekt-Daten vorbereiten ===
projekt_mapping = data.get("projects", {})
attributions = data.get("attributions", {})

projekt_namen = sorted(projekt_mapping.values())
projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}
gewähltes_projekt = st.selectbox("Projekt auswählen", projekt_namen)

if gewähltes_projekt:
    projekt_id = projekt_id_lookup[gewähltes_projekt]
    aktueller_status = attributions.get(projekt_id, {}).get("status", None)

    st.markdown(f"**Aktueller Status:** `{aktueller_status}` – **{status_labels.get(aktueller_status, 'Unbekannt')}`**")

    neuer_status = st.selectbox(
    "Neuen Status wählen",
    options=status_options,
    format_func=lambda x: status_labels.get(x, x),
    index=status_options.index(aktueller_status) if aktueller_status in status_options else 0
)

    if st.button("Status aktualisieren und hochladen"):
        if projekt_id in attributions:
            attributions[projekt_id]["status"] = neuer_status
        else:
            st.warning("Projekt nicht in attributions.json gefunden – wird neu angelegt.")
            attributions[projekt_id] = {"status": neuer_status, "personen": []}
            st.json(attributions[projekt_id])

        data["attributions"] = attributions
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        result = subprocess.run(["python", "src/sarah/project_attr_sync.py", projekt_id], capture_output=True, text=True)
        if result.returncode == 0:
            st.success("Status gespeichert und zur API übertragen.")
        else:
            # st.error(f"Fehler beim Sync: {result.stderr}")
            st.error(f"Fehler beim Sync")

    if st.button("Aktuelle Daten von API laden"):
        result = subprocess.run(["python", "src/sarah/project_attributions.py"], capture_output=True, text=True)
        if result.returncode == 0:
            st.success("Daten erfolgreich neu geladen. Bitte Seite neu laden.")
        else:
            st.error(f"Fehler beim Neuladen: {result.stderr}")
