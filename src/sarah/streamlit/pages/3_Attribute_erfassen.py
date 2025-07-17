import streamlit as st
import subprocess
import json
import os

st.set_page_config(page_title="Attribute erfassen", layout="wide")
st.header("Projektattribute erfassen oder anpassen")

# JSON-Datei laden
json_path = "attributions.json"
if not os.path.exists(json_path):
    st.error("Datei attributions.json nicht gefunden.")
    st.stop()

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Projekte extrahieren
projekt_mapping = data.get("projects", {})
projekt_namen = sorted(projekt_mapping.values())
projekt_id_lookup = {v: k for k, v in projekt_mapping.items()}

# Projektwahl
gewähltes_projekt = st.selectbox("Projekt auswählen", projekt_namen)

if gewähltes_projekt:
    projekt_id = projekt_id_lookup[gewähltes_projekt]

    # Sicherstellen, dass Attributions existieren
    if projekt_id not in data["attributions"]:
        data["attributions"][projekt_id] = {
            "status": "",
            "personen": []
        }

    projekt_eintrag = data["attributions"][projekt_id]

    if isinstance(projekt_eintrag, dict) and "personen" in projekt_eintrag:
        aktuelle_attributions = projekt_eintrag["personen"]
    else:
        st.warning("Dieses Projekt verwendet ein älteres Format und kann hier nicht bearbeitet werden.")
        st.stop()

    # Optionen
    person_options = data.get("persons", {})
    role_options = data.get("roles", {})
    person_names = list(person_options.values())
    role_names = list(role_options.values())
    name_to_person_id = {v: k for k, v in person_options.items()}
    name_to_role_id = {v: k for k, v in role_options.items()}
    person_id_to_name = {k: v for k, v in person_options.items()}
    role_id_to_name = {k: v for k, v in role_options.items()}

    st.subheader("Zugewiesene Personen")

    neue_attributions = []
    num_rows = len(aktuelle_attributions) + 1  # eine zusätzliche Zeile zum Hinzufügen

    for i in range(num_rows):
        existing = aktuelle_attributions[i] if i < len(aktuelle_attributions) else {}
        selected_person = person_id_to_name.get(existing.get("person", ""), "")
        selected_role = role_id_to_name.get(existing.get("role", ""), "")

        col1, col2 = st.columns(2)
        with col1:
            p = st.selectbox(
                f"Person {i+1}",
                options=[""] + person_names,
                index=([""] + person_names).index(selected_person) if selected_person else 0,
                key=f"person_{i}"
            )
        with col2:
            r = st.selectbox(
                f"Rolle {i+1}",
                options=[""] + role_names,
                index=([""] + role_names).index(selected_role) if selected_role else 0,
                key=f"rolle_{i}"
            )

        if p and r:
            neue_attributions.append({
                "person": name_to_person_id[p],
                "role": name_to_role_id[r]
            })

    if st.button("Änderungen speichern"):
        data["attributions"][projekt_id] = neue_attributions
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        st.success("Änderungen gespeichert.")


import os

# --- Trennlinie zur besseren Lesbarkeit ---
st.markdown("---")
st.header("Synchronisation")

def upload_data():
    script_path = os.path.join("src", "sarah", "project_attr_sync.py")

    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        st.cache_data.clear()
        st.success("Daten wurden erfolgreich hochgeladen.")
        st.text(result.stdout)
    else:
        st.error("Fehler beim Upload der Daten:")
        st.text(result.stderr)

# Button zum Starten der Synchronisation
if st.button("Synchronisation mit Datenplattform starten"):
    upload_data()

