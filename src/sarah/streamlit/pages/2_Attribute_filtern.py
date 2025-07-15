import streamlit as st
import pandas as pd
import subprocess
import json

st.set_page_config(page_title="Attributions filtern", layout="wide")

st.title("Attributions filtern nach Projekt und Person")

# Daten aus JSON laden
@st.cache_data
def load_data():
    with open("attributions.json", "r", encoding="utf-8") as file:
        return json.load(file)

def reload_data():
    result = subprocess.run(
        ["python", "src\sarah\project_attributions.py"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        st.cache_data.clear()
        st.success("Daten wurden erfolgreich neu geladen.")
    else:
        st.error(f"Fehler beim Laden der Daten: {result.stderr}")

# Button zum Aktualisieren hinzufügen
if st.button("Daten neu laden"):
    reload_data()

data = load_data()

# Prüfen, was genau geladen wurde (Debugging)
# st.json(data)

# Sicherstellen, dass alle Keys existieren
required_keys = {"projects", "persons", "roles", "attributions"}
if not required_keys.issubset(data.keys()):
    st.error(f"JSON-Datei hat unerwartete Struktur: {data.keys()}")
    st.stop()

# DataFrame aus Attributions erzeugen
df = pd.DataFrame(data["attributions"])

# Sicherstellen, dass Spalten existieren
expected_cols = ["project", "person", "role"]
for col in expected_cols:
    if col not in df.columns:
        st.error(f"Spalte '{col}' fehlt in Attributions-Daten!")
        st.stop()

# UUIDs in lesbare Labels umwandeln
df["project_label"] = df["project"].map(data["projects"])
df["person_label"] = df["person"].map(data["persons"])
df["role_label"] = df["role"].map(data["roles"])

# Sidebar-Filter erstellen
st.sidebar.header("🔍 Filteroptionen")

selected_projects = st.sidebar.multiselect(
    "Projekte auswählen",
    options=sorted(df["project_label"].dropna().unique()),
    default=None
)

selected_persons = st.sidebar.multiselect(
    "Personen auswählen",
    options=sorted(df["person_label"].dropna().unique()),
    default=None
)

# Daten filtern
filtered_df = df.copy()

if selected_projects:
    filtered_df = filtered_df[filtered_df["project_label"].isin(selected_projects)]

if selected_persons:
    filtered_df = filtered_df[filtered_df["person_label"].isin(selected_persons)]

# Übersichtliche Anzeige des Ergebnisses
st.write(f"### {len(filtered_df)} Attribution(s) gefunden")

# Ergebnis-Tabelle sauber anzeigen
st.dataframe(
    filtered_df[["project_label", "person_label", "role_label"]].rename(columns={
        "project_label": "Projekt",
        "person_label": "Person",
        "role_label": "Rolle"
    }),
    use_container_width=True,
    hide_index=True
)
