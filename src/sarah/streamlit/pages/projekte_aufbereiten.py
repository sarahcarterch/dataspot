import streamlit as st
import pandas as pd
import subprocess
import json
import csv

CSV_PATH = "src\\sarah\\data\\csv\\ogd_datensaetze.csv"

st.set_page_config(page_title="Projekte aufbereiten", layout="wide")

st.title("Projekte aufbereiten nach Projekt, Organisation und Personen")

# Daten aus CSV laden
@st.cache_data
def load_data():
    return pd.read_csv(CSV_PATH, sep=";")

data = load_data()

# Sicherstellen, dass Spalten existieren
expected_cols = ["Laufnummer","ID für data.bs.ch","Titel","in Planung","Datenowner","Interne Zuständigkeit","Kontakt","Kontakt II","Bemerkungen"]
if not all(col in data.columns for col in expected_cols):
    st.warning("Nicht alle erwarteten Spalten sind in der CSV vorhanden.")
    st.stop()

# Sidebar-Filter erstellen
st.sidebar.header("Filteroptionen")

selected_projects = st.sidebar.multiselect(
    "Projekte auswählen",
    options=sorted(data["ID für data.bs.ch"].dropna().unique()),
    default=None
)

selected_owners = st.sidebar.multiselect(
    "Data Owner auswählen",
    options=sorted(data["Datenowner"].dropna().unique()),
    default=None
)

selected_steward = st.sidebar.multiselect(
    "Data Steward auswählen",
    options=sorted(data["Kontakt"].dropna().unique()),
    default=None
)

# Daten filtern
filtered_df = data.copy()

if selected_projects:
    filtered_df = filtered_df[filtered_df["ID für data.bs.ch"].isin(selected_projects)]

if selected_owners:
    filtered_df = filtered_df[filtered_df["Datenowner"].isin(selected_owners)]

if selected_steward:
    filtered_df = filtered_df[filtered_df["Kontakt"].isin(selected_steward)]

# Übersichtliche Anzeige des Ergebnisses
st.write(f"### {len(filtered_df)} Attribution(s) gefunden")

# Ergebnis-Tabelle sauber anzeigen
st.dataframe(
    filtered_df[["Laufnummer","ID für data.bs.ch","Titel","in Planung","Datenowner","Interne Zuständigkeit","Kontakt","Kontakt II","Bemerkungen"]],
    use_container_width=True,
    hide_index=True
)