import streamlit as st
import pandas as pd
from src.dataspot_auth import DataspotAuth
from src.common import requests_get
from dotenv import load_dotenv

load_dotenv()

auth = DataspotAuth()

# Daten von der API holen
@st.cache_data(show_spinner="Lade Personen...")
def lade_personen():
    base_url = "https://bs.dataspot.io"
    endpoint = "/rest/test-sarah-1/persons"
    response = requests_get(f"{base_url}{endpoint}", headers=auth.get_headers())
    data = response.json()
    return data.get("_embedded", {}).get("persons", [])


@st.cache_data(show_spinner="Lade Posten...")
def lade_posten_label(url):
    """Lädt das Label eines Postens über die API"""
    base_url = "https://bs.dataspot.io"
    full_url = f"{base_url}{url}"
    try:
        response = requests_get(full_url, headers=auth.get_headers())
        return response.json().get("label", "Unbekannt")
    except Exception as e:
        return f"Fehler: {e}"

persons = lade_personen()

# UI konfigurieren
st.set_page_config(page_title="Personenübersicht", layout="wide")
st.title("Personenübersicht")

# Postenlabels für jede Person laden
for p in persons:
    posten_links = p.get("_links", {}).get("holdsPost", [])
    if isinstance(posten_links, list) and posten_links:
        p["post_label"] = lade_posten_label(posten_links[0]["href"])
    else:
        p["post_label"] = "Kein Posten"

# DataFrame erzeugen
df = pd.DataFrame(persons)
df["name"] = df["givenName"].fillna("") + " " + df["familyName"].fillna("")
df["link"] = df["_links"].apply(lambda x: x.get("self", {}).get("href", ""))

# Sidebar-Filter für Posten und Person
posten_liste = sorted(df["post_label"].dropna().unique())
selected_posten = st.sidebar.multiselect("Posten auswählen", posten_liste)

personen_liste = sorted(df["name"].dropna().unique())
selected_person = st.sidebar.multiselect("Person auswählen", personen_liste)

# Filter anwenden
filtered_df = df.copy()
if selected_posten:
    filtered_df = filtered_df[filtered_df["post_label"].isin(selected_posten)]

if selected_person:
    filtered_df = filtered_df[filtered_df["name"].isin(selected_person)]

# Tabelle anzeigen
st.subheader(f"{len(filtered_df)} Personen gefunden")
st.dataframe(
    filtered_df[["name", "post_label", "link"]].rename(columns={
        "name": "Name",
        "post_label": "Posten",
        "link": "Link"
    }),
    use_container_width=True,
    hide_index=True
)