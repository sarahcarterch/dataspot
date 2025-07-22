import streamlit as st
import pandas as pd
from src.dataspot_auth import DataspotAuth
from src.common import requests_get
from dotenv import load_dotenv
from src.sarah.uuid import *

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

@st.cache_data
def load_data():
    return pd.read_csv(CSV_PATH, sep=";")
data = load_data()

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
df["name"] = (
    df["givenName"].fillna("").str.replace("\u00a0", " ", regex=False).str.strip() + " " +
    df["familyName"].fillna("").str.replace("\u00a0", " ", regex=False).str.strip()
).str.strip()
df["link"] = df["_links"].apply(lambda x: x.get("self", {}).get("href", ""))

for name in df["name"].unique():
    if name != name.strip():
        print(f"Verdächtig: →{repr(name)}←")

# Sidebar-Filter für Posten und Person
posten_liste = sorted(df["post_label"].dropna().unique())
selected_posten = st.sidebar.multiselect("Posten auswählen", posten_liste)

personen_liste = sorted(df["name"].dropna().unique())
personen_liste = [p.strip() for p in personen_liste]
selected_person = st.sidebar.multiselect("Person auswählen", personen_liste)

# Person aus Projekten suchen
contact_liste = sorted(
    data["Kontakt"]  # beide Spalten zusammenführen
    .dropna()                                         # NaN entfernen
    .astype(str)                                      # sicherstellen, dass alle Strings sind
    .str.replace("\u00a0", " ", regex=False)
    .str.strip()                                      # führende/trailing Leerzeichen entfernen
    .loc[lambda x: x != ""]                           # leere Strings rausfiltern
    .unique()                                         # Duplikate entfernen
)
contact_II_liste = sorted(
    data["Kontakt II"]  # beide Spalten zusammenführen
    .dropna()                                         # NaN entfernen
    .astype(str)                                      # sicherstellen, dass alle Strings sind
    .str.replace("\u00a0", " ", regex=False)
    .str.strip()                                      # führende/trailing Leerzeichen entfernen
    .loc[lambda x: x != ""]                           # leere Strings rausfiltern
    .unique()                                         # Duplikate entfernen
)
intern_liste = sorted(
    data["Interne Zuständigkeit"]  # beide Spalten zusammenführen
    .dropna()                                         # NaN entfernen
    .astype(str)                                      # sicherstellen, dass alle Strings sind
    .str.replace("\u00a0", " ", regex=False)
    .str.strip()                                      # führende/trailing Leerzeichen entfernen
    .loc[lambda x: x != ""]                           # leere Strings rausfiltern
    .unique()                                         # Duplikate entfernen
)

kombiniert = [""] + sorted(set(contact_liste + contact_II_liste + intern_liste))
selected_project_contact = st.sidebar.selectbox(
    "Person aus Projekten durchsuchen",
    kombiniert,  # "" für „nichts gewählt“
    index=0
)

# Filter anwenden
filtered_df = df.copy()
filtered_data = data.copy()
if selected_posten:
    filtered_df = filtered_df[filtered_df["post_label"].isin(selected_posten)]

if selected_person:
    filtered_df = filtered_df[filtered_df["name"].isin(selected_person)]

if selected_project_contact:  # nur wenn etwas gewählt wurde
    if selected_project_contact in df["name"].values:
        filtered_df = filtered_df[filtered_df["name"] == selected_project_contact]
    else:
        filtered_df = filtered_df[0:0]

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

# Ergänzung: Alle Personen, die sowohl in der Projektdatei (data) als auch in der API (df) vorkommen

# Alle Werte aus 'Kontakt' und 'Kontakt II' zusammenführen
spalten = [col for col in ["Kontakt", "Kontakt II"] if col in data.columns]
projektkontakte = pd.concat([data[col] for col in spalten])
projektkontakte = (
    projektkontakte.dropna()
    .astype(str)
    .str.strip()
    .loc[lambda x: x != ""]
    .unique()
)

# Jetzt prüfen, ob df["name"] in projektkontakte vorkommt
df_gemeinsame = df[df["name"].isin(projektkontakte)]

# Ergebnis anzeigen
st.subheader(f"{len(df_gemeinsame)} Personen sind sowohl in der Projektdatei als auch in der API vorhanden")
st.dataframe(
    df_gemeinsame[["name", "post_label", "link"]].rename(columns={
        "name": "Name",
        "post_label": "Posten",
        "link": "Link"
    }),
    use_container_width=True,
    hide_index=True
)

# Jetzt prüfen, ob df["name"] NICHT in projektkontakte vorkommt
df_gemeinsame = df[~df["name"].isin(projektkontakte)]

# Ergebnis anzeigen
st.subheader(f"{len(df_gemeinsame)} Personen sind nicht in der Projektdatei vorhanden")
st.dataframe(
    df_gemeinsame[["name", "post_label", "link"]].rename(columns={
        "name": "Name",
        "post_label": "Posten",
        "link": "Link"
    }),
    use_container_width=True,
    hide_index=True
)

# Namen aus der API
api_namen = set(df["name"].dropna().astype(str).str.strip())

# Projektkontakte aus allen relevanten Spalten
spalten = [col for col in ["Kontakt", "Kontakt II", "Interne Zuständigkeit"] if col in data.columns]

alle_namen = set()  # Set für eindeutige Namen

for col in spalten:
    werte = (
        data[col]
        .dropna()
        .astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.strip()
        .loc[lambda x: x != ""]
    )
    alle_namen.update(werte)

# Differenzmenge: Nur in Projektdatei, nicht in API
nur_in_projektdatei = sorted(alle_namen - api_namen)

# Ergebnis anzeigen
st.subheader(f"{len(nur_in_projektdatei)} eindeutige Personen aus der Projektdatei wurden nicht in der API gefunden")
st.dataframe(
    pd.DataFrame(nur_in_projektdatei, columns=["Name"]),
    use_container_width=True,
    hide_index=True
)

