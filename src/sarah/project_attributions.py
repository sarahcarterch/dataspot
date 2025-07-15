import logging
from time import sleep
import time

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get

from dotenv import load_dotenv

from src.sarah.uuid import *

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

start_time = time.time()

# Schritt 1: Attributions abrufen
attributions_raw = ogd_client._get_asset("/rest/test-sarah-1/attributions")
roh_attributions = attributions_raw.get("_embedded", {}).get("attributions", [])

# Schritt 1a: Alle Projekte holen
projekte_raw = ogd_client._get_asset("/rest/test-sarah-1/projects")
alle_projekte = projekte_raw.get("_embedded", {}).get("projects", [])

# UUID des Verzeichnisses "OGD-Freigaben" finden
freigaben_uuid = "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d" # = directory_uuid ohne schemes/
# for projekt in alle_projekte:
    # if projekt.get("_type") == "ProjectDirectory" and projekt.get("label") == "OGD-Freigaben":
        # freigaben_uuid = projekt.get("id")
        # break

# Alle Projekte unterhalb von "OGD-Freigaben" sammeln (robust über _links)
gueltige_project_ids = set()

# Projekte unterhalb von "OGD-Freigaben" explizit abrufen:
projekte_unterhalb_freigaben = ogd_client._get_asset(f"/rest/test-sarah-1/schemes/{freigaben_uuid}/projects")
projekte_liste = projekte_unterhalb_freigaben.get("_embedded", {}).get("projects", [])

for projekt in projekte_liste:
    projekt_id = projekt.get("id")
    if projekt_id:
        gueltige_project_ids.add(projekt_id)
        logger.info(f"Projekt hinzugefügt: {projekt.get('label')} ({projekt_id})")

# Root-Verzeichnis selbst ebenfalls hinzufügen
gueltige_project_ids.add(freigaben_uuid)

print(f"{len(gueltige_project_ids)} gültige Projekte gefunden unterhalb von OGD-Freigaben.")

# Schritt 1b: Nur Attributions zu diesen Projekten behalten
gefilterte_attributions = []
for a in roh_attributions:
    project_id = a.get("attributionFor")
    if project_id in gueltige_project_ids:
        gefilterte_attributions.append(a)

# Schritt 2: UUIDs sammeln
project_ids = set()
person_ids = set()
role_ids = set()

for a in gefilterte_attributions:
    project_ids.add(a.get("attributionFor"))
    person_ids.add(a.get("attributedTo"))
    role_ids.add(a.get("attributedAs"))

# Schritt 3: Labels laden
projects = {}
for pid in project_ids:
    if pid:
        r = ogd_client._get_asset(f"/rest/test-sarah-1/projects/{pid}")
        projects[pid] = r.get("label") if r and isinstance(r, dict) else pid

persons = {}
for pid in person_ids:
    if pid:
        r = ogd_client._get_asset(f"/rest/test-sarah-1/persons/{pid}")
        persons[pid] = r.get("label") if r and isinstance(r, dict) else pid

roles = {}
for rid in role_ids:
    if rid:
        r = ogd_client._get_asset(f"/rest/test-sarah-1/roles/{rid}")
        roles[rid] = r.get("label") if r and isinstance(r, dict) else rid

# Schritt 4: Attributions strukturieren
attributions = []
for a in gefilterte_attributions:
    if all(k in a for k in ("attributionFor", "attributedTo", "attributedAs")):
        attributions.append({
            "project": a["attributionFor"],
            "person": a["attributedTo"],
            "role": a["attributedAs"]
        })

# Schritt 5: JSON schreiben
daten = {
    "projects": projects,
    "persons": persons,
    "roles": roles,
    "attributions": attributions
}

with open("attributions.json", "w", encoding="utf-8") as f:
    json.dump(daten, f, indent=2, ensure_ascii=False)

laufzeit = time.time() - start_time
print("Datei 'attributions.json' wurde erfolgreich erstellt.")
print(f"Laufzeit: {laufzeit:.2f} Sekunden")