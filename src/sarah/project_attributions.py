import logging
import time
import json
from dotenv import load_dotenv

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import requests_get
from src.sarah.uuid import *

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

start_time = time.time()

# Alle Attributions abrufen
attributions_raw = ogd_client._get_asset("/rest/test-sarah-1/attributions")
roh_attributions = attributions_raw.get("_embedded", {}).get("attributions", [])

# Alle Projekte unterhalb von OGD-Freigaben
freigaben_uuid = "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d"
projekte_unterhalb = ogd_client._get_asset(f"/rest/test-sarah-1/schemes/{freigaben_uuid}/projects")
projekte_liste = projekte_unterhalb.get("_embedded", {}).get("projects", [])

gueltige_project_ids = set()
projects = {}

for projekt in projekte_liste:
    pid = projekt.get("id")
    label = projekt.get("label")
    status = projekt.get("status")
    type = projekt.get("_type")
    if pid:
        gueltige_project_ids.add(pid)
        projects[pid] = label
        logger.info(f"Projekt gefunden: {label} ({pid}, {type})")

# Root-Verzeichnis ebenfalls aufnehmen
gueltige_project_ids.add(freigaben_uuid)
projects[freigaben_uuid] = "OGD-Freigaben"

# Nur Attributions zu diesen Projekten behalten
gefilterte_attributions = []
for a in roh_attributions:
    project_id = a.get("attributionFor")
    if project_id in gueltige_project_ids:
        gefilterte_attributions.append(a)

# Personen- und Rollen-IDs extrahieren
person_ids = set(a.get("attributedTo") for a in gefilterte_attributions if a.get("attributedTo"))
role_ids = set(a.get("attributedAs") for a in gefilterte_attributions if a.get("attributedAs"))

# Labels laden
persons = {}
for pid in person_ids:
    r = ogd_client._get_asset(f"/rest/test-sarah-1/persons/{pid}")
    persons[pid] = r.get("label") if r and isinstance(r, dict) else pid

roles = {}
for rid in role_ids:
    r = ogd_client._get_asset(f"/rest/test-sarah-1/roles/{rid}")
    roles[rid] = r.get("label") if r and isinstance(r, dict) else rid

# Attributions strukturieren: Nach Projekt gruppieren

struktur_pro_projekt = {}

projekt_status_map = {
    projekt.get("id"): projekt.get("status")
    for projekt in projekte_liste
}
projekt_status_map[freigaben_uuid] = "OGD_ROOT"

projekt_type_map = {
    projekt.get("id"): projekt.get("_type")
    for projekt in projekte_liste
}

# Gefundene Attributions zuordnen
for a in gefilterte_attributions:
    project_id = a.get("attributionFor")
    person_id = a.get("attributedTo")
    role_id = a.get("attributedAs")
    status = projekt_status_map.get(project_id)
    type = projekt_type_map.get(project_id)

    # Initialisieren, wenn Projekt noch nicht bekannt
    if project_id not in struktur_pro_projekt:
        struktur_pro_projekt[project_id] = {
            "_type": type,
            "status": status,
            "personen": []
        }

    # Person hinzufügen (immer)
    struktur_pro_projekt[project_id]["personen"].append({
        "person": person_id,
        "role": role_id
    })

# Ergänzung: Leere Projekte ohne Attributions hinzufügen
for projekt_id in projects:
    if projekt_id not in struktur_pro_projekt:
        struktur_pro_projekt[projekt_id] = {
            "_type": projekt_type_map.get(projekt_id),
            "status": projekt_status_map.get(projekt_id),
            "personen": []
        }

# Finales JSON
daten = {
    "projects": projects,
    "persons": persons,
    "roles": roles,
    "attributions": struktur_pro_projekt
}

with open("attributions.json", "w", encoding="utf-8") as f:
    json.dump(daten, f, indent=2, ensure_ascii=False)

laufzeit = time.time() - start_time
print("Datei 'attributions.json' wurde erfolgreich erstellt.")
print(f"Laufzeit: {laufzeit:.2f} Sekunden")
