import json
import sys
import logging
from dotenv import load_dotenv

from src.dataspot_auth import DataspotAuth
from src.common import requests_get, requests_post, requests_delete, requests_patch

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
API_BASE = "https://bs.dataspot.io/rest/test-sarah-1"


def patch_project_status(project_id, neuer_status):
    url = f"{API_BASE}/projects/{project_id}"
    headers = auth.get_headers()
    neuer_status = neuer_status.strip()

    # Projektinformationen abrufen
    response = requests_get(url, headers=headers, verify=False)
    if response.status_code != 200:
        logger.warning(f"Projekt {project_id} nicht gefunden (Status {response.status_code})")
        return

    projekt_data = response.json()
    aktueller_status = projekt_data.get("status")
    status_definitionen = projekt_data.get("definition", {}).get("statuses", [])
    user_id = auth.get_user_id()  # ← Achtung: Muss von dir implementiert oder bereitgestellt sein

    erlaubte_transitions = []

    # Transitions für den aktuellen Status extrahieren
    for status_block in status_definitionen:
        if status_block.get("status") == aktueller_status:
            for t in status_block.get("transitions", []):
                to_status = t.get("to")
                can_trigger = t.get("canTrigger", [])
                if to_status and user_id in can_trigger:
                    erlaubte_transitions.append(to_status)
            break

    logger.info(f"Aktueller Status: {aktueller_status}, Zielstatus: {neuer_status}")
    logger.info(f"Erlaubte Übergänge von {aktueller_status}: {erlaubte_transitions}")

    # Prüfung: darf auf neuen Status gewechselt werden?
    if neuer_status not in erlaubte_transitions:
        logger.warning(f"Übergang {aktueller_status} → {neuer_status} nicht erlaubt.")
        return

    # PATCH durchführen
    payload = {"status": neuer_status}
    logger.info(f"PATCH: Status von Projekt {project_id} → {neuer_status}")
    response = requests_patch(url, headers=headers, json=payload, verify=False)
    logger.info(f"Status-PATCH Antwort: {response.status_code}")

    if response.status_code >= 300:
        logger.warning(f"Fehler beim PATCH für Projektstatus: {response.text}")


def fetch_all_attributions():
    """Lädt alle aktuellen API-Attributions."""
    url = f"{API_BASE}/attributions"
    headers = auth.get_headers()
    try:
        response = requests_get(url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Fehler beim Laden der Attributions: {e}")
        return {}


def sync_project_attributions(project_id, eintrag):
    """Synchronisiert Status + Rollen für ein einzelnes Projekt."""
    if project_id == "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d":
        logger.info(f"Projektverzeichnis {project_id} wird übersprungen.")
        return

    ziel_status = eintrag.get("status")
    ziel_personen = {p["person"]: p["role"] for p in eintrag.get("personen", [])}

    # Statuswechsel prüfen
    project_url = f"{API_BASE}/projects/{project_id}"
    response = requests_get(project_url, headers=auth.get_headers(), verify=False)
    if response.status_code != 200:
        logger.warning(f"Projekt {project_id} nicht gefunden.")
        return

    aktueller_status = response.json().get("status")
    if ziel_status and ziel_status != aktueller_status:
        patch_project_status(project_id, ziel_status)

    # Rollenvergleich
    api_data = fetch_all_attributions()
    all_api_attributions = api_data.get("_embedded", {}).get("attributions", [])
    api_personen = {
        a["attributedTo"]: a["attributedAs"]
        for a in all_api_attributions if a.get("attributionFor") == project_id
    }

    logger.debug(f"Zielpersonen: {ziel_personen}")
    logger.debug(f"API-Personen: {api_personen}")

    # PATCH oder DELETE
    for pid, rolle in api_personen.items():
        zielrolle = ziel_personen.get(pid)
        if not zielrolle:
            logger.info(f"DELETE: {pid} aus Projekt {project_id}")
            # DELETE-Logik (wie gehabt)
        elif zielrolle != rolle:
            logger.info(f"PATCH: {pid} von {rolle} zu {zielrolle}")
            # PATCH-Logik (wie gehabt)

    # POST neue Einträge
    for pid, rolle in ziel_personen.items():
        if pid not in api_personen:
            logger.info(f"POST: {pid} als {rolle} zu Projekt {project_id}")
            # POST-Logik (wie gehabt)


def main():
    with open("attributions.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    all_attributions = data.get("attributions", {})
    if len(sys.argv) > 1:
        pid = sys.argv[1]
        if pid in all_attributions:
            sync_project_attributions(pid, all_attributions[pid])
    else:
        for pid, eintrag in all_attributions.items():
            sync_project_attributions(pid, eintrag)


if __name__ == "__main__":
    main()
