import json
import sys
import logging
from dotenv import load_dotenv

from src.dataspot_auth import DataspotAuth
from src.ogd_client import OGDClient
from src.common import requests_get, requests_post, requests_delete, requests_patch

load_dotenv()
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

auth = DataspotAuth()
ogd_client = OGDClient()

API_BASE = "https://bs.dataspot.io/rest/test-sarah-1"


def patch_project_status(project_id, neuer_status):
    url = f"{API_BASE}/projects/{project_id}"
    headers = auth.get_headers()
    payload = {"status": neuer_status.strip()}

    logger.info(f"Versuche direkten PATCH: Projekt {project_id} → {neuer_status}")
    response = requests_patch(url, headers=headers, json=payload, verify=False)
    logger.info(f"Status-PATCH Antwort: {response.status_code}")

    if response.status_code >= 300:
        logger.warning(f"Fehler beim direkten PATCH: {response.text}")
    else:
        logger.info(f"Status erfolgreich auf {neuer_status} gesetzt.")


def fetch_all_attributions():
    url = f"{API_BASE}/attributions"
    headers = auth.get_headers()

    try:
        response = requests_get(url, headers=headers, verify=False)
        logger.debug(f"GET {url} → {response.status_code}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Attributions: {e}")
        return {}


def sync_project_attributions(project_id, eintrag):
    if project_id == "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d":
        logger.info(f"Projekt {project_id} ist ein Projektverzeichnis und wird übersprungen.")
        return

    ziel_attributions = eintrag.get("personen", [])
    ziel_status = eintrag.get("status")

    # Aktuellen Status aus der API laden
    project_url = f"{API_BASE}/projects/{project_id}"
    response = requests_get(project_url, headers=auth.get_headers(), verify=False)
    current_status = None
    if response.status_code == 200:
        current_status = response.json().get("status")

    logger.info(f"Aktueller API-Status für Projekt {project_id}: {current_status}")
    logger.info(f"Zielstatus aus JSON: {ziel_status}")

    if ziel_status and ziel_status != current_status:
        patch_project_status(project_id, ziel_status)

    logger.debug(f"\n---\nStarte Sync für Projekt: {project_id}")
    logger.debug(f"Zielattributions (roh): {json.dumps(ziel_attributions, indent=2)}")

    api_response = fetch_all_attributions()
    if not api_response or "_embedded" not in api_response or "attributions" not in api_response["_embedded"]:
        logger.warning(f"Keine Attributions in API-Antwort für Projekt {project_id}, überspringe.")
        return

    all_api_attributions = api_response["_embedded"]["attributions"]
    api_entries = [a for a in all_api_attributions if a.get("attributionFor", "").strip() == project_id.strip()]
    ziel_by_person = {a["person"].strip(): a["role"].strip() for a in ziel_attributions}
    api_by_person = {a.get("attributedTo", '').strip(): a.get("attributedAs", '').strip()
                     for a in api_entries if a.get("attributedTo")}

    ziel_personen = set(ziel_by_person.keys())
    api_personen = set(api_by_person.keys())

    # DELETE
    for person_id in api_personen - ziel_personen:
        rolle = api_by_person[person_id]
        attribution_id = next((a.get("id") for a in api_entries
                               if a.get("attributedTo") == person_id and a.get("attributedAs") == rolle), None)
        if attribution_id:
            delete_url = f"{API_BASE}/attributions"
            logger.info(f"DELETE: Entferne {person_id} mit Rolle {rolle} aus Projekt {project_id}")
            response = requests_delete(delete_url, headers=auth.get_headers(), verify=False)
            logger.info(f"DELETE Status: {response.status_code}")
        else:
            logger.warning(f"Keine passende Attribution-ID gefunden für {person_id} mit Rolle {rolle}")

    # PATCH
    for person_id in ziel_personen & api_personen:
        zielrolle = ziel_by_person[person_id]
        aktuellerolle = api_by_person[person_id]
        if zielrolle != aktuellerolle:
            attribution_id = next((a.get("id") for a in api_entries if a.get("attributedTo") == person_id), None)
            if attribution_id:
                patch_url = f"{API_BASE}/attributions/{attribution_id}"
                payload = {"attributedAs": zielrolle}
                logger.info(f"PATCH: Ändere {person_id} von Rolle {aktuellerolle} zu {zielrolle}")
                response = requests_patch(patch_url, headers=auth.get_headers(), json=payload, verify=False)
                logger.info(f"PATCH Status: {response.status_code}")
            else:
                logger.warning(f"Keine Attribution-ID für PATCH bei {person_id}")

    # POST
    for person_id in ziel_personen - api_personen:
        rolle = ziel_by_person[person_id]
        post_url = f"{API_BASE}/attributions"
        payload = {
            "attributionFor": project_id,
            "attributedTo": person_id,
            "attributedAs": rolle
        }
        logger.info(f"POST: {person_id} als {rolle} in Projekt {project_id}")
        response = requests_post(post_url, headers=auth.get_headers(), json=payload, verify=False)
        logger.info(f"POST Status: {response.status_code}")


def main():
    with open("attributions.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    all_attributions = data.get("attributions", {})

    if len(sys.argv) > 1:
        project_id = sys.argv[1]
        if project_id in all_attributions:
            logger.info(f"Synchronisiere nur Projekt {project_id}")
            sync_project_attributions(project_id, all_attributions[project_id])
        else:
            logger.warning(f"Projekt {project_id} nicht in attributions.json gefunden.")
    else:
        logger.info("Starte vollständige Synchronisierung (alle Projekte)")
        for project_id, eintrag in all_attributions.items():
            if isinstance(eintrag, dict) and "personen" in eintrag:
                logger.info(f"Projekt {project_id} – {len(eintrag.get('personen', []))} Ziel-Zuweisungen")
                sync_project_attributions(project_id, eintrag)
            else:
                logger.info(f"Projekt {project_id} wird übersprungen (nicht synchronisierbar oder anderes Format)")

            logger.info(f"Projekt {project_id} – {len(eintrag.get('personen', []))} Ziel-Zuweisungen")
            sync_project_attributions(project_id, eintrag)

    logger.info("Synchronisierung abgeschlossen.")


if __name__ == "__main__":
    main()
