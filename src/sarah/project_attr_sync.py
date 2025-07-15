import json
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


def fetch_all_attributions():
    """Holt alle Attributions direkt von der API"""
    url = f"{API_BASE}/attributions"
    headers = auth.get_headers()

    try:
        response = requests_get(url, headers=headers, verify=False)
        logger.debug(f"GET {url} → {response.status_code}")
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Antwort JSON:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
        return data
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Attributions: {e}")
        return {}


def sync_project_attributions(project_id, ziel_attributions):
    logger.debug(f"\n---\nStarte Sync für Projekt: {project_id}")
    logger.debug(f"Zielattributions (roh): {json.dumps(ziel_attributions, indent=2)}")

    api_response = fetch_all_attributions()

    if not api_response or "_embedded" not in api_response or "attributions" not in api_response["_embedded"]:
        logger.warning(f"Keine Attributions in API-Antwort für Projekt {project_id}, überspringe.")
        return

    all_api_attributions = api_response["_embedded"]["attributions"]
    logger.debug(f"Alle geladenen Attributions: {len(all_api_attributions)}")

    api_entries = [
        a for a in all_api_attributions
        if a.get("attributionFor", "").strip() == project_id.strip()
    ]
    logger.debug(f"Gefilterte API-Attributions für Projekt {project_id}: {json.dumps(api_entries, indent=2)}")

    ziel_by_person = {a["person"].strip(): a["role"].strip() for a in ziel_attributions}
    api_by_person = {
        a.get("attributedTo", '').strip(): a.get("attributedAs", '').strip()
        for a in api_entries if a.get("attributedTo")
    }

    ziel_personen = set(ziel_by_person.keys())
    api_personen = set(api_by_person.keys())

    logger.debug(f"Zielpersonen: {sorted(ziel_personen)}")
    logger.debug(f"API-Personen: {sorted(api_personen)}")
    logger.debug(f"ziel_by_person = {json.dumps(ziel_by_person, indent=2)}")
    logger.debug(f"api_by_person  = {json.dumps(api_by_person, indent=2)}")

    for zp in ziel_personen:
        if zp not in api_by_person:
            logger.debug(f"Person {zp} ist NICHT in API vorhanden (POST geplant)")
        elif ziel_by_person[zp] != api_by_person[zp]:
            logger.debug(f"Person {zp} hat unterschiedliche Rollen: Ziel={ziel_by_person[zp]}, API={api_by_person[zp]} (PATCH geplant)")
        else:
            logger.debug(f"Person {zp} ist korrekt zugewiesen, wird übersprungen")

    # DELETE
    for person_id in api_personen - ziel_personen:
        rolle = api_by_person[person_id]
        # Passende attribution_id finden
        attribution_id = next(
            (a.get("id") for a in api_entries
            if a.get("attributedTo") == person_id and a.get("attributedAs") == rolle),
            None
        )

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
            attribution_id = next(
                (a.get("id") for a in api_entries if a.get("attributedTo") == person_id),
                None
            )
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
    logger.info("Starte Attributions-Synchronisierung (PATCH, POST, DELETE)")

    with open("attributions.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    all_attributions = data.get("attributions", {})

    for project_id, eintraege in all_attributions.items():
        logger.info(f"Projekt {project_id} – {len(eintraege)} Ziel-Zuweisungen")
        sync_project_attributions(project_id, eintraege)

    logger.info("Synchronisierung abgeschlossen.")


if __name__ == "__main__":
    main()
