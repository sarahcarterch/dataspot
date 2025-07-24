import json
from src.dataspot_auth import DataspotAuth
from src.common import requests_post, requests_get

def erstelle_konto(name, loginId, accessLevel = "EDITOR", _type = "User"):
    url = "https://bs.dataspot.io/rest/test-sarah-1/users"
    url_p = "https://bs.dataspot.io/rest/test-sarah-1/persons"

    headers = DataspotAuth().get_headers()

    request = requests_get(url_p, headers=headers).json()
    personen = request.get("_embedded", {}).get("persons", [])
    gefundene_person = next((p for p in personen if p.get("title", "") == name), None)
    if gefundene_person:
        person_id = gefundene_person.get("id")
        print("Gefundene ID:", person_id)
    else:
        print("Keine passende Person gefunden.")

    payload = {
        "loginId": loginId,
        "isPerson": person_id,
        "accessLevel": accessLevel,
        "_type": _type
    }

    response = requests_post(url, headers=headers, json=payload, verify=False)

    print("Statuscode:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
