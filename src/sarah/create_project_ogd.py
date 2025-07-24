import json
from src.dataspot_auth import DataspotAuth
from src.common import requests_post


# Projekt im Verzeichnis OGD-Freigaben erstellen
def erstelle_projekt(label, _type="Project", status = "WORKING", inCollection = "8accbced-0622-403f-a7b7-e4553407be10"):
    url = "https://bs.dataspot.io/rest/test-sarah-1/projects"
    headers = DataspotAuth().get_headers()

    payload = {
        "label": label,
        "inCollection": inCollection,
        "status": status,
        "_type": _type
    }

    response = requests_post(url, headers=headers, json=payload, verify=False)

    print("Statuscode:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
