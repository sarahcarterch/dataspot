import json
from src.dataspot_auth import DataspotAuth
from src.common import requests_post

def erstelle_person(family_name, given_name, agent_of="f3948c30-269b-4601-87e7-4c56f63acd5a"):
    url = "https://bs.dataspot.io/rest/test-sarah-1/persons"
    headers = DataspotAuth().get_headers()

    payload = {
        "familyName": family_name,
        "givenName": given_name,
        "label": f"{family_name} {given_name}",
        "title": f"{given_name} {family_name}",
        "agentOf": agent_of,
        "_type": "Person"
    }

    response = requests_post(url, headers=headers, json=payload, verify=False)

    print("Statuscode:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except Exception:
        print(response.text)
