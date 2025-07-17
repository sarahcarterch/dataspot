import json
from src.dataspot_auth import DataspotAuth
from src.common import requests_patch

project_id = "12178ec7-3e63-412b-9015-78554401c20d"
url = f"https://bs.dataspot.io/rest/test-sarah-1/projects/{project_id}"
headers = DataspotAuth().get_headers()

payload = {
    "status": "REVIEW3DS",
    "_type": "Project"
}

response = requests_patch(url, headers=headers, json=payload, verify=False)

print("Statuscode:", response.status_code)
try:
    print(json.dumps(response.json(), indent=2))
except Exception:
    print(response.text)
