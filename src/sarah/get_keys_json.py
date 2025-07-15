import json
import yaml

input_json = "workflow_ogd.json"
input_yaml = "docs/Annotation YAML files/annotations_test-api.yaml"
output_status = "workflow_ogd_stati.json"

# Rekursive Suchfunktion einfügen
def find_status_label(obj, status_key):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "literals" and isinstance(v, dict):
                for inner_key, inner_val in v.items():
                    if status_key == inner_key or status_key.startswith(inner_key):
                        return inner_val.get("label")
            result = find_status_label(v, status_key)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_status_label(item, status_key)
            if result:
                return result
    return None

# YAML und JSON laden
with open(input_json, encoding="utf-8") as f:
    data = json.load(f)

with open(input_yaml, encoding="utf-8") as f:
    label_data = yaml.safe_load(f)

# Statuswerte extrahieren
status_infos = []

try:
    statuses = data["definition"]["statuses"]
    for entry in statuses:
        key = entry.get("status")
        label = find_status_label(label_data, key)
        print(f"Status: {key} → Label: {label}")
        if key and label:
            status_infos.append({
                "status": key,
                "label": label
            })
except Exception as e:
    print("Fehler beim Zugriff:", e)

# Duplikate entfernen
unique = { (s["status"], s["label"]) for s in status_infos }
result = [ {"status": s, "label": l} for s, l in sorted(unique) ]
print(result)

print("Status-Infos, die gespeichert werden:")
print(json.dumps(result, indent=2, ensure_ascii=False))

# Speichern
with open(output_status, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
