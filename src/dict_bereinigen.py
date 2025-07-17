# Beispiel-Datenstruktur
beispiel = {"_embedded": {
    "persons": [
      {
        "_type": "Person",
        "id": "74e55232-f5f2-4370-98e9-d599cb335549",
        "_version": 4,
        "tenantId": "bf6a14c1-ed03-4d8a-beea-c6db320502fd",
        "familyName": "Editor DCC",
        "givenName": "(API)",
        "label": "Editor DCC (API)",
        "title": "(API) Editor DCC",
        "agentOf": "f3948c30-269b-4601-87e7-4c56f63acd5a",
        "memberOf": [],
        "reportsTo": [],
        "holdsPost": [
          "e6593ed8-4753-49c8-a0af-28ef19810459"
        ],
        "publicState": "PUBLIC",
        "modelId": "f3948c30-269b-4601-87e7-4c56f63acd5a",
        "href": "/web/test-sarah-1/persons/74e55232-f5f2-4370-98e9-d599cb335549",
        "status": "PUBLISHED",
        "createdBy": "renato.farruggio@bs.ch",
        "db": "test-sarah-1",
        "dateCreated": 1741092297252,
        "parentId": "f3948c30-269b-4601-87e7-4c56f63acd5a",
        "_links": {
          "self": {
            "href": "/rest/test-sarah-1/persons/74e55232-f5f2-4370-98e9-d599cb335549"
          },
          "agentOf": {
            "href": "/rest/test-sarah-1/organizations/f3948c30-269b-4601-87e7-4c56f63acd5a"
          },
          "holdsPost": [
            {
              "href": "/rest/test-sarah-1/posts/e6593ed8-4753-49c8-a0af-28ef19810459"
            }
          ]
        }
      }
    ]}}

def ersetze_alle_werte_durch_platzhalter(objekt, platzhalter="<WERT>"):
    """
    Ersetzt alle Werte in einem verschachtelten Dict/Listen-Konstrukt durch Platzhalter.
    """
    if isinstance(objekt, dict):
        return {k: ersetze_alle_werte_durch_platzhalter(v, platzhalter) for k, v in objekt.items()}
    elif isinstance(objekt, list):
        return [ersetze_alle_werte_durch_platzhalter(item, platzhalter) for item in objekt]
    else:
        return platzhalter


# Ersetzen
bereinigt = ersetze_alle_werte_durch_platzhalter(beispiel)

# Ausgabe
import json
print(json.dumps(bereinigt, indent=2, ensure_ascii=False))
