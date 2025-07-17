# Beispiel-Datenstruktur
beispiel = {
  "projects": {
    "12178ec7-3e63-412b-9015-78554401c20d": "Datensatz 100444 anlegen und freigeben",
    "04fff422-969c-4bf8-abfc-d2dd377fe186": "Datensatz 100445 anlegen und freigeben",
    "d6fec291-fe0d-4a59-bdc2-c0346e76e02d": "Datensatz 100446 anlegen und freigeben",
    "bd4aa6b8-09e1-4348-99a7-bf6cda88e725": "Datensatz 100447 anlegen und freigeben",
    "868ef5c7-8bd7-4d6e-ad3d-8aec632c3973": "Test",
    "bdb3fbbe-3ab0-4a08-bb9e-ed8c19b42924": "Hunde",
    "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d": "OGD-Freigaben"
  },
  "persons": {
    "fa57207d-c3ff-4741-9e0d-c638a22584df": "Mohler, Lukas",
    "b3eb6c86-c0a3-4eec-b2ae-1f21a5cabafc": "Gräf, Ulrich",
    "da2b46db-c376-4a61-bef7-cd065cd12cdc": "Carter, Sarah"
  },
  "roles": {
    "02222f05-5690-4cb8-8d90-c27ca57e98e9": "Data Owner",
    "e3ffbec6-86fd-4719-8b7f-5bfd08768a92": "Data Steward",
    "b9c2b21d-4852-440f-9c64-8d1f3984c3fd": "Kantonaler Data Steward"
  },
  "attributions": {
    "12178ec7-3e63-412b-9015-78554401c20d": {
      "_type": "Project",
      "status": "REVIEW3DS",
      "personen": [
        {
          "person": "fa57207d-c3ff-4741-9e0d-c638a22584df",
          "role": "02222f05-5690-4cb8-8d90-c27ca57e98e9"
        },
        {
          "person": "b3eb6c86-c0a3-4eec-b2ae-1f21a5cabafc",
          "role": "e3ffbec6-86fd-4719-8b7f-5bfd08768a92"
        },
        {
          "person": "da2b46db-c376-4a61-bef7-cd065cd12cdc",
          "role": "b9c2b21d-4852-440f-9c64-8d1f3984c3fd"
        }
      ]
    },
    "8386fbc7-2315-4d00-9bf8-47d2b04a6a7d": {
      "_type": None,
      "status": "OGD_ROOT",
      "personen": [
        {
          "person": "da2b46db-c376-4a61-bef7-cd065cd12cdc",
          "role": "b9c2b21d-4852-440f-9c64-8d1f3984c3fd"
        }
      ]
    },
    "04fff422-969c-4bf8-abfc-d2dd377fe186": {
      "_type": "Project",
      "status": "WORKING",
      "personen": [
        {
          "person": "b3eb6c86-c0a3-4eec-b2ae-1f21a5cabafc",
          "role": "e3ffbec6-86fd-4719-8b7f-5bfd08768a92"
        },
        {
          "person": "fa57207d-c3ff-4741-9e0d-c638a22584df",
          "role": "02222f05-5690-4cb8-8d90-c27ca57e98e9"
        }
      ]
    },
    "d6fec291-fe0d-4a59-bdc2-c0346e76e02d": {
      "_type": "Project",
      "status": "WORKING",
      "personen": [
        {
          "person": "b3eb6c86-c0a3-4eec-b2ae-1f21a5cabafc",
          "role": "e3ffbec6-86fd-4719-8b7f-5bfd08768a92"
        },
        {
          "person": "fa57207d-c3ff-4741-9e0d-c638a22584df",
          "role": "02222f05-5690-4cb8-8d90-c27ca57e98e9"
        }
      ]
    },
    "bd4aa6b8-09e1-4348-99a7-bf6cda88e725": {
      "_type": "Project",
      "status": "WORKING",
      "personen": [
        {
          "person": "b3eb6c86-c0a3-4eec-b2ae-1f21a5cabafc",
          "role": "e3ffbec6-86fd-4719-8b7f-5bfd08768a92"
        },
        {
          "person": "fa57207d-c3ff-4741-9e0d-c638a22584df",
          "role": "02222f05-5690-4cb8-8d90-c27ca57e98e9"
        }
      ]
    },
    "868ef5c7-8bd7-4d6e-ad3d-8aec632c3973": {
      "_type": "Project",
      "status": "WORKING",
      "personen": []
    },
    "bdb3fbbe-3ab0-4a08-bb9e-ed8c19b42924": {
      "_type": "Project",
      "status": "WORKING",
      "personen": []
    }
  }
}

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
