# README.md

---
## Setup
Add two files `config.py` and `.env` in the root folder. Add a `config.py` with the following content:

```config.py
# Dataspot configuration variables

# Database name for features and testing
database_name = 'prod'

# Base URL
base_url = 'https://www.myserver.com'

# Prod database name
database_name_prod = 'prod'

# Scheme names
dnk_scheme_name = 'Datennutzungskatalog'
dnk_scheme_name_short = 'DNK'
fdm_scheme_name = 'Fachdatenmodell'
fdm_scheme_name_short = 'FDM'
rdm_scheme_name = 'Referenzdatenmodell'
rdm_scheme_name_short = 'RDM'
datatype_scheme_name = 'Datentypmodell (technisch)'
datatype_scheme_name_short = 'DTM'
tdm_scheme_name = 'Technische Datenmodelle'
tdm_scheme_name_short = 'TDM'

# Default ODS Imports collection name
ods_imports_collection_name = 'OGD-Datensätze aus ODS'

# Default path for the ODS Imports collection within the DNK scheme.
# An empty list [] means it's directly under the scheme root.
# Example: ["Parent Folder", "Sub Folder"] places it under Parent Folder/Sub Folder.
ods_imports_collection_path = []

# Validate that critical configuration values are present
assert database_name, "database_name must be set in config.py"
assert base_url, "base_url must be set in config.py"
assert database_name_prod, "database_name_prod must be set in config.py"
assert dnk_scheme_name, "dnk_scheme_name must be set in config.py"
assert rdm_scheme_name, "rdm_scheme_name must be set in config.py"
assert datatype_scheme_name, "datatype_scheme_name must be set in config.py"
assert tdm_scheme_name, "tdm_scheme_name must be set in config.py"
assert ods_imports_collection_name, "ods_imports_collection_name must be set in config.py"
```


Add a `.env` with the following values:
```.env
DATASPOT_EMAIL_RECEIVERS=["petra.muster@bs.ch", "peter.muster@bs.ch]
DATASPOT_EMAIL_SERVER=
DATASPOT_EMAIL_SENDER=



DATASPOT_EDITOR_USERNAME=
DATASPOT_EDITOR_PASSWORD=
DATASPOT_ADMIN_USERNAME=
DATASPOT_ADMIN_PASSWORD=

DATASPOT_CLIENT_ID=
DATASPOT_AUTHENTICATION_TOKEN_URL=https://login.microsoftonline.com/organizations/oauth2/v2.0/token
DATASPOT_API_BASE_URL=https://www.myserver.com
```

---
## How to integrate code from a `dev` (or `feature`) environment into `prod` [Work-In-Progress]
When integrating a `dev` into `prod`, first we need to clone the `dev` into an `int` database.

Then:
1. Export DNK from `dev` as xlsx and import it again (dry run is enough).
1. If we don't fix warnings or errors that occur, then they will appear later again.
1. Integrate yaml from `dev` into `int`
1. Run job "Regelverletzungen prüfen"
1. Export DNK as xlsx and import it again (dry run is enough)
1. Export and reimport other models that might be affected aswell
1. Merge `dev` into `main` and delete `dev` branch

If everything worked without errors, we can apply the `int` yaml into the `prod` yaml and reapply the changes made to the `int` to the `prod`.

After that, delete the `dev` branch on github, in dataspot, and also its corresponding Annotations.yaml. Also delete the `int` environment in dataspot.

---
## Notes (Renato)
### Colors for top bar background:
Not explicitely defined, this is written here just so that I know what I used before. You can use whatever you want.
- dev: 167a3e
- int: ff0000

---


<!-- 
## How to do regular updates (not yet implemented):
Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot. but instead through a file managed by the Data Competence Center DCC. This means that fields that should be updated outside of the workflow are written to the centrally managed file instead of dataspot directly. These changes are then regularly updated by a script from the DCC to dataspot. The key should always be the dataspot-internal UUID. Dates should be provided as Unix timestamps in in UTC timezone. Times should be provided in Unix timestamps aswell in a ??? format (TBD; the same as is used internally in dataspot.). TODO: Add examples
**(put on hold)**

Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot, as this does not work with the workflow. Instead, the changes are pushed to a non-public dataset on [opendatasoft](data.bs.ch). Please [get in touch](mailto:opendata@bs.ch) with us for the setup.

The columns should be (so far): uuid,lastactl,lastpub

This is put on hold for the moment, as lastactl does not really need to be in dataspot. (?)
---
-->
