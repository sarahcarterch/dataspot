# README.md

## How to do regular updates (not yet implemented):
<!-- Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot. but instead through a file managed by the Data Competence Center DCC. This means that fields that should be updated outside of the workflow are written to the centrally managed file instead of dataspot directly. These changes are then regularly updated by a script from the DCC to dataspot. The key should always be the dataspot-internal UUID. Dates should be provided as Unix timestamps in in UTC timezone. Times should be provided in Unix timestamps aswell in a ??? format (TBD; the same as is used internally in dataspot.). TODO: Add examples -->

Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot, as this does not work with the workflow. Instead, the changes are pushed to a non-public dataset on [opendatasoft](data.bs.ch). Please [get in touch](mailto:opendata@bs.ch) with us for the setup.

The columns should be (so far): uuid,lastactl,lastpub

This is put on hold for the moment, as lastactl does not really need to be in dataspot. (?)

## How to integrate an `int` environment into `prod` (work-in-progress)
When integrating a `dev` into `prod`, first we need to clone the `dev` into an `int`. 
Then:
1. Integrate yaml
1. Run job "Regelverletzungen prüfen"
1. Export DNK as xlsx and import it again (dry run is enough)
1. Export and reimport other models that might be affected aswell

If everything worked without errors, we can apply the `int` yaml into the `prod` yaml and reapply the changes made to the `int` to the `prod`.

After that, delete the `dev` branch on github, in dataspot, and also its corresponding Annotations.yaml

## Notes (Renato)
### Colors for top bar background:
Not explicitely defined, this is written here just so that I know what I used before. You can use whatever you want.
- dev: 167a3e
- int: ff0000

## System Architecture Overview

This project facilitates data synchronization between OpenDataSoft (ODS) and Dataspot's Datennutzungskatalog (DNK). Below is a UML diagram showing the main components and their relationships:

```mermaid
classDiagram
    %% Core Authentication
    class DataspotAuth {
        -token_url: str
        -client_id: str
        -username: str
        -password: str
        -token: str
        -token_expires_at: datetime
        +get_bearer_token()
        +get_headers()
        -_is_token_valid()
        -_request_new_bearer_token()
    }

    %% Base Client
    class BaseDataspotClient {
        <<abstract>>
        -auth: DataspotAuth
        -base_url: str
        -database_name: str
        -ods_imports_collection_name: str
        +require_scheme_exists()* 
        +create_resource()
        +bulk_create_or_update_resources()
        +update_resource()
        +delete_resource()
        +get_resource_if_exists()
    }

    %% DNK Client
    class DNKClient {
        -scheme_name: str
        -mapping: ODSDataspotMapping
        +require_scheme_exists()
        +ensure_ods_imports_collection_exists()
        +create_dataset()
        +update_dataset()
        +create_or_update_dataset()
        +bulk_create_or_update_datasets()
        +delete_dataset()
        +update_mappings_from_upload()
        -_download_and_update_mappings()
    }

    %% ODS Client
    class ODSClient {
        -explore_api_version: str
        +get_dataset_columns()
        +get_organization_data()
    }

    %% Mapping
    class ODSDataspotMapping {
        -csv_file_path: str
        -mapping: Dict
        +get_entry()
        +add_entry()
        +remove_entry()
        +get_uuid()
        +get_href()
        -_load_mapping()
        -_save_mapping()
    }

    %% Dataset Classes
    class Dataset {
        <<abstract>>
        +name: str
        +_type: str
        +to_json()*
    }

    class BasicDataset {
        +beschreibung: str
        +schluesselwoerter: List[str]
        +aktualisierungszyklus: str
        +geographische_dimension: str
        +publikationsdatum: int
        +to_json()
        +from_json()
    }

    class OGDDataset {
        +lizenz: List[str]
        +nutzungsrechte: str
        +datenportal_identifikation: str
        +tags: List[str]
        +stereotype: str
    }

    %% Helper Functions
    class Helpers {
        <<static>>
        +url_join()
        +get_uuid_and_href_from_response()
        +generate_potential_staatskalender_url()
        +escape_special_chars()
    }

    %% Transformer Functions
    class DatasetTransformer {
        <<static>>
        +transform_ods_to_dnk()
        +_iso_8601_to_unix_timestamp()
        +_get_field_value()
    }

    %% Relationships
    BaseDataspotClient o-- DataspotAuth : uses
    DNKClient --|> BaseDataspotClient : extends
    DNKClient o-- ODSDataspotMapping : uses
    DNKClient ..> Helpers : uses
    DNKClient ..> Dataset : creates/updates
    Dataset <|-- BasicDataset : extends
    BasicDataset <|-- OGDDataset : extends
    DatasetTransformer ..> OGDDataset : creates
    ODSClient ..> DatasetTransformer : data feeds into
```

### Key Components:

1. **Authentication (DataspotAuth)**: Handles OAuth token management for Dataspot API access.

2. **Clients**:
   - **BaseDataspotClient**: Abstract base class providing common functionality for Dataspot API interaction.
   - **DNKClient**: Extends BaseDataspotClient to specifically work with the Datennutzungskatalog (DNK).
   - **ODSClient**: Interfaces with the OpenDataSoft API to retrieve dataset information.

3. **Data Models**:
   - **Dataset**: Abstract base class for all dataset types.
   - **BasicDataset**: Extends Dataset with basic metadata fields.
   - **OGDDataset**: Extends BasicDataset with Open Government Data specific fields.

4. **Mapping (ODSDataspotMapping)**: Maintains a persistent mapping between ODS dataset IDs and Dataspot UUIDs/hrefs in a CSV file.

5. **Transformation (DatasetTransformer)**: Contains functions to convert ODS metadata format to Dataspot DNK format.

6. **Helpers**: Utility functions for URL manipulation, response parsing, and special character handling.

### Data Flow:

1. The process begins with fetching dataset metadata from OpenDataSoft using ODSClient.
2. This metadata is transformed into Dataspot's format using functions in DatasetTransformer.
3. The DNKClient uses ODSDataspotMapping to track relationships between systems.
4. DNKClient creates, updates, or deletes datasets in Dataspot's DNK, with each operation requiring authentication via DataspotAuth.
5. For bulk operations, multiple datasets can be processed in a single API call.

This architecture enables synchronization of datasets between OpenDataSoft and Dataspot while maintaining a mapping between the two systems.
