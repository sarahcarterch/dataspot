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
        -scheme_name: str
        -scheme_name_short: str
        -ods_imports_collection_name: str
        +get_all_assets_from_scheme()
        +_get_asset()
        +_create_asset()
        +_update_asset()
        +_delete_asset()
        +require_scheme_exists()
        +ensure_ods_imports_collection_exists()
        +bulk_create_or_update_assets()
    }

    %% DNK Client
    class DNKClient {
        -org_handler: OrgStructureHandler
        -dataset_handler: DatasetHandler
        +create_dataset()
        +update_dataset()
        +delete_dataset()
        +bulk_create_or_update_datasets()
        +bulk_create_or_update_organizational_units()
        +sync_org_units()
        +sync_datasets()
    }

    %% ODS Client
    class ODSClient {
        -explore_api_version: str
        +get_dataset_columns()
        +get_organization_data()
        +get_all_organization_data()
    }

    %% Handler Classes
    class BaseDataspotHandler {
        -client: BaseDataspotClient
        -mapping: BaseDataspotMapping
        -database_name: str
        -scheme_name: str
        -scheme_name_short: str
        -logger: Logger
        +_download_and_update_mappings()*
        +update_mappings_from_upload()
        +bulk_create_or_update_assets()
        +get_all_external_ids()
    }
    
    class DatasetHandler {
        -ods_dataset_mapping: DatasetMapping
        -ods_imports_collection_name: str
        +sync_datasets()
        +_download_and_update_mappings()
        +get_all_ods_ids()
        +update_mappings_from_upload()
        +bulk_create_or_update_datasets()
        +create_dataset()
        +update_dataset()
        +create_or_update_dataset()
        +delete_dataset()
    }
    
    class OrgStructureHandler {
        -org_mapping: OrgStructureMapping
        +sync_org_units()
        +_download_and_update_mappings()
        +update_staatskalender_mappings_from_upload()
        +get_all_staatskalender_ids()
        +build_organization_hierarchy_from_ods_bulk()
        +transform_organization_for_bulk_upload()
        +bulk_create_or_update_organizational_units()
        +get_validated_staatskalender_url()
        -_sync_staatskalender_org_units()
        -_transform_org_data_to_layers()
        -_fetch_current_org_units()
        -_compare_org_structures()
        -_check_for_unit_changes()
        -_apply_org_unit_changes()
        -_generate_sync_summary()
    }

    %% Mapping Classes    
    class BaseDataspotMapping {
        -database_name: str
        -csv_file_path: str
        -mapping: Dict
        -_id_field_name: str
        -_file_prefix: str
        -_scheme: str
        +id_field_name: property
        +csv_headers: property
        +get_entry()
        +add_entry()
        +remove_entry()
        +get_type()
        +get_uuid()
        +get_inCollection()
        +get_all_entries()
        +get_all_ids()
        -_get_mapping_file_path()
        -_load_mapping()
        -_save_mapping()
        -_is_valid_uuid()
    }

    class DatasetMapping {
        +get_all_ods_ids()
    }

    class OrgStructureMapping {
        +get_all_staatskalender_ids()
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

    %% HTTP Request Function Modules
    class common {
        <<module>>
        +requests_get()
        +requests_post()
        +requests_put()
        +requests_patch()
        +requests_delete()
    }

    %% Retry Utilities
    class retry {
        <<function>>
        +retry(ExceptionToCheck, tries, delay, backoff, logger)
    }

    %% Helper Function Module
    class helpers {
        <<module>>
        +url_join()
        +get_uuid_from_response()
        +escape_special_chars()
        +generate_potential_staatskalender_url()
    }

    %% Transformer Function Module
    class dataset_transformer {
        <<module>>
        +transform_ods_to_dnk()
        +_iso_8601_to_unix_timestamp()
        +_get_field_value()
    }

    %% Relationships
    BaseDataspotClient o-- DataspotAuth : uses
    DNKClient --|> BaseDataspotClient : extends
    DNKClient o-- OrgStructureHandler : uses
    DNKClient o-- DatasetHandler : uses
    BaseDataspotClient ..> common : uses
    DataspotAuth ..> common : uses
    common ..> retry : uses
    
    %% Dataset relationships
    Dataset <|-- BasicDataset : extends
    BasicDataset <|-- OGDDataset : extends
    dataset_transformer ..> OGDDataset : creates
    ODSClient ..> dataset_transformer : uses
    DatasetHandler ..> Dataset : uses
    
    %% Handler relationships
    BaseDataspotHandler o-- BaseDataspotClient : uses
    BaseDataspotHandler o-- BaseDataspotMapping : uses
    DatasetHandler --|> BaseDataspotHandler : extends
    OrgStructureHandler --|> BaseDataspotHandler : extends
    DatasetHandler o-- DatasetMapping : uses
    OrgStructureHandler o-- OrgStructureMapping : uses
    
    %% Mapping relationships
    BaseDataspotMapping <|-- DatasetMapping : extends
    BaseDataspotMapping <|-- OrgStructureMapping : extends
    
    %% Helper relationships
    BaseDataspotClient ..> helpers : uses
    ODSClient ..> helpers : uses
    OrgStructureHandler ..> helpers : uses
    DatasetHandler ..> helpers : uses
```

### Key Components:

1. **Authentication (DataspotAuth)**: Handles OAuth token management for Dataspot API access.

2. **Clients**:
   - **BaseDataspotClient**: Abstract base class providing common functionality for Dataspot API interaction.
   - **DNKClient**: Extends `BaseDataspotClient` to specifically work with the Datennutzungskatalog (DNK). Delegates handling to specialized handlers.
   - **ODSClient**: Interfaces with the OpenDataSoft API to retrieve dataset and organization information.

3. **Handlers**:
   - **BaseDataspotHandler**: Abstract base class for handlers that manage different types of assets in Dataspot.
   - **DatasetHandler**: Extends `BaseDataspotHandler` to handle dataset synchronization operations.
   - **OrgStructureHandler**: Extends `BaseDataspotHandler` to handle organizational unit synchronization operations.

4. **Data Models**:
   - **Dataset**: Abstract base class for all dataset types.
   - **BasicDataset**: Extends Dataset with basic metadata fields.
   - **OGDDataset**: Extends BasicDataset with Open Government Data specific fields.

5. **Mapping**:
   - **BaseDataspotMapping**: Base class providing mapping functionality for external IDs to Dataspot UUIDs.
   - **DatasetMapping**: Extends BaseDataspotMapping to specifically map ODS dataset IDs to Dataspot UUIDs.
   - **OrgStructureMapping**: Extends BaseDataspotMapping to map Staatskalender organization IDs to Dataspot UUIDs.

6. **HTTP Utilities**:
   - **common**: Module providing standardized HTTP request functions with consistent error handling.
   - **retry**: Decorator function implementing retry logic for HTTP requests that may experience transient failures.

7. **Utility Modules**:
   - **helpers**: Module containing utility functions for URL manipulation, response parsing, and special character handling.
   - **dataset_transformer**: Module containing functions to convert ODS metadata format to Dataspot DNK format.

### Data Flow:

1. **Dataset Synchronization**:
   - The process begins with fetching dataset metadata from OpenDataSoft using ODSClient.
   - This metadata is transformed into Dataspot's format using functions in the dataset_transformer module.
   - The DNKClient delegates operations to DatasetHandler, which uses DatasetMapping to track relationships between systems.
   - DatasetHandler creates, updates, or deletes datasets in Dataspot's DNK, with each operation requiring authentication via DataspotAuth.
   - For bulk operations, multiple datasets can be processed in a single API call.

2. **Organizational Unit Synchronization**:
   - Organization data is retrieved from OpenDataSoft via ODSClient.
   - DNKClient delegates operations to OrgStructureHandler, which transforms the flat organization data into a hierarchical structure.
   - The hierarchical data is uploaded to Dataspot level by level to preserve parent-child relationships.
   - OrgStructureMapping is used to track the mapping between Staatskalender IDs and Dataspot UUIDs.

This architecture enables synchronization of both datasets and organizational units between OpenDataSoft and Dataspot while maintaining mappings between the systems.

### Note on Debugging Code

The repository contains a `renato_debugging.py` file which is not part of the core system architecture. This file contains temporary debugging code and should not be considered part of the production system.
