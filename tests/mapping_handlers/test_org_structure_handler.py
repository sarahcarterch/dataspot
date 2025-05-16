import pytest
from unittest.mock import MagicMock, patch

from src.clients.base_client import BaseDataspotClient
from src.mapping_handlers.org_structure_handler import OrgStructureHandler
from src.mapping_handlers.org_structure_helpers.org_structure_updater import OrgStructureUpdater


@pytest.fixture
def mock_client():
    """Create a mock BaseDataspotClient instance."""
    client = MagicMock(spec=BaseDataspotClient)
    client.database_name = "test_db"
    client.scheme_name = "test_scheme"
    client.scheme_name_short = "TST"
    return client


@pytest.fixture
def handler(mock_client):
    """Create an OrgStructureHandler instance with a mock client."""
    handler = OrgStructureHandler(mock_client)
    # Mock the mapping to avoid file system operations
    handler.mapping.save_to_csv = MagicMock()
    handler.mapping.load_from_csv = MagicMock()
    handler.mapping.mapping = {}
    return handler


@pytest.fixture
def sample_org_data():
    """Sample organization data from ODS API."""
    return {
        "results": [
            {
                "id": "1",
                "title": "Root Organization",
                "parent_id": None,
                "url_website": "https://example.com/org/1"
            },
            {
                "id": "2",
                "title": "Child Organization",
                "parent_id": "1",
                "url_website": "https://example.com/org/2"
            }
        ]
    }


@pytest.fixture
def sample_dataspot_units():
    """Sample organizational units from Dataspot."""
    return [
        {
            "id": "uuid-1",
            "_type": "Collection",
            "stereotype": "Organisationseinheit",
            "label": "Root Organization",
            "id_im_staatskalender": "1",
            "link_zum_staatskalender": "https://example.com/org/1"
        }
    ]


@pytest.fixture
def sample_transformed_units_by_layer():
    """Sample transformed organizational units by layer."""
    return {
        0: [
            {
                "_type": "Collection",
                "stereotype": "Organisationseinheit",
                "label": "Root Organization",
                "id_im_staatskalender": "1",
                "customProperties": {
                    "id_im_staatskalender": "1",
                    "link_zum_staatskalender": "https://example.com/org/1"
                }
            }
        ],
        1: [
            {
                "_type": "Collection",
                "stereotype": "Organisationseinheit",
                "label": "Child Organization",
                "id_im_staatskalender": "2",
                "inCollection": "Root Organization",
                "customProperties": {
                    "id_im_staatskalender": "2",
                    "link_zum_staatskalender": "https://example.com/org/2"
                }
            }
        ]
    }


def test_init(handler):
    """Test initialization of OrgStructureHandler."""
    assert handler.asset_id_field == "id_im_staatskalender"
    assert callable(handler.asset_type_filter)
    assert isinstance(handler.updater, OrgStructureUpdater)


def test_fetch_current_org_units(handler, sample_dataspot_units):
    """Test fetching current org units."""
    # Setup
    handler.client.get_all_assets_from_scheme.return_value = sample_dataspot_units
    
    # Execute
    result = handler._fetch_current_org_units()
    
    # Verify
    assert result == sample_dataspot_units
    handler.client.get_all_assets_from_scheme.assert_called_once_with(handler.asset_type_filter)


@patch('src.mapping_handlers.org_structure_helpers.org_structure_transformer.OrgStructureTransformer.transform_to_layered_structure')
def test_build_organization_hierarchy_from_ods_bulk(mock_transform, handler, sample_org_data, sample_transformed_units_by_layer):
    """Test building organization hierarchy using bulk upload."""
    # Setup
    mock_transform.return_value = sample_transformed_units_by_layer
    handler.bulk_create_or_update_organizational_units = MagicMock(return_value=[{"level": "INFO", "message": "Success"}])
    handler.update_mappings_after_upload = MagicMock(return_value=2)
    
    # Execute
    result = handler.build_organization_hierarchy_from_ods_bulk(sample_org_data)
    
    # Verify
    mock_transform.assert_called_once_with(sample_org_data)
    assert handler.bulk_create_or_update_organizational_units.call_count == 2  # One for each depth
    assert handler.update_mappings_after_upload.call_count == 1
    assert handler.mapping.save_to_csv.call_count == 1
    assert result["status"] == "success"
    assert "levels_processed" in result


@patch('src.mapping_handlers.org_structure_helpers.org_structure_transformer.OrgStructureTransformer.transform_to_layered_structure')
@patch('src.mapping_handlers.org_structure_helpers.org_structure_comparer.OrgStructureComparer.compare_structures')
@patch('src.mapping_handlers.org_structure_helpers.org_structure_comparer.OrgStructureComparer.generate_detailed_sync_report')
def test_sync_org_units_incremental(
    mock_generate_report, mock_compare, mock_transform, 
    handler, sample_org_data, sample_dataspot_units
):
    """Test incremental synchronization of org units."""
    # Setup
    handler._fetch_current_org_units = MagicMock(return_value=sample_dataspot_units)
    handler.update_mappings_before_upload = MagicMock()
    handler.update_mappings_after_upload = MagicMock()
    handler._download_and_update_mappings = MagicMock()
    
    source_units_by_layer = {0: [{"id_im_staatskalender": "1"}], 1: [{"id_im_staatskalender": "2"}]}
    mock_transform.return_value = source_units_by_layer
    
    changes = [MagicMock(staatskalender_id="1"), MagicMock(staatskalender_id="2")]
    mock_compare.return_value = changes
    
    expected_summary = {"status": "success", "message": "Test summary"}
    mock_generate_report.return_value = expected_summary
    
    handler.updater.apply_changes = MagicMock()
    
    # Execute
    result = handler.sync_org_units(sample_org_data)
    
    # Verify
    handler._fetch_current_org_units.assert_called_once()
    handler.update_mappings_before_upload.assert_called_once()
    mock_transform.assert_called_once_with(sample_org_data)
    mock_compare.assert_called_once()
    handler.updater.apply_changes.assert_called_once_with(changes, is_initial_run=False)
    handler.update_mappings_after_upload.assert_called_once()
    handler.mapping.save_to_csv.assert_called_once()
    mock_generate_report.assert_called_once_with(changes)
    assert result == expected_summary


def test_sync_org_units_initial(handler, sample_org_data):
    """Test initial sync when no org units exist."""
    # Setup
    handler._fetch_current_org_units = MagicMock(return_value=[])  # No existing units
    handler.update_mappings_before_upload = MagicMock()
    handler.build_organization_hierarchy_from_ods_bulk = MagicMock(
        return_value={"status": "success", "message": "Initial upload successful"}
    )
    handler.update_mappings_after_upload = MagicMock()
    
    # Execute
    result = handler.sync_org_units(sample_org_data)
    
    # Verify
    handler._fetch_current_org_units.assert_called_once()
    handler.update_mappings_before_upload.assert_called_once()
    handler.build_organization_hierarchy_from_ods_bulk.assert_called_once_with(sample_org_data)
    assert result["status"] == "success"
    assert "Performed initial bulk upload" in result["message"]


def test_sync_org_units_no_changes(handler, sample_org_data, sample_dataspot_units):
    """Test sync when no changes are detected."""
    # Setup
    handler._fetch_current_org_units = MagicMock(return_value=sample_dataspot_units)
    handler.update_mappings_before_upload = MagicMock()
    handler.update_mappings_after_upload = MagicMock()
    
    mock_transform = MagicMock(return_value={})
    with patch('src.mapping_handlers.org_structure_helpers.org_structure_transformer.OrgStructureTransformer.transform_to_layered_structure', mock_transform):
        mock_compare = MagicMock(return_value=[])  # No changes
        with patch('src.mapping_handlers.org_structure_helpers.org_structure_comparer.OrgStructureComparer.compare_structures', mock_compare):
            
            # Execute
            result = handler.sync_org_units(sample_org_data)
    
    # Verify
    assert result["status"] == "no_changes"
    handler.update_mappings_after_upload.assert_not_called()
