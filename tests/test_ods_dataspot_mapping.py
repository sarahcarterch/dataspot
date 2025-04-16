import os
import unittest
import uuid
import tempfile
from src.ods_dataspot_mapping import ODSDataspotMapping


class TestODSDataspotMapping(unittest.TestCase):
    """Test case for the ODSDataspotMapping class."""
    
    def setUp(self):
        # Use a temporary test file
        self.test_file = "test_mapping.csv"
        self.mapping = ODSDataspotMapping(csv_file_path=self.test_file)
        
    def tearDown(self):
        # Clean up test file
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
            
    def test_add_and_retrieve_entry(self):
        # Test adding and retrieving an entry
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7016"
        test_href = f"/rest/local-test-environment/datasets/{test_uuid}"
        test_inCollection = "Test Collection"
        self.mapping.add_entry("ods-123", test_uuid, test_href, test_inCollection)
        
        # Retrieve the entry
        entry = self.mapping.get_entry("ods-123")
        self.assertEqual(entry, (test_uuid, test_href, test_inCollection))
        
        # Test convenience methods
        self.assertEqual(self.mapping.get_uuid("ods-123"), test_uuid)
        self.assertEqual(self.mapping.get_href("ods-123"), test_href)
        self.assertEqual(self.mapping.get_inCollection("ods-123"), test_inCollection)
        
        # Test without inCollection parameter
        test_uuid2 = "daeb7cb4-3279-46c5-b7cc-19e0c58d7016"
        test_href2 = f"/rest/local-test-environment/datasets/{test_uuid2}"
        self.mapping.add_entry("ods-124", test_uuid2, test_href2)
        
        # Retrieve the entry
        entry2 = self.mapping.get_entry("ods-124")
        self.assertEqual(entry2, (test_uuid2, test_href2, None))
        self.assertIsNone(self.mapping.get_inCollection("ods-124"))
        
    def test_remove_entry(self):
        # Add an entry
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7017"
        test_href = f"/rest/local-test-environment/datasets/{test_uuid}"
        test_inCollection = "Test Collection"
        self.mapping.add_entry("ods-456", test_uuid, test_href, test_inCollection)
        
        # Remove it
        result = self.mapping.remove_entry("ods-456")
        self.assertTrue(result)
        
        # Check that it's gone
        self.assertIsNone(self.mapping.get_entry("ods-456"))
        
        # Try removing a non-existent entry
        result = self.mapping.remove_entry("nonexistent")
        self.assertFalse(result)
        
    def test_persistence(self):
        # Add entries
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7018"
        uuid2 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7019"
        href1 = f"/rest/local-test-environment/datasets/{uuid1}"
        href2 = f"/rest/local-test-environment/datasets/{uuid2}"
        inColl1 = "Collection 1"
        inColl2 = "Collection 2/Subcollection"
        
        self.mapping.add_entry("ods-789", uuid1, href1, inColl1)
        self.mapping.add_entry("ods-abc", uuid2, href2, inColl2)
        
        # Create a new instance that should load from the same file
        new_mapping = ODSDataspotMapping(csv_file_path=self.test_file)
        
        # Check that entries were loaded
        self.assertEqual(new_mapping.get_uuid("ods-789"), uuid1)
        self.assertEqual(new_mapping.get_href("ods-abc"), href2)
        self.assertEqual(new_mapping.get_inCollection("ods-789"), inColl1)
        self.assertEqual(new_mapping.get_inCollection("ods-abc"), inColl2)

    def test_update_existing_entry(self):
        # Add an initial entry
        old_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7020"
        new_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7021"
        old_href = f"/rest/local-test-environment/datasets/{old_uuid}"
        new_href = f"/rest/local-test-environment/datasets/{new_uuid}"
        old_inColl = "Old Collection"
        new_inColl = "New Collection"
        
        self.mapping.add_entry("ods-update", old_uuid, old_href, old_inColl)
        
        # Update with new values
        self.mapping.add_entry("ods-update", new_uuid, new_href, new_inColl)
        
        # Check that the update worked
        entry = self.mapping.get_entry("ods-update")
        self.assertEqual(entry, (new_uuid, new_href, new_inColl))
        
        # Update only uuid and href, keeping the same inCollection
        newest_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7022"
        newest_href = f"/rest/local-test-environment/datasets/{newest_uuid}"
        
        # Test updating without specifying inCollection
        self.mapping.add_entry("ods-update", newest_uuid, newest_href)
        
        # inCollection should be None
        entry = self.mapping.get_entry("ods-update")
        self.assertEqual(entry, (newest_uuid, newest_href, None))
    
    def test_special_characters_in_ods_id(self):
        # Test with special characters in ODS IDs
        special_ods = "ods/special#char"
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7022"
        test_href = f"/rest/local-test-environment/datasets/{test_uuid}"
        test_inCollection = "Collection with / and spaces"
        
        self.mapping.add_entry(special_ods, test_uuid, test_href, test_inCollection)
        
        # Verify retrieval works
        entry = self.mapping.get_entry(special_ods)
        self.assertEqual(entry, (test_uuid, test_href, test_inCollection))
    
    def test_uuid_format(self):
        # Test with proper UUID format
        valid_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7023"
        valid_href = f"/rest/local-test-environment/datasets/{valid_uuid}"
        valid_inCollection = "Test Collection"
        
        self.mapping.add_entry("ods-valid", valid_uuid, valid_href, valid_inCollection)
        entry = self.mapping.get_entry("ods-valid")
        self.assertEqual(entry, (valid_uuid, valid_href, valid_inCollection))
        
        # Test invalid UUID format
        invalid_uuid = "not-a-uuid"
        valid_href = "/rest/local-test-environment/datasets/caeb7cb4-3279-46c5-b7cc-19e0c58d7027"
        valid_inCollection = "Test Collection"
        
        result = self.mapping.add_entry("ods-invalid-uuid", invalid_uuid, valid_href, valid_inCollection)
        self.assertFalse(result)
        self.assertIsNone(self.mapping.get_entry("ods-invalid-uuid"))
        
        # Test that a random generated UUID works too
        random_uuid = str(uuid.uuid4())
        random_href = f"/rest/local-test-environment/datasets/{random_uuid}"
        random_inCollection = "Random Collection"
        self.mapping.add_entry("ods-random", random_uuid, random_href, random_inCollection)
        self.assertEqual(self.mapping.get_uuid("ods-random"), random_uuid)
        self.assertEqual(self.mapping.get_inCollection("ods-random"), random_inCollection)
    
    def test_nonexistent_entry(self):
        # Test behavior when requesting non-existent entries
        self.assertIsNone(self.mapping.get_entry("does-not-exist"))
        self.assertIsNone(self.mapping.get_uuid("does-not-exist"))
        self.assertIsNone(self.mapping.get_href("does-not-exist"))
        self.assertIsNone(self.mapping.get_inCollection("does-not-exist"))
    
    def test_empty_file(self):
        # Test with an empty file (just headers)
        empty_file = "empty_mapping.csv"
        
        try:
            # Create a new empty mapping
            empty_mapping = ODSDataspotMapping(csv_file_path=empty_file)
            
            # Should not have any entries
            self.assertIsNone(empty_mapping.get_entry("any-id"))
            
            # Should be able to add entries
            test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7024"
            test_href = f"/rest/local-test-environment/datasets/{test_uuid}"
            test_inCollection = "Empty File Collection"
            empty_mapping.add_entry("ods-empty", test_uuid, test_href, test_inCollection)
            self.assertEqual(empty_mapping.get_uuid("ods-empty"), test_uuid)
            self.assertEqual(empty_mapping.get_inCollection("ods-empty"), test_inCollection)
        finally:
            # Clean up
            if os.path.exists(empty_file):
                os.remove(empty_file)
    
    def test_file_does_not_exist(self):
        # Test with a non-existent file path
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_file = os.path.join(temp_dir, "non_existent.csv")
            
            # Should create the file and add headers
            non_existent_mapping = ODSDataspotMapping(csv_file_path=non_existent_file)
            
            # Check that the file was created
            self.assertTrue(os.path.exists(non_existent_file))
            
            # Should be empty
            self.assertIsNone(non_existent_mapping.get_entry("any-id"))
    
    def test_database_name_parameter(self):
        # Test the database_name parameter
        database_name = "test_db"
        expected_filename = f"ods-dataspot-mapping_{database_name}.csv"
        
        try:
            # Create mapping with database_name
            db_mapping = ODSDataspotMapping(database_name=database_name)
            
            # Check that the correct file was created
            self.assertTrue(os.path.exists(expected_filename))
            self.assertEqual(db_mapping.csv_file_path, expected_filename)
            
            # Should be able to add and retrieve entries
            test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7025"
            test_href = f"/rest/local-test-environment/datasets/{test_uuid}"
            test_inCollection = "Database Collection"
            db_mapping.add_entry("ods-db", test_uuid, test_href, test_inCollection)
            self.assertEqual(db_mapping.get_uuid("ods-db"), test_uuid)
            self.assertEqual(db_mapping.get_inCollection("ods-db"), test_inCollection)
        finally:
            # Clean up
            if os.path.exists(expected_filename):
                os.remove(expected_filename)
    
    def test_invalid_inputs(self):
        # Test with empty or None parameters
        valid_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7026"
        valid_href = f"/rest/local-test-environment/datasets/{valid_uuid}"
        valid_inCollection = "Test Collection"
        
        result1 = self.mapping.add_entry("", valid_uuid, valid_href, valid_inCollection)
        result2 = self.mapping.add_entry("ods-test", "", valid_href, valid_inCollection) 
        result3 = self.mapping.add_entry("ods-test", valid_uuid, "", valid_inCollection)
        
        # All should fail
        self.assertFalse(result1)
        self.assertFalse(result2)
        self.assertFalse(result3)
        
        # Nothing should be added
        self.assertIsNone(self.mapping.get_entry("ods-test"))
        
        # Empty inCollection is allowed since it's optional
        result4 = self.mapping.add_entry("ods-valid", valid_uuid, valid_href)
        self.assertTrue(result4)
        self.assertEqual(self.mapping.get_entry("ods-valid"), (valid_uuid, valid_href, None))
    
    def test_invalid_uuid_format(self):
        # Test with invalid UUID format
        invalid_uuid = "not-a-uuid"
        valid_href = "/rest/local-test-environment/datasets/caeb7cb4-3279-46c5-b7cc-19e0c58d7027"
        valid_inCollection = "Test Collection"
        
        result = self.mapping.add_entry("ods-invalid-uuid", invalid_uuid, valid_href, valid_inCollection)
        self.assertFalse(result)
        self.assertIsNone(self.mapping.get_entry("ods-invalid-uuid"))
    
    # TODO: Maybe we want to test this sometime in the future, but for now we only test the UUID format
    #def test_invalid_href_format(self):
    #    # Test with invalid HREF format
    #    valid_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7027"
    #    invalid_href = "http://example.com/wrong-format"
    #    
    #    result = self.mapping.add_entry("ods-invalid-href", valid_uuid, invalid_href)
    #    self.assertFalse(result)
    #    self.assertIsNone(self.mapping.get_entry("ods-invalid-href"))
    
    def test_get_all_entries(self):
        # Add multiple entries
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7028"
        uuid2 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7029"
        uuid3 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7030"
        href1 = f"/rest/local-test-environment/datasets/{uuid1}"
        href2 = f"/rest/local-test-environment/datasets/{uuid2}"
        href3 = f"/rest/local-test-environment/datasets/{uuid3}"
        inColl1 = "Collection 1"
        inColl2 = "Collection 2"
        
        self.mapping.add_entry("ods-1", uuid1, href1, inColl1)
        self.mapping.add_entry("ods-2", uuid2, href2, inColl2)
        self.mapping.add_entry("ods-3", uuid3, href3)  # No inCollection
        
        # Get all entries
        all_entries = self.mapping.get_all_entries()
        
        # Check entries
        self.assertEqual(len(all_entries), 3)
        self.assertEqual(all_entries["ods-1"], (uuid1, href1, inColl1))
        self.assertEqual(all_entries["ods-2"], (uuid2, href2, inColl2))
        self.assertEqual(all_entries["ods-3"], (uuid3, href3, None))
    
    def test_get_all_ods_ids(self):
        # Add multiple entries
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7031"
        uuid2 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7032"
        href1 = f"/rest/local-test-environment/datasets/{uuid1}"
        href2 = f"/rest/local-test-environment/datasets/{uuid2}"
        inColl1 = "Collection 1"
        
        self.mapping.add_entry("ods-a", uuid1, href1, inColl1)
        self.mapping.add_entry("ods-b", uuid2, href2)
        
        # Get all ODS IDs
        all_ids = self.mapping.get_all_ods_ids()
        
        # Check IDs
        self.assertEqual(len(all_ids), 2)
        self.assertIn("ods-a", all_ids)
        self.assertIn("ods-b", all_ids)
        
    def test_get_inCollection(self):
        # Test specifically for the get_inCollection method
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7033"
        href1 = f"/rest/local-test-environment/datasets/{uuid1}"
        inColl1 = "Test/Hierarchical/Collection Path"
        
        # Entry with inCollection
        self.mapping.add_entry("ods-with-collection", uuid1, href1, inColl1)
        self.assertEqual(self.mapping.get_inCollection("ods-with-collection"), inColl1)
        
        # Entry without inCollection
        self.mapping.add_entry("ods-without-collection", uuid1, href1)
        self.assertIsNone(self.mapping.get_inCollection("ods-without-collection"))
        
        # Non-existent entry
        self.assertIsNone(self.mapping.get_inCollection("non-existent"))


if __name__ == "__main__":
    unittest.main() 