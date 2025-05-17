import os
import unittest
import uuid
from src.mapping_handlers.dataset_handler import DatasetMapping


class TestODSDataspotMapping(unittest.TestCase):
    """Test case for the ODSDataspotMapping class."""
    
    def setUp(self):
        # Use a temporary test file
        self.database_name = "local-test-environment"
        self.scheme = "test-scheme"
        # Create mapping first to get the correct path
        self.mapping = DatasetMapping(database_name=self.database_name, scheme=self.scheme)
        self.test_file = self.mapping.csv_file_path 
        self.test_type = "Dataset"
        
    def tearDown(self):
        # Clean up test file using the path from the mapping instance
        if os.path.exists(self.mapping.csv_file_path):
            os.remove(self.mapping.csv_file_path)
            
    def test_add_and_retrieve_entry(self):
        # Test adding and retrieving an entry
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7016"
        test_inCollection = "Test Collection"
        self.mapping.add_entry("ods-123", self.test_type, test_uuid, test_inCollection)
        
        # Retrieve the entry
        entry = self.mapping.get_entry("ods-123")
        self.assertEqual(entry, (self.test_type, test_uuid, test_inCollection))
        
        # Test convenience methods
        self.assertEqual(self.mapping.get_type("ods-123"), self.test_type)
        self.assertEqual(self.mapping.get_uuid("ods-123"), test_uuid)
        self.assertEqual(self.mapping.get_inCollection("ods-123"), test_inCollection)
        
        # Test without inCollection parameter
        test_uuid2 = "daeb7cb4-3279-46c5-b7cc-19e0c58d7016"
        self.mapping.add_entry("ods-124", self.test_type, test_uuid2)
        
        # Retrieve the entry
        entry2 = self.mapping.get_entry("ods-124")
        self.assertEqual(entry2, (self.test_type, test_uuid2, None))
        self.assertIsNone(self.mapping.get_inCollection("ods-124"))
        
    def test_remove_entry(self):
        # Add an entry
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7017"
        test_inCollection = "Test Collection"
        self.mapping.add_entry("ods-456", self.test_type, test_uuid, test_inCollection)
        
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
        inColl1 = "Collection 1"
        inColl2 = "Collection 2/Subcollection"
        
        self.mapping.add_entry("ods-789", self.test_type, uuid1, inColl1)
        self.mapping.add_entry("ods-abc", self.test_type, uuid2, inColl2)
        
        # Create a new instance that should load from the same file
        new_mapping = DatasetMapping(database_name=self.database_name, scheme=self.scheme)
        
        # Check that entries were loaded
        self.assertEqual(new_mapping.get_type("ods-789"), self.test_type)
        self.assertEqual(new_mapping.get_uuid("ods-789"), uuid1)
        self.assertEqual(new_mapping.get_inCollection("ods-789"), inColl1)
        self.assertEqual(new_mapping.get_inCollection("ods-abc"), inColl2)

    def test_update_existing_entry(self):
        # Add an initial entry
        old_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7020"
        new_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7021"
        old_inColl = "Old Collection"
        new_inColl = "New Collection"
        
        self.mapping.add_entry("ods-update", self.test_type, old_uuid, old_inColl)
        
        # Update with new values
        self.mapping.add_entry("ods-update", self.test_type, new_uuid, new_inColl)
        
        # Check that the update worked
        entry = self.mapping.get_entry("ods-update")
        self.assertEqual(entry, (self.test_type, new_uuid, new_inColl))
        
        # Test updating without specifying inCollection (sets it to None)
        newest_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7022"
        self.mapping.add_entry("ods-update", self.test_type, newest_uuid)
        
        # inCollection should be None
        entry = self.mapping.get_entry("ods-update")
        self.assertEqual(entry, (self.test_type, newest_uuid, None))
    
    def test_special_characters_in_ods_id(self):
        # Test with special characters in ODS IDs
        special_ods = "ods/special#char"
        test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7022"
        test_inCollection = "Collection with / and spaces"
        
        self.mapping.add_entry(special_ods, self.test_type, test_uuid, test_inCollection)
        
        # Verify retrieval works
        entry = self.mapping.get_entry(special_ods)
        self.assertEqual(entry, (self.test_type, test_uuid, test_inCollection))
    
    def test_uuid_format(self):
        # Test with proper UUID format
        valid_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7023"
        valid_inCollection = "Test Collection"
        
        self.mapping.add_entry("ods-valid", self.test_type, valid_uuid, valid_inCollection)
        entry = self.mapping.get_entry("ods-valid")
        self.assertEqual(entry, (self.test_type, valid_uuid, valid_inCollection))
        
        # Test invalid UUID format
        invalid_uuid = "not-a-uuid"
        valid_inCollection = "Test Collection"
        
        result = self.mapping.add_entry("ods-invalid-uuid", self.test_type, invalid_uuid, valid_inCollection)
        self.assertFalse(result)
        self.assertIsNone(self.mapping.get_entry("ods-invalid-uuid"))
        
        # Test that a random generated UUID works too
        random_uuid = str(uuid.uuid4())
        random_inCollection = "Random Collection"
        self.mapping.add_entry("ods-random", self.test_type, random_uuid, random_inCollection)
        self.assertEqual(self.mapping.get_uuid("ods-random"), random_uuid)
        self.assertEqual(self.mapping.get_inCollection("ods-random"), random_inCollection)
    
    def test_nonexistent_entry(self):
        # Test behavior when requesting non-existent entries
        self.assertIsNone(self.mapping.get_entry("does-not-exist"))
        self.assertIsNone(self.mapping.get_uuid("does-not-exist"))
        self.assertIsNone(self.mapping.get_inCollection("does-not-exist"))
    
    def test_empty_file(self):
        # Test with an empty file (just headers)
        empty_file_db_name = "empty_mapping_db"
        empty_mapping = None
        empty_file_path = None
        try:
            # Create a new empty mapping using database_name
            empty_mapping = DatasetMapping(database_name=empty_file_db_name, scheme=self.scheme)
            empty_file_path = empty_mapping.csv_file_path
            
            # Should not have any entries
            self.assertIsNone(empty_mapping.get_entry("any-id"))
            
            # Should be able to add entries
            test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7024"
            test_inCollection = "Empty File Collection"
            empty_mapping.add_entry("ods-empty", self.test_type, test_uuid, test_inCollection)
            self.assertEqual(empty_mapping.get_uuid("ods-empty"), test_uuid)
            self.assertEqual(empty_mapping.get_inCollection("ods-empty"), test_inCollection)
        finally:
            # Clean up
            if empty_file_path and os.path.exists(empty_file_path):
                os.remove(empty_file_path)
    
    def test_file_does_not_exist(self):
        # Test with a non-existent file path by using a unique database name
        non_existent_db_name = f"non_existent_db_{uuid.uuid4()}"
        non_existent_mapping = None
        non_existent_file_path = None
        try:
            # Should create the file and add headers when initialized with database_name
            non_existent_mapping = DatasetMapping(database_name=non_existent_db_name, scheme=self.scheme)
            non_existent_file_path = non_existent_mapping.csv_file_path
            
            # Check that the file was created
            self.assertTrue(os.path.exists(non_existent_file_path))
            
            # Should be empty
            self.assertIsNone(non_existent_mapping.get_entry("any-id"))
        finally:
            # Clean up
            if non_existent_file_path and os.path.exists(non_existent_file_path):
                os.remove(non_existent_file_path)
    
    def test_database_name_parameter(self):
        # Test the database_name parameter
        database_name = "test_db"
        db_mapping = None
        expected_filename = None
        try:
            # Create mapping with database_name
            db_mapping = DatasetMapping(database_name=database_name, scheme=self.scheme)
            expected_filename = db_mapping.csv_file_path
            
            # Check that the correct file was created
            self.assertTrue(os.path.exists(expected_filename))
            self.assertEqual(db_mapping.csv_file_path, expected_filename)
            
            # Should be able to add and retrieve entries
            test_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7025"
            test_inCollection = "Database Collection"
            db_mapping.add_entry("ods-db", self.test_type, test_uuid, test_inCollection)
            self.assertEqual(db_mapping.get_uuid("ods-db"), test_uuid)
            self.assertEqual(db_mapping.get_inCollection("ods-db"), test_inCollection)
            self.assertEqual(db_mapping.get_type("ods-db"), self.test_type)
        finally:
            # Clean up
            if expected_filename and os.path.exists(expected_filename):
                os.remove(expected_filename)
    
    def test_invalid_inputs(self):
        # Test with empty or None parameters
        valid_uuid = "caeb7cb4-3279-46c5-b7cc-19e0c58d7026"
        valid_inCollection = "Test Collection"
        
        result1 = self.mapping.add_entry("", self.test_type, valid_uuid, valid_inCollection)
        result2 = self.mapping.add_entry("ods-test", self.test_type, "", valid_inCollection)
        result_missing_type = self.mapping.add_entry("ods-test", "", valid_uuid, valid_inCollection)
        
        # All should fail
        self.assertFalse(result1)
        self.assertFalse(result2)
        self.assertFalse(result_missing_type)
        
        # Nothing should be added
        self.assertIsNone(self.mapping.get_entry("ods-test"))
        
        # Empty inCollection is allowed since it's optional
        result4 = self.mapping.add_entry("ods-valid", self.test_type, valid_uuid)
        self.assertTrue(result4)
        self.assertEqual(self.mapping.get_entry("ods-valid"), (self.test_type, valid_uuid, None))
    
    def test_invalid_uuid_format(self):
        # Test with invalid UUID format
        invalid_uuid = "not-a-uuid"
        valid_inCollection = "Test Collection"
        
        result = self.mapping.add_entry("ods-invalid-uuid", self.test_type, invalid_uuid, valid_inCollection)
        self.assertFalse(result)
        self.assertIsNone(self.mapping.get_entry("ods-invalid-uuid"))

    def test_get_all_entries(self):
        # Add multiple entries
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7028"
        uuid2 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7029"
        uuid3 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7030"
        inColl1 = "Collection 1"
        inColl2 = "Collection 2"
        
        self.mapping.add_entry("ods-1", self.test_type, uuid1, inColl1)
        self.mapping.add_entry("ods-2", self.test_type, uuid2, inColl2)
        self.mapping.add_entry("ods-3", self.test_type, uuid3)  # No inCollection
        
        # Get all entries
        all_entries = self.mapping.get_all_entries()
        
        # Check entries
        self.assertEqual(len(all_entries), 3)
        self.assertEqual(all_entries["ods-1"], (self.test_type, uuid1, inColl1))
        self.assertEqual(all_entries["ods-2"], (self.test_type, uuid2, inColl2))
        self.assertEqual(all_entries["ods-3"], (self.test_type, uuid3, None))
    
    def test_get_all_ids(self):
        # Add multiple entries
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7031"
        uuid2 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7032"
        inColl1 = "Collection 1"
        
        self.mapping.add_entry("ods-a", self.test_type, uuid1, inColl1)
        self.mapping.add_entry("ods-b", self.test_type, uuid2)
        
        # Get all IDs directly using the base method
        all_ids = self.mapping.get_all_ids()
        
        # Check IDs
        self.assertEqual(len(all_ids), 2)
        self.assertIn("ods-a", all_ids)
        self.assertIn("ods-b", all_ids)
        
    def test_get_inCollection(self):
        # Test specifically for the get_inCollection method
        uuid1 = "caeb7cb4-3279-46c5-b7cc-19e0c58d7033"
        inColl1 = "Test/Hierarchical/Collection Path"
        
        # Entry with inCollection
        self.mapping.add_entry("ods-with-collection", self.test_type, uuid1, inColl1)
        self.assertEqual(self.mapping.get_inCollection("ods-with-collection"), inColl1)
        
        # Entry without inCollection
        self.mapping.add_entry("ods-without-collection", self.test_type, uuid1)
        self.assertIsNone(self.mapping.get_inCollection("ods-without-collection"))
        
        # Non-existent entry
        self.assertIsNone(self.mapping.get_inCollection("non-existent"))


if __name__ == "__main__":
    unittest.main() 