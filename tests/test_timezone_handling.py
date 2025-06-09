import unittest
from src.dataset_transformer import _iso_8601_to_unix_timestamp

class TestTimezoneHandling(unittest.TestCase):
    def test_utc_timezone_in_string(self):
        """Test ISO 8601 with explicit UTC timezone"""
        # 2025-03-07T00:00:00Z in UTC
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00Z"), 1741305600000)
    
    def test_utc_timezone_without_z(self):
        """Test ISO 8601 with +00:00 UTC timezone"""
        # 2025-03-07T00:00:00+00:00 in UTC
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00+00:00"), 1741305600000)
    
    def test_explicit_timezone_in_string(self):
        """Test ISO 8601 with explicit non-UTC timezone"""
        # 2025-03-07T00:00:00+01:00 in CET
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00+01:00"), 1741302000000)
    
    def test_dataset_timezone_zurich(self):
        """Test dataset timezone when not in string"""
        # 2025-03-07T00:00:00 with Europe/Zurich timezone
        # During winter time, Zurich is UTC+1
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Europe/Zurich"), 1741302000000)
    
    def test_dataset_timezone_summer(self):
        """Test dataset timezone during summer time"""
        # 2025-07-07T00:00:00 with Europe/Zurich timezone
        # During summer time, Zurich is UTC+2
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-07-07T00:00:00", "Europe/Zurich"), 1751839200000)
    
    def test_no_timezone_info(self):
        """Test when no timezone is provided (should default to UTC)"""
        # 2025-03-07T00:00:00 with no timezone info (defaults to UTC)
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00"), 1741305600000)
    
    def test_invalid_timezone(self):
        """Test with invalid timezone (should fall back to UTC)"""
        # Should fall back to UTC
        self.assertEqual(_iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Invalid/Timezone"), 1741305600000)
    
    def test_missing_datetime(self):
        """Test with empty datetime string"""
        self.assertIsNone(_iso_8601_to_unix_timestamp(None))
        self.assertIsNone(_iso_8601_to_unix_timestamp(""))
    
    def test_malformed_datetime(self):
        """Test with malformed datetime string"""
        self.assertIsNone(_iso_8601_to_unix_timestamp("not-a-date"))
        
    # New tests for time normalization
    
    def test_normalize_utc_time_to_midnight(self):
        """Test that any time in UTC gets normalized to midnight UTC"""
        # These should all normalize to the same midnight timestamp
        midnight = _iso_8601_to_unix_timestamp("2025-03-07T00:00:00Z")
        morning = _iso_8601_to_unix_timestamp("2025-03-07T08:30:45Z")
        evening = _iso_8601_to_unix_timestamp("2025-03-07T23:59:59Z")
        
        self.assertEqual(morning, midnight)
        self.assertEqual(evening, midnight)
        self.assertEqual(midnight, 1741305600000)  # 2025-03-07T00:00:00Z
    
    def test_normalize_zurich_time_to_midnight(self):
        """Test that any time in Europe/Zurich gets normalized to midnight Zurich time"""
        # These should all normalize to the same midnight timestamp
        midnight = _iso_8601_to_unix_timestamp("2025-03-07T00:00:00+01:00")  # CET
        morning = _iso_8601_to_unix_timestamp("2025-03-07T08:30:45+01:00")  # CET
        evening = _iso_8601_to_unix_timestamp("2025-03-07T23:59:59+01:00")  # CET
        
        self.assertEqual(morning, midnight)
        self.assertEqual(evening, midnight)
        self.assertEqual(midnight, 1741302000000)  # 2025-03-07T00:00:00+01:00 in UTC milliseconds
    
    def test_normalize_zurich_dataset_timezone(self):
        """Test normalization with dataset timezone"""
        # These should all normalize to the same midnight timestamp
        midnight = _iso_8601_to_unix_timestamp("2025-03-07T00:00:00", "Europe/Zurich")
        morning = _iso_8601_to_unix_timestamp("2025-03-07T08:30:45", "Europe/Zurich")
        evening = _iso_8601_to_unix_timestamp("2025-03-07T23:59:59", "Europe/Zurich")
        
        self.assertEqual(morning, midnight)
        self.assertEqual(evening, midnight)
        self.assertEqual(midnight, 1741302000000)  # 2025-03-07T00:00:00+01:00 in UTC milliseconds

if __name__ == '__main__':
    unittest.main() 