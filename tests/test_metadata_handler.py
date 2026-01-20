import unittest
import shutil
import tempfile
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from takeout_import.metadata_handler import MetadataHandler

class TestMetadataHandler(unittest.TestCase):
    def setUp(self):
        # Mock shutil.which to avoid SystemExit
        with patch('shutil.which', return_value='/usr/bin/exiftool'):
            self.handler = MetadataHandler()
    
    def test_parse_json_sidecar(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            json.dump({
                "photoTakenTime": {"timestamp": "1672531200"},
                "geoData": {
                    "latitude": 37.7749,
                    "longitude": -122.4194,
                    "altitude": 10.0
                },
                "description": "Test Photo"
            }, tmp)
            tmp_path = Path(tmp.name)
        
        try:
            metadata = self.handler.parse_json_sidecar(tmp_path)
            self.assertEqual(metadata['timestamp'], 1672531200)
            self.assertEqual(metadata['gps']['latitude'], 37.7749)
        finally:
            os.remove(tmp_path)

    def test_read_metadata(self):
        # Mock ExifToolHelper
        with patch('exiftool.ExifToolHelper') as MockHelper:
            mock_et = MockHelper.return_value
            mock_et.__enter__.return_value = mock_et
            
            # Case 1: EXIF DateTimeOriginal present
            mock_et.get_tags.return_value = [{
                'SourceFile': str(Path("dummy.jpg").resolve()),
                'EXIF:DateTimeOriginal': '2023:01:01 12:00:00'
            }]
            
            metadata = self.handler.read_metadata(Path("dummy.jpg"))
            # 2023-01-01 12:00:00
            expected_ts = datetime(2023, 1, 1, 12, 0, 0).timestamp()
            self.assertEqual(metadata['timestamp'], expected_ts)
            
            # Case 2: No tags
            mock_et.get_tags.return_value = []
            metadata = self.handler.read_metadata(Path("dummy.jpg"))
            self.assertEqual(metadata, {})
            
            # Case 3: Invalid date string
            mock_et.get_tags.return_value = [{
                'SourceFile': str(Path("dummy.jpg").resolve()),
                'EXIF:DateTimeOriginal': 'invalid'
            }]
            metadata = self.handler.read_metadata(Path("dummy.jpg"))
            self.assertEqual(metadata, {})

    def test_write_metadata(self):
        with patch('exiftool.ExifToolHelper') as MockHelper:
            mock_et = MockHelper.return_value
            mock_et.__enter__.return_value = mock_et
            
            ts = 1672531200 # 2023-01-01 00:00:00 UTC
            expected_local_str = datetime.fromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S")
            expected_utc_str = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y:%m:%d %H:%M:%S")

            # Case 1: Image File
            json_metadata = {
                'timestamp': ts,
                'gps': {'latitude': 10.0, 'longitude': 20.0, 'altitude': 5.0},
                'people': ['Alice', 'Bob'],
                'url': 'http://example.com'
            }
            
            self.handler.write_metadata(Path("test.jpg"), json_metadata)
            
            # Verify Image Tags (Local Time)
            expected_image_tags = {
                'DateTimeOriginal': expected_local_str,
                'CreateDate': expected_local_str,
                'ModifyDate': expected_local_str,
                'GPSLatitude': 10.0,
                'GPSLatitudeRef': 10.0,
                'GPSLongitude': 20.0,
                'GPSLongitudeRef': 20.0,
                'GPSAltitude': 5.0,
                'XMP:Subject': ['Alice', 'Bob'],
                'XMP:PersonInImage': ['Alice', 'Bob'],
                'IPTC:Keywords': ['Alice', 'Bob'],
                'ExifIFD:UserComment': 'http://example.com'
            }
            
            mock_et.set_tags.assert_called_with(
                ['test.jpg'],
                tags=expected_image_tags,
                params=["-overwrite_original"]
            )
            
            # Case 2: Video File
            self.handler.write_metadata(Path("test.mp4"), json_metadata)
            
            # Verify Video Tags (UTC Time)
            expected_video_tags = {
                'QuickTime:CreateDate': expected_utc_str,
                'QuickTime:ModifyDate': expected_utc_str,
                'QuickTime:TrackCreateDate': expected_utc_str,
                'QuickTime:MediaCreateDate': expected_utc_str,
                'XMP:DateCreated': expected_utc_str,
                'QuickTime:GPSCoordinates': '10.0, 20.0, 5.0',
                'XMP:Subject': ['Alice', 'Bob'],
                'XMP:PersonInImage': ['Alice', 'Bob'],
                'XMP:UserComment': 'http://example.com'
            }
            
            # Check that set_tags was called with expected video tags
            # Note: assert_called_with checks the most recent call
            mock_et.set_tags.assert_called_with(
                ['test.mp4'],
                tags=expected_video_tags,
                params=["-overwrite_original"]
            )

if __name__ == '__main__':
    unittest.main()
