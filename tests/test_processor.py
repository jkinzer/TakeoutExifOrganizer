import unittest
import shutil
import tempfile
import json
import os
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

# Mock exiftool module before importing organize_photos
# import sys
# from unittest.mock import MagicMock
# sys.modules['exiftool'] = MagicMock()

from takeout_import.media_processor import MediaProcessor
from takeout_import.metadata_handler import MetadataHandler
from takeout_import.file_organizer import FileOrganizer

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
                'EXIF:DateTimeOriginal': 'invalid'
            }]
            metadata = self.handler.read_metadata(Path("dummy.jpg"))
            self.assertEqual(metadata, {})

class TestFileOrganizer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.dest_root = Path(self.temp_dir)
        self.organizer = FileOrganizer(self.dest_root)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_get_target_path(self):
        # Use a timestamp that is safe across timezones (e.g. noon UTC)
        # 2023-06-15 12:00:00 UTC = 1686830400
        ts = 1686830400 
        path = self.organizer.get_target_path(ts, "test.jpg")
        expected = self.dest_root / "2023" / "06" / "test.jpg"
        self.assertEqual(path, expected)

    def test_resolve_collision(self):
        # Create a file that conflicts
        ts = 1672531200
        target = self.organizer.get_target_path(ts, "test.jpg")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
        
        resolved = self.organizer.resolve_collision(target)
        self.assertEqual(resolved.name, "test_1.jpg")
        
        # Create the _1 file and test again
        resolved.touch()
        resolved_2 = self.organizer.resolve_collision(target)
        self.assertEqual(resolved_2.name, "test_2.jpg")

class TestMediaProcessor(unittest.TestCase):
    def setUp(self):
        self.source_dir = Path(tempfile.mkdtemp())
        self.dest_dir = Path(tempfile.mkdtemp())
        # Mock shutil.which to return a path so MetadataHandler doesn't exit
        with patch('shutil.which', return_value='/usr/bin/exiftool'):
            self.processor = MediaProcessor(self.source_dir, self.dest_dir)

    def tearDown(self):
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.dest_dir)

    def test_find_json_sidecar_simple(self):
        # Setup: image.jpg and image.jpg.json
        img = self.source_dir / "image.jpg"
        img.touch()
        json_file = self.source_dir / "image.jpg.json"
        json_file.touch()
        
        found = self.processor.find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_legacy(self):
        # Setup: image.jpg and image.json
        img = self.source_dir / "image.jpg"
        img.touch()
        json_file = self.source_dir / "image.json"
        json_file.touch()
        
        found = self.processor.find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_edited(self):
        # Setup: image-edited.jpg and image.json
        img = self.source_dir / "image-edited.jpg"
        img.touch()
        json_file = self.source_dir / "image.json"
        json_file.touch()
        
        found = self.processor.find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_duplicate(self):
        # Setup: image(1).jpg and image.jpg(1).json
        img = self.source_dir / "image(1).jpg"
        img.touch()
        json_file = self.source_dir / "image.jpg(1).json"
        json_file.touch()
        
        found = self.processor.find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_supplemental(self):
        # Setup: image.jpg and image.jpg.supplemental-metadata.json
        img = self.source_dir / "image.jpg"
        img.touch()
        json_file = self.source_dir / "image.jpg.supplemental-metadata.json"
        json_file.touch()
        
        found = self.processor.find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_motion_photo_mp_renaming(self):
        # Setup: motion.mp and motion.json
        mp_file = self.source_dir / "motion.mp"
        mp_file.touch()
        json_file = self.source_dir / "motion.json"
        
        # Create dummy JSON with timestamp
        # Use a safe mid-year timestamp to avoid timezone issues (e.g. 2023-06-15)
        ts = 1686830400
        with open(json_file, 'w') as f:
            json.dump({"photoTakenTime": {"timestamp": str(ts)}}, f)
            
        self.processor.process_file(mp_file)
        
        # Expected: dest/2023/06/motion.mp4
        expected_dest = self.dest_dir / "2023" / "06" / "motion.mp4"
        self.assertTrue(expected_dest.exists())
        
        # Verify original .mp is NOT there (it wasn't copied as .mp)
        unexpected_dest = self.dest_dir / "2023" / "06" / "motion.mp"
        self.assertFalse(unexpected_dest.exists())

    def test_is_valid_timestamp(self):
        # 2023 = Valid
        ts_2023 = datetime(2023, 1, 1).timestamp()
        self.assertTrue(self.processor.is_valid_timestamp(ts_2023))
        
        # 1999 = Valid
        ts_1999 = datetime(1999, 1, 1).timestamp()
        self.assertTrue(self.processor.is_valid_timestamp(ts_1999))
        
        # 1998 = Invalid
        ts_1998 = datetime(1998, 12, 31).timestamp()
        self.assertFalse(self.processor.is_valid_timestamp(ts_1998))

    def test_process_file_priority(self):
        # Setup file
        img = self.source_dir / "priority.jpg"
        img.touch()
        # Set mtime to 2020
        ts_mtime = datetime(2020, 1, 1).timestamp()
        os.utime(img, (ts_mtime, ts_mtime))
        
        # Setup JSON with 2021
        json_file = self.source_dir / "priority.jpg.json"
        ts_json = datetime(2021, 1, 1).timestamp()
        with open(json_file, 'w') as f:
            json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
            
        # Mock read_metadata to return 2022
        ts_exif = datetime(2022, 1, 1).timestamp()
        
        with patch.object(self.processor.metadata_handler, 'read_metadata') as mock_read:
            # Scenario 1: EXIF present and valid -> Should use EXIF (2022)
            mock_read.return_value = {'timestamp': ts_exif}
            
            self.processor.process_file(img)
            
            # Check destination
            expected_dest = self.dest_dir / "2022" / "01" / "priority.jpg"
            self.assertTrue(expected_dest.exists())
            
            # Cleanup for next scenario
            shutil.rmtree(self.dest_dir)
            self.dest_dir.mkdir()
            self.processor.file_organizer = FileOrganizer(self.dest_dir) # Re-init organizer with new dir
            
            # Scenario 2: EXIF missing -> Should use JSON (2021)
            mock_read.return_value = {}
            
            self.processor.process_file(img)
            
            expected_dest = self.dest_dir / "2021" / "01" / "priority.jpg"
            self.assertTrue(expected_dest.exists())
            
             # Cleanup
            shutil.rmtree(self.dest_dir)
            self.dest_dir.mkdir()
            self.processor.file_organizer = FileOrganizer(self.dest_dir)

            # Scenario 3: EXIF invalid (<1999), JSON valid -> Should use JSON (2021)
            ts_invalid = datetime(1990, 1, 1).timestamp()
            mock_read.return_value = {'timestamp': ts_invalid}
            
            self.processor.process_file(img)
            
            expected_dest = self.dest_dir / "2021" / "01" / "priority.jpg"
            self.assertTrue(expected_dest.exists())
            
            # Cleanup
            shutil.rmtree(self.dest_dir)
            self.dest_dir.mkdir()
            self.processor.file_organizer = FileOrganizer(self.dest_dir)

            # Scenario 4: EXIF invalid, JSON invalid -> Should use Mtime (2020)
            # Update JSON to be invalid
            with open(json_file, 'w') as f:
                json.dump({"photoTakenTime": {"timestamp": str(int(ts_invalid))}}, f)
            
            mock_read.return_value = {'timestamp': ts_invalid}
            
            self.processor.process_file(img)
            
            expected_dest = self.dest_dir / "2020" / "01" / "priority.jpg"
            self.assertTrue(expected_dest.exists())

    def test_process_file_no_overwrite(self):
        # Verify that if EXIF is valid, we do NOT overwrite it with JSON
        img = self.source_dir / "overwrite.jpg"
        img.touch()
        
        json_file = self.source_dir / "overwrite.jpg.json"
        # JSON has 2021
        ts_json = datetime(2021, 1, 1).timestamp()
        with open(json_file, 'w') as f:
            json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
            
        # EXIF has 2022 (Valid)
        ts_exif = datetime(2022, 1, 1).timestamp()
        
        with patch.object(self.processor.metadata_handler, 'read_metadata') as mock_read:
            mock_read.return_value = {'timestamp': ts_exif}
            
            # Mock write_metadata to verify what gets passed
            with patch.object(self.processor.metadata_handler, 'write_metadata') as mock_write:
                self.processor.process_file(img)
                
                # Verify write_metadata was called (maybe for other fields), 
                # but 'timestamp' should NOT be in the metadata dict passed to it.
                # Note: process_file passes the 'metadata' dict from JSON.
                # We expect 'timestamp' key to be removed from it.
                
                args, _ = mock_write.call_args
                passed_metadata = args[1]
                self.assertNotIn('timestamp', passed_metadata)
                
                # Also verify destination is based on EXIF (2022)
                expected_dest = self.dest_dir / "2022" / "01" / "overwrite.jpg"
                self.assertTrue(expected_dest.exists())

if __name__ == '__main__':
    unittest.main()
