import unittest
import shutil
import tempfile
import json
import os
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

from takeout_import.media_processor import MediaProcessor
from takeout_import.file_organizer import FileOrganizer
from takeout_import.media_metadata import MediaMetadata

class TestMediaProcessor(unittest.TestCase):
    def setUp(self):
        self.source_dir = Path(tempfile.mkdtemp())
        self.dest_dir = Path(tempfile.mkdtemp())
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
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_legacy(self):
        # Setup: image.jpg and image.json
        img = self.source_dir / "image.jpg"
        img.touch()
        json_file = self.source_dir / "image.json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_edited(self):
        # Setup: image-edited.jpg and image.json
        img = self.source_dir / "image-edited.jpg"
        img.touch()
        json_file = self.source_dir / "image.json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_duplicate(self):
        # Setup: image(1).jpg and image.jpg(1).json
        img = self.source_dir / "image(1).jpg"
        img.touch()
        json_file = self.source_dir / "image.jpg(1).json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_supplemental(self):
        # Setup: image.jpg and image.jpg.supplemental-metadata.json
        img = self.source_dir / "image.jpg"
        img.touch()
        json_file = self.source_dir / "image.jpg.supplemental-metadata.json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
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
            
        mock_media_type = MagicMock()
        self.processor._process_single_file(mp_file, mock_media_type, MediaMetadata())
        
        # Expected: dest/2023/06/motion.mp4
        expected_dest = self.dest_dir / "2023" / "06" / "motion.mp4"
        self.assertTrue(expected_dest.exists())
        
        # Verify original .mp is NOT there (it wasn't copied as .mp)
        unexpected_dest = self.dest_dir / "2023" / "06" / "motion.mp"
        self.assertFalse(unexpected_dest.exists())

    def test_is_valid_timestamp(self):
        # 2023 = Valid
        ts_2023 = datetime(2023, 1, 1).timestamp()
        self.assertTrue(self.processor._is_valid_timestamp(ts_2023))
        
        # 1999 = Valid
        ts_1999 = datetime(1999, 1, 1).timestamp()
        self.assertTrue(self.processor._is_valid_timestamp(ts_1999))
        
        # 1998 = Invalid
        ts_1998 = datetime(1998, 12, 31).timestamp()
        self.assertFalse(self.processor._is_valid_timestamp(ts_1998))

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
            
        # Scenario 1: EXIF present and valid -> Should use EXIF (2022)
        ts_exif = datetime(2022, 1, 1).timestamp()
        media_metadata = MediaMetadata(timestamp=ts_exif)
        
        mock_media_type = MagicMock()
        self.processor._process_single_file(img, mock_media_type, media_metadata)
        
        # Check destination
        expected_dest = self.dest_dir / "2022" / "01" / "priority.jpg"
        self.assertTrue(expected_dest.exists())
        
        # Cleanup for next scenario
        shutil.rmtree(self.dest_dir)
        self.dest_dir.mkdir()
        self.processor.file_organizer = FileOrganizer(self.dest_dir) # Re-init organizer with new dir
        
        # Scenario 2: EXIF missing -> Should use JSON (2021)
        media_metadata = MediaMetadata()
        
        self.processor._process_single_file(img, mock_media_type, media_metadata)
        
        expected_dest = self.dest_dir / "2021" / "01" / "priority.jpg"
        self.assertTrue(expected_dest.exists())
        
        # Cleanup
        shutil.rmtree(self.dest_dir)
        self.dest_dir.mkdir()
        self.processor.file_organizer = FileOrganizer(self.dest_dir)

        # Scenario 3: EXIF invalid (<1999), JSON valid -> Should use JSON (2021)
        ts_invalid = datetime(1990, 1, 1).timestamp()
        media_metadata = MediaMetadata(timestamp=ts_invalid)
        
        self.processor._process_single_file(img, mock_media_type, media_metadata)
        
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
        
        media_metadata = MediaMetadata(timestamp=ts_invalid)
        
        self.processor._process_single_file(img, mock_media_type, media_metadata)
        
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
        media_metadata = MediaMetadata(timestamp=ts_exif)
        
        # Call _process_single_file and check return value
        mock_media_type = MagicMock()
        result = self.processor._process_single_file(img, mock_media_type, media_metadata)
        
        # Result should be (final_path, media_type, json_metadata)
        self.assertIsNotNone(result)
        final_path, _, json_metadata_to_write = result
        
        # Verify 'timestamp' is NOT in the metadata to write (should be None)
        self.assertIsNone(json_metadata_to_write.timestamp)
        
        # Also verify destination is based on EXIF (2022)
        expected_dest = self.dest_dir / "2022" / "01" / "overwrite.jpg"
        self.assertTrue(expected_dest.exists())

    def test_find_json_sidecar_emerging_pattern(self):
        # Setup: image.jpg and image.jpg.some.other.json
        img = self.source_dir / "emerging.jpg"
        img.touch()
        json_file = self.source_dir / "emerging.jpg.some.other.json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

    def test_find_json_sidecar_duplicate_legacy(self):
        # Setup: image(1).jpg and image(1).json
        img = self.source_dir / "image(1).jpg"
        img.touch()
        json_file = self.source_dir / "image(1).json"
        json_file.touch()
        
        found = self.processor._find_json_sidecar(img)
        self.assertEqual(found, json_file)

if __name__ == '__main__':
    unittest.main()
