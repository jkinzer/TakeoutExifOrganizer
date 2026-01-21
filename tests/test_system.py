import unittest
import tempfile
import shutil
import json
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock

from takeout_import.media_type import SUPPORTED_MEDIA, MediaType
from takeout_import.media_processor import MediaProcessor
from takeout_import.metadata_handler import MetadataHandler
from takeout_import.file_organizer import FileOrganizer
from tests.media_helper import create_dummy_media

# Configure logging to capture output during tests if needed
logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)

class TestSystemFormats(unittest.TestCase):
    """
    System tests verifying support for all media formats.
    """
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.source_dir = self.test_dir / "source"
        self.dest_dir = self.test_dir / "dest"
        self.source_dir.mkdir()
        self.dest_dir.mkdir()
        
        self.handler = MetadataHandler()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_all_formats_pipeline(self):
        """
        System test for all supported formats.
        Verifies:
        1. JSON sidecar reading
        2. File organization (copy to dest)
        3. Metadata writing (tags)
        """
        # Timestamp to use for testing: 2023-01-01 12:00:00
        test_timestamp = 1672574400.0 
        test_dt = datetime.fromtimestamp(test_timestamp)
        expected_year = str(test_dt.year)
        expected_month = f"{test_dt.month:02d}"
        
        # Metadata to put in JSON sidecar
        sidecar_metadata = {
            "title": "Test Title",
            "description": "Test Description",
            "photoTakenTime": {
                "timestamp": str(int(test_timestamp))
            },
            "geoData": {
                "latitude": 37.7749,
                "longitude": -122.4194,
                "altitude": 0.0
            },
            "people": [{"name": "Person A"}, {"name": "Person B"}]
        }

        # 1. Setup Phase: Create files for all formats
        created_files = []
        for ext, media_type in SUPPORTED_MEDIA.items():
            # Create media file
            filename = f"test_file{ext}"
            file_path = self.source_dir / filename
            create_dummy_media(file_path)
            created_files.append((file_path, media_type))
            
            # Create corresponding JSON sidecar
            json_path = self.source_dir / f"{filename}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(sidecar_metadata, f)

        # Run the processor
        processor = MediaProcessor(self.source_dir, self.dest_dir)
        processor.process()

        # 3. Verification Phase
        for source_path, media_type in created_files:
            with self.subTest(ext=source_path.suffix):
                # Check if file exists in destination
                # Expected path: dest/YEAR/MONTH/filename
                
                # Handle .mp renaming
                dest_filename = source_path.name
                if source_path.suffix.lower() == '.mp':
                    dest_filename = source_path.stem + '.mp4'
                
                expected_dest_path = self.dest_dir / expected_year / expected_month / dest_filename
                
                self.assertTrue(expected_dest_path.exists(), f"File not found in destination: {expected_dest_path}")
                
                # Verify Metadata
                # We need to read back the metadata from the destination file
                # Note: Not all formats support all tags, so we check based on capabilities
                
                # Known formats where ExifTool write might fail or is not supported
                SKIP_WRITE_VERIFICATION = {'.avi', '.mkv', '.wmv', '.bmp', '.gif'}
                
                if source_path.suffix.lower() in SKIP_WRITE_VERIFICATION:
                    continue

                if media_type.supports_write():
                    try:
                        results = self.handler.read_metadata_batch([(expected_dest_path, media_type)])
                        if expected_dest_path in results:
                            metadata = results[expected_dest_path]
                            
                            # Verify Timestamp (should match JSON)
                            # Allow for small precision differences if any
                            if 'timestamp' in metadata:
                                self.assertAlmostEqual(metadata['timestamp'], test_timestamp, delta=1.0, msg=f"Timestamp mismatch for {source_path.suffix}")
                            else:
                                self.fail(f"Timestamp missing for {source_path.suffix}")

                            # Verify GPS (if supported)
                            if 'gps' in metadata:
                                gps = metadata['gps']
                                self.assertAlmostEqual(gps['latitude'], 37.7749, places=3, msg=f"Latitude mismatch for {source_path.suffix}")
                                self.assertAlmostEqual(gps['longitude'], -122.4194, places=3, msg=f"Longitude mismatch for {source_path.suffix}")

                            # Verify People (if supported)
                            if media_type.supports_xmp or media_type.supports_iptc:
                                if 'people' in metadata:
                                    expected_people = ["Person A", "Person B"]
                                    self.assertEqual(sorted(metadata['people']), sorted(expected_people), f"People mismatch for {source_path.suffix}")
                                else:
                                    self.fail(f"People metadata missing for {source_path.suffix}")

                    except Exception as e:
                        self.fail(f"Failed to verify metadata for {source_path.suffix}: {e}")

class TestSystemLogic(unittest.TestCase):
    """
    System tests verifying specific logic requirements (Duplicate handling, Merging, Priority).
    """
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.source_dir = self.test_dir / "source"
        self.dest_dir = self.test_dir / "dest"
        self.source_dir.mkdir()
        self.dest_dir.mkdir()
        
        # Setup Processor
        self.processor = MediaProcessor(self.source_dir, self.dest_dir)
        # Mock MetadataHandler to avoid external dependencies in unit logic test
        self.processor.metadata_handler = MagicMock()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_duplicate_skip(self):
        # Create a file
        filename = "test.jpg"
        src_file = self.source_dir / filename
        src_file.write_text("content")
        
        # Set mtime
        timestamp = 1600000000.0
        os.utime(src_file, (timestamp, timestamp))
        
        # Create identical file in dest (simulate previous run)
        year = "2020"
        month = "09"
        dest_file = self.dest_dir / year / month / filename
        dest_file.parent.mkdir(parents=True)
        dest_file.write_text("content")
        os.utime(dest_file, (timestamp, timestamp))
        
        # Mock metadata
        self.processor.metadata_handler.read_metadata_batch.return_value = {
            src_file: {'timestamp': timestamp}
        }
        self.processor.metadata_handler.parse_json_sidecar.return_value = {}
        
        # Mock extract_metadata and is_metadata_identical
        self.processor.metadata_handler.extract_metadata.return_value = {'timestamp': timestamp}
        # The processor now constructs a comparison_metadata dict. 
        # We need to ensure is_metadata_identical returns True when called with it.
        self.processor.metadata_handler.is_metadata_identical.return_value = True

        # Run process single file
        # We expect it to return None (skipped)
        result = self.processor._process_single_file(
            src_file, 
            MediaType({'.jpg'}, True, True, True), 
            {'timestamp': timestamp}
        )
        
        self.assertIsNone(result)
        
        # Verify no renamed file exists
        self.assertFalse((dest_file.parent / "test_1.jpg").exists())

    def test_people_merge(self):
        filename = "people.jpg"
        src_file = self.source_dir / filename
        src_file.touch()
        
        # Create dummy JSON sidecar so _find_json_sidecar works
        json_file = self.source_dir / (filename + ".json")
        json_file.touch()
        
        # Mock metadata
        media_metadata = {
            'timestamp': 1600000000.0,
            'people': ['Alice', 'Bob']
        }
        json_metadata = {
            'people': ['Bob', 'Charlie']
        }
        
        self.processor.metadata_handler.parse_json_sidecar.return_value = json_metadata
        
        # Run
        result = self.processor._process_single_file(
            src_file,
            MediaType({'.jpg'}, True, True, True),
            media_metadata
        )
        
        # Verify result
        self.assertIsNotNone(result)
        path, mt, metadata_to_write = result
        
        # Expect merged: Alice, Bob, Charlie
        self.assertEqual(metadata_to_write['people'], ['Alice', 'Bob', 'Charlie'])

    def test_url_priority(self):
        filename = "url.jpg"
        src_file = self.source_dir / filename
        src_file.touch()
        
        # Create dummy JSON sidecar
        json_file = self.source_dir / (filename + ".json")
        json_file.touch()
        
        # Mock metadata
        media_metadata = {
            'timestamp': 1600000000.0,
            'url': 'http://original.com'
        }
        json_metadata = {
            'url': 'http://json.com'
        }
        
        self.processor.metadata_handler.parse_json_sidecar.return_value = json_metadata
        
        # Run
        result = self.processor._process_single_file(
            src_file,
            MediaType({'.jpg'}, True, True, True),
            media_metadata
        )
        
        # Verify result
        self.assertIsNotNone(result)
        path, mt, metadata_to_write = result
        
        # Expect 'url' to be absent from write metadata (preserved)
        self.assertNotIn('url', metadata_to_write)

if __name__ == '__main__':
    unittest.main()
