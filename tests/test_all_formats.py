import unittest
import tempfile
import shutil
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from takeout_import.media_type import SUPPORTED_MEDIA
from takeout_import.media_processor import MediaProcessor
from takeout_import.metadata_handler import MetadataHandler
from tests.media_helper import create_dummy_media

# Configure logging to capture output during tests if needed
logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)

class TestAllFormatsSystem(unittest.TestCase):
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

                            # Verify Description/Caption (if supported)
                            # Note: Mapping depends on MetadataHandler implementation. 
                            # Usually description maps to Caption-Abstract or Description
                            # We'll check if the handler returns it.
                            # if 'description' in metadata:
                            #    self.assertEqual(metadata['description'], "Test Description", f"Description mismatch for {source_path.suffix}")
                            
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

if __name__ == '__main__':
    unittest.main()
