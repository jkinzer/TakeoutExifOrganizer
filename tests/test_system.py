import pytest
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
from takeout_import.media_metadata import MediaMetadata
from tests.media_helper import create_dummy_media

# Configure logging to capture output during tests if needed
logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)

@pytest.fixture
def test_dirs(tmp_path):
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "dest"
    source_dir.mkdir()
    dest_dir.mkdir()
    return source_dir, dest_dir

@pytest.fixture
def handler():
    return MetadataHandler()

@pytest.mark.parametrize("ext, media_type", SUPPORTED_MEDIA.items())
def test_all_formats_pipeline(test_dirs, handler, ext, media_type):
    """
    System test for all supported formats.
    Verifies:
    1. JSON sidecar reading
    2. File organization (copy to dest)
    3. Metadata writing (tags)
    """
    source_dir, dest_dir = test_dirs
    
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

    # 1. Setup Phase: Create file for this format
    filename = f"test_file{ext}"
    file_path = source_dir / filename
    create_dummy_media(file_path)
    
    # Create corresponding JSON sidecar
    json_path = source_dir / f"{filename}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(sidecar_metadata, f)

    # Run the processor
    processor = MediaProcessor(source_dir, dest_dir)
    processor.process()

    # 3. Verification Phase
    # Check if file exists in destination
    # Expected path: dest/YEAR/MONTH/filename
    
    # Handle .mp renaming
    dest_filename = file_path.name
    if file_path.suffix.lower() == '.mp':
        dest_filename = file_path.stem + '.mp4'
    
    expected_dest_path = dest_dir / expected_year / expected_month / dest_filename
    
    assert expected_dest_path.exists(), f"File not found in destination: {expected_dest_path}"
    
    # Verify Metadata
    # We need to read back the metadata from the destination file
    # Note: Not all formats support all tags, so we check based on capabilities
    if media_type.supports_write():
        try:
            results = handler.read_metadata_batch([(expected_dest_path, media_type)])
            if expected_dest_path in results:
                metadata = results[expected_dest_path]
                
                # Verify Timestamp (should match JSON)
                # Allow for small precision differences if any
                if metadata.timestamp is not None:
                    assert metadata.timestamp == pytest.approx(test_timestamp, abs=1.0), f"Timestamp mismatch for {file_path.suffix}"
                else:
                    pytest.fail(f"Timestamp missing for {file_path.suffix}")

                # Verify GPS (if supported)
                if metadata.gps:
                    gps = metadata.gps
                    assert gps.latitude == pytest.approx(37.7749, abs=0.001), f"Latitude mismatch for {file_path.suffix}"
                    assert gps.longitude == pytest.approx(-122.4194, abs=0.001), f"Longitude mismatch for {file_path.suffix}"

                # Verify People (if supported)
                if media_type.supports_xmp or media_type.supports_iptc:
                    if metadata.people:
                        expected_people = ["Person A", "Person B"]
                        assert sorted(metadata.people) == sorted(expected_people), f"People mismatch for {file_path.suffix}"
                    else:
                        pytest.fail(f"People metadata missing for {file_path.suffix}")

        except Exception as e:
            pytest.fail(f"Failed to verify metadata for {file_path.suffix}: {e}")

class TestSystemLogic:
    """
    System tests verifying specific logic requirements (Duplicate handling, Merging, Priority).
    """
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.test_dir = tmp_path
        self.source_dir = self.test_dir / "source"
        self.dest_dir = self.test_dir / "dest"
        self.source_dir.mkdir()
        self.dest_dir.mkdir()
        
        # Setup Processor
        self.processor = MediaProcessor(self.source_dir, self.dest_dir)
        # Mock MetadataHandler to avoid external dependencies in unit logic test
        self.processor.metadata_handler = MagicMock()

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
            src_file: MediaMetadata(timestamp=timestamp)
        }
        self.processor.metadata_handler.parse_json_sidecar.return_value = MediaMetadata()
        
        # Mock extract_metadata
        self.processor.metadata_handler.extract_metadata.return_value = MediaMetadata(timestamp=timestamp)

        # Run process single file
        # We expect it to return None (skipped)
        result = self.processor._process_single_file(
            src_file, 
            MediaType({'.jpg'}, True, True, True), 
            MediaMetadata(timestamp=timestamp)
        )
        
        assert result is None
        
        # Verify no renamed file exists
        assert not (dest_file.parent / "test_1.jpg").exists()

    def test_people_merge(self):
        filename = "people.jpg"
        src_file = self.source_dir / filename
        src_file.touch()
        
        # Create dummy JSON sidecar so _find_json_sidecar works
        json_file = self.source_dir / (filename + ".json")
        json_file.touch()
        
        # Mock metadata
        media_metadata = MediaMetadata(
            timestamp=1600000000.0,
            people=['Alice', 'Bob']
        )
        json_metadata = MediaMetadata(
            people=['Bob', 'Charlie']
        )
        
        self.processor.metadata_handler.parse_json_sidecar.return_value = json_metadata
        
        # Run
        result = self.processor._process_single_file(
            src_file,
            MediaType({'.jpg'}, True, True, True),
            media_metadata
        )
        
        # Verify result
        assert result is not None
        path, mt, metadata_to_write = result
        
        # Expect merged: Alice, Bob, Charlie
        assert sorted(metadata_to_write.people) == sorted(['Alice', 'Bob', 'Charlie'])

    def test_url_priority(self):
        filename = "url.jpg"
        src_file = self.source_dir / filename
        src_file.touch()
        
        # Create dummy JSON sidecar
        json_file = self.source_dir / (filename + ".json")
        json_file.touch()
        
        # Mock metadata
        media_metadata = MediaMetadata(
            timestamp=1600000000.0,
            url='http://original.com'
        )
        json_metadata = MediaMetadata(
            url='http://json.com'
        )
        
        self.processor.metadata_handler.parse_json_sidecar.return_value = json_metadata
        
        # Run
        result = self.processor._process_single_file(
            src_file,
            MediaType({'.jpg'}, True, True, True),
            media_metadata
        )
        
        # Verify result
        assert result is not None
        path, mt, metadata_to_write = result
        
        # Expect 'url' to be absent from write metadata (preserved)
        assert metadata_to_write.url is None


