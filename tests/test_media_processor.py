import pytest
import shutil
import json
import os
from datetime import datetime
from unittest.mock import MagicMock

from takeout_import.media_processor import MediaProcessor
from takeout_import.file_organizer import FileOrganizer
from takeout_import.media_metadata import MediaMetadata

@pytest.fixture
def processor(tmp_path):
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "dest"
    source_dir.mkdir()
    dest_dir.mkdir()
    return MediaProcessor(source_dir, dest_dir)

@pytest.mark.parametrize("img_name, json_name", [
    ("image.jpg", "image.jpg.json"),
    ("image.jpg", "image.json"),
    ("image-edited.jpg", "image.json"),
    ("image(1).jpg", "image.jpg(1).json"),
    ("image.jpg", "image.jpg.supplemental-metadata.json"),
    ("emerging.jpg", "emerging.jpg.some.other.json"),
    ("image(1).jpg", "image(1).json"),
])
def test_find_json_sidecar(processor, img_name, json_name):
    # Setup
    img = processor.source_dir / img_name
    img.touch()
    json_file = processor.source_dir / json_name
    json_file.touch()
    
    found = processor._find_json_sidecar(img)
    assert found == json_file

def test_motion_photo_mp_renaming(processor):
    # Setup: motion.mp and motion.json
    mp_file = processor.source_dir / "motion.mp"
    mp_file.touch()
    json_file = processor.source_dir / "motion.json"
    
    # Create dummy JSON with timestamp
    # Use a safe mid-year timestamp to avoid timezone issues (e.g. 2023-06-15)
    ts = 1686830400
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(ts)}}, f)
        
    mock_media_type = MagicMock()
    processor._process_single_file(mp_file, mock_media_type, MediaMetadata())
    
    # Expected: dest/2023/06/motion.mp4
    expected_dest = processor.dest_dir / "2023" / "06" / "motion.mp4"
    assert expected_dest.exists()
    
    # Verify original .mp is NOT there (it wasn't copied as .mp)
    unexpected_dest = processor.dest_dir / "2023" / "06" / "motion.mp"
    assert not unexpected_dest.exists()

@pytest.mark.parametrize("timestamp, expected", [
    (datetime(2023, 1, 1).timestamp(), True),
    (datetime(1999, 1, 1).timestamp(), True),
    (datetime(1998, 12, 31).timestamp(), False),
])
def test_is_valid_timestamp(processor, timestamp, expected):
    assert processor._is_valid_timestamp(timestamp) == expected

def test_process_file_priority(processor):
    # Setup file
    img = processor.source_dir / "priority.jpg"
    img.touch()
    # Set mtime to 2020
    ts_mtime = datetime(2020, 1, 1).timestamp()
    os.utime(img, (ts_mtime, ts_mtime))
    
    # Setup JSON with 2021
    json_file = processor.source_dir / "priority.jpg.json"
    ts_json = datetime(2021, 1, 1).timestamp()
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
        
    mock_media_type = MagicMock()

    # Scenario 1: EXIF present and valid -> Should use EXIF (2022)
    ts_exif = datetime(2022, 1, 1).timestamp()
    media_metadata = MediaMetadata(timestamp=ts_exif)
    
    processor._process_single_file(img, mock_media_type, media_metadata)
    
    # Check destination
    expected_dest = processor.dest_dir / "2022" / "01" / "priority.jpg"
    assert expected_dest.exists()
    
    # Cleanup for next scenario
    shutil.rmtree(processor.dest_dir)
    processor.dest_dir.mkdir()
    processor.file_organizer = FileOrganizer(processor.dest_dir) # Re-init organizer with new dir
    
    # Scenario 2: EXIF missing -> Should use JSON (2021)
    media_metadata = MediaMetadata()
    
    processor._process_single_file(img, mock_media_type, media_metadata)
    
    expected_dest = processor.dest_dir / "2021" / "01" / "priority.jpg"
    assert expected_dest.exists()
    
    # Cleanup
    shutil.rmtree(processor.dest_dir)
    processor.dest_dir.mkdir()
    processor.file_organizer = FileOrganizer(processor.dest_dir)

    # Scenario 3: EXIF invalid (<1999), JSON valid -> Should use JSON (2021)
    ts_invalid = datetime(1990, 1, 1).timestamp()
    media_metadata = MediaMetadata(timestamp=ts_invalid)
    
    processor._process_single_file(img, mock_media_type, media_metadata)
    
    expected_dest = processor.dest_dir / "2021" / "01" / "priority.jpg"
    assert expected_dest.exists()
    
    # Cleanup
    shutil.rmtree(processor.dest_dir)
    processor.dest_dir.mkdir()
    processor.file_organizer = FileOrganizer(processor.dest_dir)

    # Scenario 4: EXIF invalid, JSON invalid -> Should use Mtime (2020)
    # Update JSON to be invalid
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(int(ts_invalid))}}, f)
    
    media_metadata = MediaMetadata(timestamp=ts_invalid)
    
    processor._process_single_file(img, mock_media_type, media_metadata)
    
    expected_dest = processor.dest_dir / "2020" / "01" / "priority.jpg"
    assert expected_dest.exists()

def test_process_file_no_overwrite(processor):
    # Verify that if EXIF is valid, we do NOT overwrite it with JSON
    img = processor.source_dir / "overwrite.jpg"
    img.touch()
    
    json_file = processor.source_dir / "overwrite.jpg.json"
    # JSON has 2021
    ts_json = datetime(2021, 1, 1).timestamp()
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
        
    # EXIF has 2022 (Valid)
    ts_exif = datetime(2022, 1, 1).timestamp()
    media_metadata = MediaMetadata(timestamp=ts_exif)
    
    # Call _process_single_file and check return value
    mock_media_type = MagicMock()
    result = processor._process_single_file(img, mock_media_type, media_metadata)
    
    # Result should be (final_path, media_type, json_metadata)
    assert result is not None
    final_path, _, json_metadata_to_write = result
    
    # Verify 'timestamp' is NOT in the metadata to write (should be None)
    assert json_metadata_to_write.timestamp is None
    
    # Also verify destination is based on EXIF (2022)
    expected_dest = processor.dest_dir / "2022" / "01" / "overwrite.jpg"
    assert expected_dest.exists()
