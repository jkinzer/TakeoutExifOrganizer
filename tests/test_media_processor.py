import pytest
import json
import os
from datetime import datetime

from takeout_import.media_processor import MediaProcessor
from takeout_import.media_metadata import MediaMetadata
from takeout_import.persistence_manager import PersistenceManager
from takeout_import.media_type import get_media_type

@pytest.fixture
def processor(tmp_path):
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "dest"
    source_dir.mkdir()
    dest_dir.mkdir()
    return MediaProcessor(source_dir, dest_dir, PersistenceManager.in_memory())

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
    ts = 1686830400
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(ts)}}, f)
        
    # Simulate DB record
    media_type = get_media_type(mp_file)
    file_id = processor.persistence.add_file(mp_file, media_type, 0, 0)
    
    # Simulate Metadata
    json_metadata = MediaMetadata(timestamp=float(ts))
    processor.persistence.save_metadata(file_id, 'JSON', json_metadata)
    processor.persistence.save_metadata(file_id, 'MERGED', json_metadata) # Assume merged
    
    # Determine target path (Resolution Phase logic)
    timestamp = processor._determine_timestamp(mp_file, MediaMetadata(), json_metadata)
    target_path = processor.file_organizer.get_target_path(timestamp, mp_file.name)
    processor.persistence.update_target_path(file_id, target_path)
    
    # Execute
    file_record = processor.persistence.get_file_by_id(file_id)
    processor._execute_single_file(file_record)
    
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
        
    media_type = get_media_type(img)

    # Scenario 1: EXIF present and valid -> Should use EXIF (2022)
    ts_exif = datetime(2022, 1, 1).timestamp()
    media_metadata = MediaMetadata(timestamp=ts_exif)
    json_metadata = MediaMetadata(timestamp=ts_json)
    
    timestamp = processor._determine_timestamp(img, media_metadata, json_metadata)
    target_path = processor.file_organizer.get_target_path(timestamp, img.name)
    
    assert "2022" in str(target_path)
    
    # Scenario 2: EXIF missing -> Should use JSON (2021)
    media_metadata = MediaMetadata()
    timestamp = processor._determine_timestamp(img, media_metadata, json_metadata)
    target_path = processor.file_organizer.get_target_path(timestamp, img.name)
    
    assert "2021" in str(target_path)

    # Scenario 3: EXIF invalid (<1999), JSON valid -> Should use JSON (2021)
    ts_invalid = datetime(1990, 1, 1).timestamp()
    media_metadata = MediaMetadata(timestamp=ts_invalid)
    timestamp = processor._determine_timestamp(img, media_metadata, json_metadata)
    target_path = processor.file_organizer.get_target_path(timestamp, img.name)
    
    assert "2021" in str(target_path)

    # Scenario 4: EXIF invalid, JSON invalid -> Should use Mtime (2020)
    json_metadata = MediaMetadata(timestamp=ts_invalid)
    timestamp = processor._determine_timestamp(img, media_metadata, json_metadata)
    target_path = processor.file_organizer.get_target_path(timestamp, img.name)
    
    assert "2020" in str(target_path)

def test_process_file_no_overwrite(processor):
    # Verify that if EXIF is valid, we do NOT overwrite it with JSON
    img = processor.source_dir / "overwrite.jpg"
    img.touch()
    
    ts_json = datetime(2021, 1, 1).timestamp()
    ts_exif = datetime(2022, 1, 1).timestamp()
    
    json_metadata = MediaMetadata(timestamp=ts_json)
    media_metadata = MediaMetadata(timestamp=ts_exif)
    media_type = get_media_type(img)
    
    merged = processor._merge_metadata(img, media_type, media_metadata, json_metadata)
    
    # Verify 'timestamp' is None (preserved)
    assert merged.timestamp is None
