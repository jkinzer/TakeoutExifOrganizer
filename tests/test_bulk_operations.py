import pytest
import shutil
import json
import logging
from pathlib import Path
from datetime import datetime

from takeout_import.metadata_handler import MetadataHandler
from takeout_import.media_processor import MediaProcessor
from takeout_import.media_type import MediaType, SUPPORTED_MEDIA
from takeout_import.media_metadata import MediaMetadata
from tests.media_helper import create_dummy_image

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

def test_read_metadata_batch(test_dirs, handler):
    source_dir, _ = test_dirs
    # Setup files
    file1 = source_dir / "file1.jpg"
    file2 = source_dir / "file2.jpg"
    create_dummy_image(file1)
    create_dummy_image(file2)

    # Write metadata to files using internal exiftool
    ts1_str = "2023:01:01 12:00:00"
    ts2_str = "2023:01:02 12:00:00"
    
    handler._exif_tool.set_tags(
        [str(file1)], 
        tags={'DateTimeOriginal': ts1_str, 'CreateDate': ts1_str},
        params=['-overwrite_original']
    )
    handler._exif_tool.set_tags(
        [str(file2)], 
        tags={'DateTimeOriginal': ts2_str, 'CreateDate': ts2_str},
        params=['-overwrite_original']
    )
    
    # Call batch read
    # Pass (file, MediaType) tuples
    results = handler.read_metadata_batch([
        (file1, SUPPORTED_MEDIA.get('.jpg')), 
        (file2, SUPPORTED_MEDIA.get('.jpg'))
    ])
    
    # Verify results
    assert len(results) == 2
    
    # Expected timestamps (local time)
    ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
    ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
    
    assert results[file1].timestamp == ts1
    assert results[file2].timestamp == ts2

def test_read_metadata_batch_missing_file(test_dirs, handler, caplog):
    source_dir, _ = test_dirs
    # Setup files
    file1 = source_dir / "file1.jpg"
    file2 = source_dir / "file2.jpg"
    file3 = source_dir / "file3.jpg"
    create_dummy_image(file1)
    # file2 is missing
    create_dummy_image(file3)

    # Write metadata
    ts1_str = "2023:01:01 12:00:00"
    ts3_str = "2023:01:03 12:00:00"
    
    handler._exif_tool.set_tags(
        [str(file1)], 
        tags={'DateTimeOriginal': ts1_str},
        params=['-overwrite_original']
    )
    handler._exif_tool.set_tags(
        [str(file3)], 
        tags={'DateTimeOriginal': ts3_str},
        params=['-overwrite_original']
    )
    
    # Call batch read with missing file2
    # ExifTool fails the batch if a file is missing, so we expect empty results and an error log
    with caplog.at_level(logging.ERROR):
        results = handler.read_metadata_batch([
            (file1, SUPPORTED_MEDIA.get('.jpg')), 
            (file2, SUPPORTED_MEDIA.get('.jpg')), 
            (file3, SUPPORTED_MEDIA.get('.jpg'))
        ])
        
        assert any("Batch read failed" in r.message for r in caplog.records)
    
    # Verify results are empty due to failure
    assert len(results) == 0

def test_write_metadata_batch(test_dirs, handler):
    source_dir, _ = test_dirs
    # Setup files
    file1 = source_dir / "file1.jpg"
    file2 = source_dir / "file2.jpg"
    create_dummy_image(file1)
    create_dummy_image(file2)

    # Use specific timestamps
    ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
    ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
    
    # Use real MediaType
    mt = SUPPORTED_MEDIA.get('.jpg')
    
    write_ops = [
        (file1, mt, MediaMetadata(timestamp=ts1)), 
        (file2, mt, MediaMetadata(timestamp=ts2))
    ]
    
    # Call batch write
    handler.write_metadata_batch(write_ops)
        
    # Verify metadata was written
    # Read back using handler
    results = handler.read_metadata_batch([
        (file1, mt), 
        (file2, mt)
    ])
    
    assert results[file1].timestamp == ts1
    assert results[file2].timestamp == ts2

def test_chunking(test_dirs, caplog):
    source_dir, dest_dir = test_dirs
    # Setup 3 files
    files = []
    for i in range(3):
        f = source_dir / f"chunk{i}.jpg"
        create_dummy_image(f)
        files.append(f)

    # Initialize Processor with batch_size=2
    processor = MediaProcessor(source_dir, dest_dir, batch_size=2)
    
    # Run Process and check logs
    with caplog.at_level(logging.INFO):
        processor.process()
        
        # Verify chunk messages
        # Should see "Processing chunk 1/2" and "Processing chunk 2/2"
        assert any("Processing chunk 1/2" in r.message for r in caplog.records)
        assert any("Processing chunk 2/2" in r.message for r in caplog.records)

def test_media_processor_pipeline(test_dirs):
    source_dir, dest_dir = test_dirs
    # Setup files
    img = source_dir / "test.jpg"
    create_dummy_image(img)
    json_file = source_dir / "test.jpg.json"

    # JSON has 2023
    ts_json = 1672574400.0 # 2023-01-01
    with open(json_file, 'w') as f:
        json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
        
    # Initialize Processor
    processor = MediaProcessor(source_dir, dest_dir)
    
    # Run Process
    processor.process()
    
    # Verify Phase 4 (Write) called
    # Should write metadata to destination file
    dest_file = dest_dir / "2023" / "01" / "test.jpg"
    assert dest_file.exists()
    
    # Verify metadata in dest file
    # Check if DateTimeOriginal is set
    mt = SUPPORTED_MEDIA.get('.jpg')
    results = processor.metadata_handler.read_metadata_batch([(dest_file, mt)])
    assert dest_file in results
    assert results[dest_file].timestamp == ts_json
