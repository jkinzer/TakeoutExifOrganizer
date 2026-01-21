import pytest
import sqlite3
from pathlib import Path
from datetime import datetime
from takeout_import.persistence_manager import PersistenceManager, FileStatus, ProcessingPhase
from takeout_import.media_type import get_media_type
from takeout_import.media_metadata import MediaMetadata

@pytest.fixture
def sqlite_persistence(tmp_path):
    db_path = tmp_path / "test.db"
    pm = PersistenceManager.file_db(db_path)
    pm.initialize()
    yield pm
    pm.close()

@pytest.fixture
def memory_persistence():
    pm = PersistenceManager.in_memory()
    pm.initialize()
    yield pm
    pm.close()

@pytest.mark.parametrize("persistence_fixture", ["sqlite_persistence", "memory_persistence"])
def test_add_and_get_file(persistence_fixture, request):
    pm = request.getfixturevalue(persistence_fixture)
    path = Path("/tmp/test.jpg")
    media_type = get_media_type(path)
    file_size = 1024
    mtime = 1234567890.0

    file_id = pm.add_file(path, media_type, file_size, mtime)
    assert file_id is not None

    # Get by ID
    file_record = pm.get_file_by_id(file_id)
    assert file_record['source_path'] == str(path)
    assert file_record['media_type'] == media_type.type
    assert file_record['status'] == FileStatus.NEW.value
    assert file_record['phase'] == ProcessingPhase.DISCOVERY.value

    # Get by Path
    file_record_path = pm.get_file_by_path(path)
    assert file_record_path['id'] == file_id

def test_sqlite_persistence_reopen(tmp_path):
    db_path = tmp_path / "test_reopen.db"
    
    # Open, add file, close
    pm1 = PersistenceManager.file_db(db_path)
    pm1.initialize()
    path = Path("/tmp/test.jpg")
    pm1.add_file(path, get_media_type(path), 100, 100.0)
    pm1.close()

    # Reopen, verify file exists
    pm2 = PersistenceManager.file_db(db_path)
    pm2.initialize()
    file_record = pm2.get_file_by_path(path)
    assert file_record is not None
    pm2.close()

@pytest.mark.parametrize("persistence_fixture", ["sqlite_persistence", "memory_persistence"])
def test_update_status(persistence_fixture, request):
    pm = request.getfixturevalue(persistence_fixture)
    path = Path("/tmp/test.jpg")
    file_id = pm.add_file(path, get_media_type(path), 100, 100.0)

    pm.update_status(file_id, FileStatus.METADATA_READ, ProcessingPhase.METADATA_READ)
    
    file_record = pm.get_file_by_id(file_id)
    assert file_record['status'] == FileStatus.METADATA_READ.value
    assert file_record['phase'] == ProcessingPhase.METADATA_READ.value

@pytest.mark.parametrize("persistence_fixture", ["sqlite_persistence", "memory_persistence"])
def test_save_and_get_metadata(persistence_fixture, request):
    pm = request.getfixturevalue(persistence_fixture)
    path = Path("/tmp/test.jpg")
    file_id = pm.add_file(path, get_media_type(path), 100, 100.0)

    metadata = MediaMetadata(timestamp=12345.0, people=["Alice", "Bob"])
    pm.save_metadata(file_id, "JSON", metadata)

    retrieved = pm.get_metadata(file_id, "JSON")
    assert retrieved.timestamp == 12345.0
    assert retrieved.people == ["Alice", "Bob"]

    # Test missing metadata
    assert pm.get_metadata(file_id, "MEDIA") is None

@pytest.mark.parametrize("persistence_fixture", ["sqlite_persistence", "memory_persistence"])
def test_get_files_by_status(persistence_fixture, request):
    pm = request.getfixturevalue(persistence_fixture)
    
    path1 = Path("/tmp/1.jpg")
    path2 = Path("/tmp/2.jpg")
    
    pm.add_file(path1, get_media_type(path1), 100, 100.0)
    id2 = pm.add_file(path2, get_media_type(path2), 100, 100.0)
    
    pm.update_status(id2, FileStatus.SUCCESS, ProcessingPhase.EXECUTION)

    new_files = pm.get_files_by_status([FileStatus.NEW])
    assert len(new_files) == 1
    assert new_files[0]['source_path'] == str(path1)

    success_files = pm.get_files_by_status([FileStatus.SUCCESS])
    assert len(success_files) == 1
    assert success_files[0]['source_path'] == str(path2)
