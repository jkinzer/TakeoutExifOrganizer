import pytest
from pathlib import Path
from takeout_import.file_organizer import FileOrganizer

@pytest.fixture
def organizer(tmp_path):
    return FileOrganizer(tmp_path)

def test_get_target_path(organizer):
    # Use a timestamp that is safe across timezones (e.g. noon UTC)
    # 2023-06-15 12:00:00 UTC = 1686830400
    ts = 1686830400 
    path = organizer.get_target_path(ts, "test.jpg")
    expected = organizer.dest_root / "2023" / "06" / "test.jpg"
    assert path == expected

def test_resolve_collision(organizer):
    # Create a file that conflicts
    ts = 1672531200
    target = organizer.get_target_path(ts, "test.jpg")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch()
    
    resolved = organizer.resolve_collision(target)
    assert resolved.name == "test_1.jpg"
    
    # Create the _1 file and test again
    resolved.touch()
    resolved_2 = organizer.resolve_collision(target)
    assert resolved_2.name == "test_2.jpg"
