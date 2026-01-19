import unittest
import shutil
import tempfile
from pathlib import Path

from takeout_import.file_organizer import FileOrganizer

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

if __name__ == '__main__':
    unittest.main()
