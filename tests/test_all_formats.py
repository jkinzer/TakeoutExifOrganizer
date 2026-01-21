import unittest
import tempfile
import shutil
from pathlib import Path
from takeout_import.media_type import SUPPORTED_MEDIA
from takeout_import.metadata_handler import MetadataHandler
from tests.media_helper import create_dummy_media

class TestAllFormats(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.handler = MetadataHandler()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_all_formats(self):
        """Iterate through all supported formats and verify read/write."""
        for ext, media_type in SUPPORTED_MEDIA.items():
            with self.subTest(ext=ext):
                file_path = self.tmp_dir / f"test{ext}"
                
                # Create dummy file
                create_dummy_media(file_path)
                
                if not file_path.exists() or file_path.stat().st_size == 0:
                    print(f"Skipping {ext} - failed to create valid file")
                    continue

                # Write metadata
                metadata = {'timestamp': 1672531200} # 2023-01-01
                try:
                    self.handler.write_metadata(file_path, media_type, metadata)
                except Exception as e:
                    self.fail(f"Failed to write metadata to {ext}: {e}")

                # Read metadata
                try:
                    results = self.handler.read_metadata_batch([(file_path, media_type)])
                    
                    if media_type.supports_tags():
                        self.assertIn(file_path, results)
                        if 'timestamp' in results[file_path]:
                             self.assertEqual(results[file_path]['timestamp'], 1672531200)
                        else:
                             # Some formats might support tags but we failed to write/read timestamp specifically
                             # For now, fail if supports_tags is True but no timestamp
                             self.fail(f"Timestamp not found for {ext} which supports tags")
                    else:
                        # If tags not supported, we expect either no result or empty result
                        if file_path in results:
                            self.assertNotIn('timestamp', results[file_path])
                except Exception as e:
                    self.fail(f"Failed to read metadata from {ext}: {e}")

if __name__ == '__main__':
    unittest.main()
