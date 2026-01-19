import unittest
import shutil
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

# Mock exiftool module
# sys.modules['exiftool'] = MagicMock()

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from takeout_import.metadata_handler import MetadataHandler

class TestMetadataKeys(unittest.TestCase):
    def setUp(self):
        # Mock shutil.which to avoid SystemExit
        with patch('shutil.which', return_value='/usr/bin/exiftool'):
            self.handler = MetadataHandler()
    
    def test_parse_json_sidecar_new_keys(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            json.dump({
                "photoTakenTime": {"timestamp": "1672531200"},
                "title": "My Vacation Photo",
                "description": "A beautiful view",
                "url": "https://photos.google.com/share/...",
                "people": [
                    {"name": "John Doe"},
                    {"name": "Jane Smith"}
                ]
            }, tmp)
            tmp_path = Path(tmp.name)
        
        try:
            metadata = self.handler.parse_json_sidecar(tmp_path)
            
            # Verify new keys
            self.assertEqual(metadata['url'], "https://photos.google.com/share/...")
            self.assertEqual(metadata['people'], ["John Doe", "Jane Smith"])
            
            # Verify Title and Description are NOT present
            self.assertNotIn('title', metadata)
            self.assertNotIn('description', metadata)
            
            # Verify existing keys still work
            self.assertEqual(metadata['timestamp'], 1672531200)
            
        finally:
            os.remove(tmp_path)

    def test_write_metadata_new_keys(self):
        metadata = {
            'title': "My Vacation Photo",
            'url': "https://photos.google.com/share/...",
            'people': ["John Doe", "Jane Smith"],
            'description': "A beautiful view"
        }
        
        file_path = Path("dummy.jpg")
        
        with patch('exiftool.ExifToolHelper') as MockHelper:
            mock_et = MockHelper.return_value
            mock_et.__enter__.return_value = mock_et
            
            self.handler.write_metadata(file_path, metadata)
            
            # Verify set_tags call
            args, kwargs = mock_et.set_tags.call_args
            tags = kwargs['tags']
            
            # People mappings
            self.assertEqual(tags['XMP:Subject'], ["John Doe", "Jane Smith"])
            self.assertEqual(tags['IPTC:Keywords'], ["John Doe", "Jane Smith"])
            self.assertEqual(tags['XMP:PersonInImage'], ["John Doe", "Jane Smith"])
            
            # URL mapping
            self.assertEqual(tags['ExifIFD:UserComment'], "https://photos.google.com/share/...")
            
            # Verify Title and Description are NOT written
            self.assertNotIn('XMP:Title', tags)
            self.assertNotIn('Caption-Abstract', tags)
            self.assertNotIn('Description', tags)

if __name__ == '__main__':
    unittest.main()
