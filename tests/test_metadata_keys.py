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
from takeout_import.media_type import SUPPORTED_MEDIA
from tests.media_helper import create_dummy_image


class TestMetadataKeys(unittest.TestCase):
    def setUp(self):
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
        
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "dummy.jpg"
            create_dummy_image(file_path)
            
            mt = SUPPORTED_MEDIA.get('.jpg')
            
            self.handler.write_metadata(file_path, mt, metadata)
            
            # Read back all tags
            tags_list = self.handler._exif_tool.get_tags([str(file_path)], tags=None, params=["-G1"])

            tags = tags_list[0]
            
            # People mappings
            # ExifTool might return list or string depending on options. 
            # PyExifTool usually returns list for list tags if -n is not used or if configured.
            # Let's check what we get.
            
            # XMP:Subject
            self.assertIn('XMP:Subject', tags)
            subject = tags['XMP:Subject']
            # It might be a list or a single string if only one item, but here we have two.
            if isinstance(subject, list):
                self.assertEqual(sorted(subject), ["Jane Smith", "John Doe"])
            else:
                # Should be list
                self.assertEqual(subject, ["John Doe", "Jane Smith"]) # Order might vary?
                
            # IPTC:Keywords
            if 'IPTC:Keywords' in tags:
                keywords = tags['IPTC:Keywords']
                if isinstance(keywords, list):
                    self.assertEqual(sorted(keywords), ["Jane Smith", "John Doe"])
            
            # XMP:PersonInImage
            if 'XMP:PersonInImage' in tags:
                person = tags['XMP:PersonInImage']
                if isinstance(person, list):
                    self.assertEqual(sorted(person), ["Jane Smith", "John Doe"])
            
            # URL mapping
            # ExifIFD:UserComment might be reported as EXIF:UserComment
            if 'ExifIFD:UserComment' in tags:
                self.assertEqual(tags['ExifIFD:UserComment'], "https://photos.google.com/share/...")
            else:
                self.assertIn('EXIF:UserComment', tags)
                self.assertEqual(tags['EXIF:UserComment'], "https://photos.google.com/share/...")
            
            # Verify Title and Description are NOT written
            self.assertNotIn('XMP:Title', tags)
            self.assertNotIn('IPTC:Caption-Abstract', tags)
            self.assertNotIn('XMP:Description', tags)


if __name__ == '__main__':
    unittest.main()
