import unittest
import shutil
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from takeout_import.metadata_handler import MetadataHandler
from takeout_import.media_processor import MediaProcessor

class TestBulkOperations(unittest.TestCase):
    def setUp(self):
        self.source_dir = Path(tempfile.mkdtemp())
        self.dest_dir = Path(tempfile.mkdtemp())
        self.handler = MetadataHandler()

    def tearDown(self):
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.dest_dir)

    @patch('takeout_import.metadata_handler.exiftool.ExifToolHelper')
    def test_read_metadata_batch(self, mock_et_cls):
        # Setup mock
        mock_et = mock_et_cls.return_value
        mock_et.__enter__.return_value = mock_et
        
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        file1.touch()
        file2.touch()
        
        # Mock get_tags return
        mock_et.get_tags.return_value = [
            {'SourceFile': str(file1), 'EXIF:DateTimeOriginal': '2023:01:01 12:00:00'},
            {'SourceFile': str(file2), 'EXIF:DateTimeOriginal': '2023:01:02 12:00:00'}
        ]
        
        # Call batch read
        with self.handler as h:
            results = h.read_metadata_batch([file1, file2])
        
        # Verify results
        self.assertEqual(len(results), 2)
        
        # Expected timestamps (local time)
        # The code parses '2023:01:01 12:00:00' as naive datetime, then calls .timestamp()
        # which assumes it's local time.
        from datetime import datetime
        ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
        
        self.assertEqual(results[file1]['timestamp'], ts1)
        self.assertEqual(results[file2]['timestamp'], ts2)
        
        # Verify ExifTool called with list
        mock_et.get_tags.assert_called_once()
        args, kwargs = mock_et.get_tags.call_args
        self.assertEqual(set(args[0]), {str(file1.resolve()), str(file2.resolve())})

    @patch('takeout_import.metadata_handler.exiftool.ExifToolHelper')
    def test_read_metadata_batch_missing_file(self, mock_et_cls):
        # Setup mock
        mock_et = mock_et_cls.return_value
        mock_et.__enter__.return_value = mock_et
        
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        file3 = self.source_dir / "file3.jpg"
        file1.touch()
        file2.touch()
        file3.touch()
        
        # Mock get_tags return: OMIT file2
        # Use resolved paths for SourceFile as the code expects
        mock_et.get_tags.return_value = [
            {'SourceFile': str(file1.resolve()), 'EXIF:DateTimeOriginal': '2023:01:01 12:00:00'},
            {'SourceFile': str(file3.resolve()), 'EXIF:DateTimeOriginal': '2023:01:03 12:00:00'}
        ]
        
        # Call batch read
        with self.handler as h:
            results = h.read_metadata_batch([file1, file2, file3])
        
        # Verify results
        self.assertEqual(len(results), 2)
        self.assertIn(file1, results)
        self.assertIn(file3, results)
        self.assertNotIn(file2, results)
        
        # Verify timestamps
        from datetime import datetime
        ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        ts3 = datetime(2023, 1, 3, 12, 0, 0).timestamp()
        
        self.assertEqual(results[file1]['timestamp'], ts1)
        self.assertEqual(results[file3]['timestamp'], ts3)

    @patch('takeout_import.metadata_handler.exiftool.ExifToolHelper')
    def test_write_metadata_batch(self, mock_et_cls):
        # Setup mock
        mock_et = mock_et_cls.return_value
        mock_et.__enter__.return_value = mock_et
        
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        
        # Use specific timestamps
        from datetime import datetime
        ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
        
        write_ops = [
            (file1, {'timestamp': ts1}), 
            (file2, {'timestamp': ts2})
        ]
        
        # Call batch write
        with self.handler as h:
            h.write_metadata_batch(write_ops)
            
        # Verify set_tags called twice (since we loop)
        self.assertEqual(mock_et.set_tags.call_count, 2)
        
        # Verify calls
        # The code converts timestamp to local string for EXIF tags
        str1 = datetime.fromtimestamp(ts1).strftime("%Y:%m:%d %H:%M:%S")
        str2 = datetime.fromtimestamp(ts2).strftime("%Y:%m:%d %H:%M:%S")
        
        calls = [
            call([str(file1)], tags={'DateTimeOriginal': str1, 'CreateDate': str1, 'ModifyDate': str1}, params=['-overwrite_original']),
            call([str(file2)], tags={'DateTimeOriginal': str2, 'CreateDate': str2, 'ModifyDate': str2}, params=['-overwrite_original'])
        ]
        mock_et.set_tags.assert_has_calls(calls, any_order=True)

    @patch('takeout_import.metadata_handler.exiftool.ExifToolHelper')
    def test_chunking(self, mock_et_cls):
        # Setup mock
        mock_et = mock_et_cls.return_value
        mock_et.__enter__.return_value = mock_et
        
        # Setup 3 files
        files = []
        for i in range(3):
            f = self.source_dir / f"chunk{i}.jpg"
            f.touch()
            files.append(f)
            
        # Mock get_tags to return empty (no EXIF)
        mock_et.get_tags.return_value = [{'SourceFile': str(f.resolve())} for f in files]
        
        # Initialize Processor with batch_size=2
        processor = MediaProcessor(self.source_dir, self.dest_dir, batch_size=2)
        
        # Run Process
        processor.process()
        
        # Verify get_tags called twice (3 files / 2 = 2 chunks)
        # First call: 2 files
        # Second call: 1 file
        self.assertEqual(mock_et.get_tags.call_count, 2)
        
        # Verify calls arguments
        args1, _ = mock_et.get_tags.call_args_list[0]
        args2, _ = mock_et.get_tags.call_args_list[1]
        
        self.assertEqual(len(args1[0]), 2)
        self.assertEqual(len(args2[0]), 1)

    @patch('takeout_import.metadata_handler.exiftool.ExifToolHelper')
    def test_media_processor_pipeline(self, mock_et_cls):
        # Setup mock
        mock_et = mock_et_cls.return_value
        mock_et.__enter__.return_value = mock_et
        
        # Setup files
        img = self.source_dir / "test.jpg"
        img.touch()
        json_file = self.source_dir / "test.jpg.json"
        
        # JSON has 2023
        ts_json = 1672574400.0 # 2023-01-01
        with open(json_file, 'w') as f:
            json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
            
        # Mock get_tags (Phase 2) -> Return empty (no EXIF)
        mock_et.get_tags.return_value = [{'SourceFile': str(img)}]
        
        # Initialize Processor
        processor = MediaProcessor(self.source_dir, self.dest_dir)
        
        # Run Process
        processor.process()
        
        # Verify Phase 2 (Read) called
        mock_et.get_tags.assert_called()
        
        # Verify Phase 4 (Write) called
        # Should write metadata to destination file
        dest_file = self.dest_dir / "2023" / "01" / "test.jpg"
        self.assertTrue(dest_file.exists())
        
        mock_et.set_tags.assert_called()
        args, _ = mock_et.set_tags.call_args
        self.assertEqual(args[0], [str(dest_file)])

if __name__ == '__main__':
    unittest.main()
