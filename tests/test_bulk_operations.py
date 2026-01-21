import unittest
import shutil
import tempfile
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime

from takeout_import.metadata_handler import MetadataHandler
from takeout_import.media_processor import MediaProcessor
from takeout_import.media_type import MediaType, SUPPORTED_MEDIA
from tests.media_helper import create_dummy_image

class TestBulkOperations(unittest.TestCase):
    def setUp(self):
        self.source_dir = Path(tempfile.mkdtemp())
        self.dest_dir = Path(tempfile.mkdtemp())
        self.handler = MetadataHandler()

    def tearDown(self):
        shutil.rmtree(self.source_dir)
        shutil.rmtree(self.dest_dir)


    def test_read_metadata_batch(self):
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        create_dummy_image(file1)
        create_dummy_image(file2)

        
        # Write metadata to files using internal exiftool
        ts1_str = "2023:01:01 12:00:00"
        ts2_str = "2023:01:02 12:00:00"
        
        self.handler._exif_tool.set_tags(
            [str(file1)], 
            tags={'DateTimeOriginal': ts1_str, 'CreateDate': ts1_str},
            params=['-overwrite_original']
        )
        self.handler._exif_tool.set_tags(
            [str(file2)], 
            tags={'DateTimeOriginal': ts2_str, 'CreateDate': ts2_str},
            params=['-overwrite_original']
        )
        
        # Call batch read
        # Pass (file, MediaType) tuples
        results = self.handler.read_metadata_batch([
            (file1, SUPPORTED_MEDIA.get('.jpg')), 
            (file2, SUPPORTED_MEDIA.get('.jpg'))
        ])
        
        # Verify results
        self.assertEqual(len(results), 2)
        
        # Expected timestamps (local time)
        ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
        
        self.assertEqual(results[file1]['timestamp'], ts1)
        self.assertEqual(results[file2]['timestamp'], ts2)


    def test_read_metadata_batch_missing_file(self):
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        file3 = self.source_dir / "file3.jpg"
        create_dummy_image(file1)
        # file2 is missing
        create_dummy_image(file3)

        
        # Write metadata
        ts1_str = "2023:01:01 12:00:00"
        ts3_str = "2023:01:03 12:00:00"
        
        self.handler._exif_tool.set_tags(
            [str(file1)], 
            tags={'DateTimeOriginal': ts1_str},
            params=['-overwrite_original']
        )
        self.handler._exif_tool.set_tags(
            [str(file3)], 
            tags={'DateTimeOriginal': ts3_str},
            params=['-overwrite_original']
        )
        
        # Call batch read with missing file2
        # ExifTool fails the batch if a file is missing, so we expect empty results and an error log
        with self.assertLogs('takeout_import.metadata_handler', level='ERROR') as cm:
            results = self.handler.read_metadata_batch([
                (file1, SUPPORTED_MEDIA.get('.jpg')), 
                (file2, SUPPORTED_MEDIA.get('.jpg')), 
                (file3, SUPPORTED_MEDIA.get('.jpg'))
            ])
            
            self.assertTrue(any("Batch read failed" in o for o in cm.output))
        
        # Verify results are empty due to failure
        self.assertEqual(len(results), 0)



    def test_write_metadata_batch(self):
        # Setup files
        file1 = self.source_dir / "file1.jpg"
        file2 = self.source_dir / "file2.jpg"
        create_dummy_image(file1)
        create_dummy_image(file2)

        
        # Use specific timestamps
        ts1 = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        ts2 = datetime(2023, 1, 2, 12, 0, 0).timestamp()
        
        # Use real MediaType
        mt = SUPPORTED_MEDIA.get('.jpg')
        
        write_ops = [
            (file1, mt, {'timestamp': ts1}), 
            (file2, mt, {'timestamp': ts2})
        ]
        
        # Call batch write
        self.handler.write_metadata_batch(write_ops)
            
        # Verify metadata was written
        # Read back using handler
        results = self.handler.read_metadata_batch([
            (file1, mt), 
            (file2, mt)
        ])
        
        self.assertEqual(results[file1]['timestamp'], ts1)
        self.assertEqual(results[file2]['timestamp'], ts2)


    def test_chunking(self):
        # Setup 3 files
        files = []
        for i in range(3):
            f = self.source_dir / f"chunk{i}.jpg"
            create_dummy_image(f)
            files.append(f)

            
        # Initialize Processor with batch_size=2
        processor = MediaProcessor(self.source_dir, self.dest_dir, batch_size=2)
        
        # Run Process and check logs
        with self.assertLogs('takeout_import.media_processor', level='INFO') as cm:
            processor.process()
            
            # Verify chunk messages
            # Should see "Processing chunk 1/2" and "Processing chunk 2/2"
            self.assertTrue(any("Processing chunk 1/2" in o for o in cm.output))
            self.assertTrue(any("Processing chunk 2/2" in o for o in cm.output))


    def test_media_processor_pipeline(self):
        # Setup files
        img = self.source_dir / "test.jpg"
        create_dummy_image(img)
        json_file = self.source_dir / "test.jpg.json"

        
        # JSON has 2023
        ts_json = 1672574400.0 # 2023-01-01
        with open(json_file, 'w') as f:
            json.dump({"photoTakenTime": {"timestamp": str(int(ts_json))}}, f)
            
        # Initialize Processor
        processor = MediaProcessor(self.source_dir, self.dest_dir)
        
        # Run Process
        processor.process()
        
        # Verify Phase 4 (Write) called
        # Should write metadata to destination file
        dest_file = self.dest_dir / "2023" / "01" / "test.jpg"
        self.assertTrue(dest_file.exists())
        
        # Verify metadata in dest file
        # Check if DateTimeOriginal is set
        mt = SUPPORTED_MEDIA.get('.jpg')
        results = processor.metadata_handler.read_metadata_batch([(dest_file, mt)])
        self.assertIn(dest_file, results)
        self.assertEqual(results[dest_file]['timestamp'], ts_json)


if __name__ == '__main__':
    unittest.main()
