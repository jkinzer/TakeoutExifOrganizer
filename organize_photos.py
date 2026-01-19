#!/usr/bin/env python3
import os
import sys
import json
import shutil
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

import exiftool

class MetadataHandler:
    """Handles parsing JSON sidecars and reading/writing metadata via PyExifTool."""
    
    def __init__(self):
        # PyExifTool will look for 'exiftool' in PATH by default.
        # We can verify it exists by checking shutil.which if we want, 
        # but PyExifTool might raise an error if not found.
        if shutil.which("exiftool") is None:
             logger.error("ExifTool not found in PATH. Please install ExifTool.")
             sys.exit(1)

    def parse_json_sidecar(self, json_path: Path) -> Dict[str, Any]:
        """Parses the JSON sidecar file and extracts relevant metadata."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = {}
            
            # Timestamp
            if 'photoTakenTime' in data and 'timestamp' in data['photoTakenTime']:
                metadata['timestamp'] = int(data['photoTakenTime']['timestamp'])
            
            # GPS
            if 'geoData' in data:
                geo = data['geoData']
                if 'latitude' in geo and 'longitude' in geo and 'altitude' in geo:
                    metadata['gps'] = {
                        'latitude': geo['latitude'],
                        'longitude': geo['longitude'],
                        'altitude': geo['altitude']
                    }
            
            # URL
            if 'url' in data and data['url']:
                metadata['url'] = data['url']

            # People
            if 'people' in data and isinstance(data['people'], list):
                people_names = []
                for person in data['people']:
                    if 'name' in person and person['name']:
                        people_names.append(person['name'])
                if people_names:
                    metadata['people'] = people_names
                
            return metadata
        except Exception as e:
            logger.warning(f"Failed to parse JSON {json_path}: {e}")
            return {}

    def read_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Reads metadata from the media file using PyExifTool."""
        try:
            with exiftool.ExifToolHelper() as et:
                # Get common date tags
                # We ask for Composite:SubSecDateTimeOriginal first as it's often most accurate if available
                tags_to_read = ['DateTimeOriginal', 'CreateDate', 'ModifyDate']
                result = et.get_tags(str(file_path), tags=tags_to_read)
                
                if not result:
                    return {}
                
                data = result[0]
                metadata = {}
                
                # Priority of tags to check
                date_tags = [
                    'Composite:SubSecDateTimeOriginal',
                    'EXIF:DateTimeOriginal', 
                    'XMP:DateTimeOriginal', 
                    'DateTimeOriginal',
                    'EXIF:CreateDate', 
                    'XMP:CreateDate', 
                    'CreateDate',
                    'EXIF:ModifyDate', 
                    'XMP:ModifyDate', 
                    'ModifyDate'
                ]
                
                for tag in date_tags:
                    # PyExifTool might return keys with group names or without depending on how it was found
                    # But get_tags usually returns keys as they are in the file or specific group if requested.
                    # Since we didn't specify group in tags_to_read, it might be loose.
                    # However, let's check both exact match and just the tag name if we can.
                    # Actually, let's just iterate through the returned data and look for "DateTimeOriginal" etc.
                    pass

                # Simpler approach: Check the data dict for our preferred keys
                # We'll just look for the first one that exists and is valid
                found_date = None
                
                # Helper to find key in data (ignoring group prefix if needed, but exact match is safer)
                # Let's just check the keys present in data
                keys = data.keys()
                
                # Map our priority list to potential keys in the result
                # We need to be careful because 'DateTimeOriginal' request might return 'EXIF:DateTimeOriginal'
                
                # Let's try to find the best date string
                for priority_tag in ['DateTimeOriginal', 'CreateDate', 'ModifyDate']:
                    # Find keys that end with this tag
                    matches = [k for k in keys if k.endswith(priority_tag)]
                    # Sort matches? usually EXIF is better than XMP? 
                    # Let's just take the first one found for now, or prefer EXIF.
                    
                    exif_match = next((k for k in matches if 'EXIF' in k), None)
                    if exif_match:
                        found_date = data[exif_match]
                        break
                    
                    if matches:
                        found_date = data[matches[0]]
                        break
                
                if found_date:
                    # Parse date. ExifTool format: YYYY:mm:dd HH:MM:SS
                    # It might have subseconds or timezone: YYYY:mm:dd HH:MM:SS.ss+HH:MM
                    try:
                        # Take first 19 chars for standard format
                        clean_str = str(found_date)[:19]
                        dt = datetime.strptime(clean_str, "%Y:%m:%d %H:%M:%S")
                        metadata['timestamp'] = dt.timestamp()
                    except ValueError:
                        pass
                        
                return metadata

        except Exception as e:
            logger.warning(f"Failed to read metadata from {file_path}: {e}")
            return {}

    def write_metadata(self, file_path: Path, metadata: Dict[str, Any], dry_run: bool = False):
        """Writes metadata to the media file using PyExifTool."""
        if not metadata:
            return

        tags = {}
        
        # Date/Time
        if 'timestamp' in metadata:
            dt_str = datetime.fromtimestamp(metadata['timestamp']).strftime("%Y:%m:%d %H:%M:%S")
            tags['DateTimeOriginal'] = dt_str
            tags['CreateDate'] = dt_str
            tags['ModifyDate'] = dt_str

        # GPS
        if 'gps' in metadata:
            gps = metadata['gps']
            tags['GPSLatitude'] = gps['latitude']
            tags['GPSLatitudeRef'] = gps['latitude']
            tags['GPSLongitude'] = gps['longitude']
            tags['GPSLongitudeRef'] = gps['longitude']
            tags['GPSAltitude'] = gps['altitude']

        # People
        if 'people' in metadata:
            people = metadata['people']
            tags['XMP:Subject'] = people
            tags['IPTC:Keywords'] = people
            tags['XMP:PersonInImage'] = people

        # URL
        if 'url' in metadata:
            tags['ExifIFD:UserComment'] = metadata['url']

        if not tags:
            return

        if dry_run:
            logger.info(f"[DRY RUN] Writing tags to {file_path}: {tags}")
        else:
            try:
                with exiftool.ExifToolHelper() as et:
                    et.set_tags(
                        [str(file_path)],
                        tags=tags,
                        params=["-overwrite_original"]
                    )
                logger.debug(f"Updated metadata for {file_path}")
            except Exception as e:
                logger.error(f"ExifTool failed for {file_path}: {e}")

class FileOrganizer:
    """Handles file organization, naming, and moving/copying."""
    
    def __init__(self, dest_root: Path, dry_run: bool = False):
        self.dest_root = dest_root
        self.dry_run = dry_run

    def get_target_path(self, timestamp: float, original_filename: str) -> Path:
        """Determines the target path based on timestamp."""
        dt = datetime.fromtimestamp(timestamp)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        
        # Handle Motion Photos: Rename .mp to .mp4
        path = Path(original_filename)
        if path.suffix.lower() == '.mp':
            filename = path.with_suffix('.mp4').name
        else:
            filename = original_filename
            
        return self.dest_root / year / month / filename

    def resolve_collision(self, target_path: Path) -> Path:
        """Resolves filename collisions by appending a counter."""
        if not target_path.exists():
            return target_path
        
        parent = target_path.parent
        stem = target_path.stem
        suffix = target_path.suffix
        counter = 1
        
        while True:
            new_name = f"{stem}_{counter}{suffix}"
            new_path = parent / new_name
            if not new_path.exists():
                return new_path
            counter += 1

    def copy_file(self, src: Path, dest: Path, timestamp: float):
        """Copies file to destination and updates modification time."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Copy {src} -> {dest}")
            return

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            
            # Update mtime
            os.utime(dest, (timestamp, timestamp))
            logger.info(f"Copied {src.name} to {dest}")
        except Exception as e:
            logger.error(f"Failed to copy {src} to {dest}: {e}")

class MediaProcessor:
    """Main processor class."""
    
    SUPPORTED_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.heic', '.gif', '.tif', '.tiff', '.bmp', '.webp',
        '.mp4', '.mov', '.avi', '.wmv', '.3gp', '.m4v', '.mkv', '.mp'
    }

    def __init__(self, source_dir: Path, dest_dir: Path, dry_run: bool = False):
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.dry_run = dry_run
        self.metadata_handler = MetadataHandler()
        self.file_organizer = FileOrganizer(dest_dir, dry_run)

    def is_valid_timestamp(self, timestamp: float) -> bool:
        """Checks if the timestamp is valid (Year >= 1999)."""
        try:
            dt = datetime.fromtimestamp(timestamp)
            return dt.year >= 1999
        except (ValueError, OSError, OverflowError):
            return False

    def find_json_sidecar(self, media_path: Path) -> Optional[Path]:
        """
        Attempts to find the JSON sidecar for a media file.
        Handles:
        - filename.ext -> filename.ext.json
        - filename.ext -> filename.json
        - filename(n).ext -> filename.ext(n).json
        - filename.ext -> filename.ext.supplemental-metadata.json
        - filename-edited.ext -> filename.ext.json
        """
        candidates = []
        
        # 1. Standard: filename.ext.json
        candidates.append(media_path.with_name(media_path.name + ".json"))
        
        # 2. Legacy/Simple: filename.json
        candidates.append(media_path.with_suffix(".json"))
        
        # 3. Supplemental Metadata
        candidates.append(media_path.with_name(media_path.name + ".supplemental-metadata.json"))
        candidates.append(media_path.with_suffix(".supplemental-metadata.json"))

        # 4. Handle Duplicates: filename(n).ext -> filename.ext(n).json
        # Regex to find (n) at the end of the stem
        import re
        match = re.search(r'(\(\d+\))$', media_path.stem)
        if match:
            duplicate_suffix = match.group(1) # e.g. "(1)"
            base_stem = media_path.stem[:-len(duplicate_suffix)] # e.g. "IMG_1234"
            
            # Construct: IMG_1234.jpg(1).json
            # We need the original extension. media_path.suffix includes the dot.
            # base_stem + suffix + duplicate_suffix + .json
            potential_name = f"{base_stem}{media_path.suffix}{duplicate_suffix}.json"
            candidates.append(media_path.with_name(potential_name))
            
            # Also try: filename(n).json (less common but possible)
            candidates.append(media_path.with_name(media_path.stem + ".json"))

        # 5. Handle Edited: filename-edited.ext -> filename.ext.json
        if "-edited" in media_path.stem:
            original_stem = media_path.stem.replace("-edited", "")
            # Try filename.ext.json (using original stem)
            original_name = original_stem + media_path.suffix
            candidates.append(media_path.with_name(original_name + ".json"))
            # Try filename.json (using original stem)
            candidates.append(media_path.with_name(original_stem + ".json"))

        # Check all candidates
        for candidate in candidates:
            if candidate.exists():
                return candidate
        
        return None

    def process(self):
        """Scans and processes all files."""
        count = 0
        for root, _, files in os.walk(self.source_dir):
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                    continue
                
                count += 1
                self.process_file(file_path)
        
        logger.info(f"Processed {count} files.")

    def process_file(self, file_path: Path):
        """Processes a single media file."""
        logger.debug(f"Processing {file_path}")
        
        # 1. Find JSON
        json_path = self.find_json_sidecar(file_path)
        metadata = {}
        if json_path:
            metadata = self.metadata_handler.parse_json_sidecar(json_path)
        else:
            logger.warning(f"No JSON found for {file_path}")
        
        # 2. Determine Timestamp
        # Priority 1: Media Metadata (EXIF/IPTC/XMP)
        media_metadata = self.metadata_handler.read_metadata(file_path)
        media_timestamp = media_metadata.get('timestamp')
        
        timestamp = None
        
        if media_timestamp and self.is_valid_timestamp(media_timestamp):
            timestamp = media_timestamp
            logger.debug(f"Using Media Metadata timestamp: {timestamp}")
        
        # Priority 2: JSON Metadata
        if not timestamp:
            json_timestamp = metadata.get('timestamp')
            if json_timestamp and self.is_valid_timestamp(json_timestamp):
                timestamp = json_timestamp
                logger.debug(f"Using JSON timestamp: {timestamp}")
        
        # Fallback: File Modification Time
        if not timestamp:
            timestamp = file_path.stat().st_mtime
            logger.debug(f"Using File Mtime: {timestamp}")
        
        # 3. Determine Target Path
        target_path = self.file_organizer.get_target_path(timestamp, file_path.name)
        final_path = self.file_organizer.resolve_collision(target_path)
        
        # 4. Copy File
        self.file_organizer.copy_file(file_path, final_path, timestamp)
        
        # 5. Write Metadata (to the destination file)
        if not self.dry_run and json_path:
            # Don't overwrite valid media timestamp with JSON timestamp
            # If we found a valid media timestamp, we should preserve it.
            if media_timestamp and self.is_valid_timestamp(media_timestamp):
                if 'timestamp' in metadata:
                    del metadata['timestamp']
                    
            self.metadata_handler.write_metadata(final_path, metadata)

def main():
    parser = argparse.ArgumentParser(description="Organize Google Takeout Photos")
    parser.add_argument("source", type=Path, help="Source directory (Takeout/Google Photos)")
    parser.add_argument("dest", type=Path, help="Destination directory")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually move/write files")
    
    args = parser.parse_args()
    
    if not args.source.exists():
        logger.error("Source directory does not exist")
        sys.exit(1)
        
    processor = MediaProcessor(args.source, args.dest, args.dry_run)
    processor.process()

if __name__ == "__main__":
    main()
