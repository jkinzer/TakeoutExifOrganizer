import shutil
import sys
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import exiftool

logger = logging.getLogger(__name__)

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
