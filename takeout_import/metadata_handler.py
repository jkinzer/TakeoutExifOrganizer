import shutil
import sys
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
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

    VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.wmv', '.3gp', '.m4v', '.mkv', '.mp'}

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
                    lat = geo['latitude']
                    lon = geo['longitude']
                    # Google Photos often exports 0.0, 0.0 for missing location data
                    if not (lat == 0.0 and lon == 0.0):
                        gps_data = {
                            'latitude': lat,
                            'longitude': lon
                        }
                        if geo['altitude'] != 0.0:
                            gps_data['altitude'] = geo['altitude']
                        metadata['gps'] = gps_data
            
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
                # Get common date tags and GPS tags
                # We ask for Composite:SubSecDateTimeOriginal first as it's often most accurate if available
                tags_to_read = ['DateTimeOriginal', 'CreateDate', 'ModifyDate', 'GPSLatitude', 'GPSLongitude', 'GPSAltitude']
                # Use -n to get numerical values for GPS coordinates
                result = et.get_tags(str(file_path), tags=tags_to_read, params=["-n"])
                
                if not result:
                    return {}
                
                data = result[0]
                metadata = {}
                
                # Simpler approach: Check the data dict for our preferred keys
                # We'll just look for the first one that exists and is valid
                found_date = None
                
                # Helper to find key in data (ignoring group prefix if needed, but exact match is safer)
                # Let's just check the keys present in data
                keys = data.keys()
                
                # Map our priority list to potential keys in the result
                # We need the original extension. media_path.suffix includes the dot.
                # base_stem + suffix + duplicate_suffix + .json
                
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

                # Extract GPS Data
                lat = None
                lon = None
                alt = None
                
                # Prioritize Composite tags for GPS as they are often most convenient
                # But fallback to any available
                for k, v in data.items():
                    if k.endswith('GPSLatitude'):
                        lat = v
                    elif k.endswith('GPSLongitude'):
                        lon = v
                    elif k.endswith('GPSAltitude'):
                        alt = v
                
                # If we found lat/lon, check validity
                if lat is not None and lon is not None:
                    try:
                        lat_float = float(lat)
                        lon_float = float(lon)
                        # Check for 0.0, 0.0 which is often invalid/missing
                        if not (lat_float == 0.0 and lon_float == 0.0):
                            gps_data = {
                                'latitude': lat_float,
                                'longitude': lon_float
                            }
                            if alt is not None:
                                try:
                                    gps_data['altitude'] = float(alt)
                                except (ValueError, TypeError):
                                    pass
                            metadata['gps'] = gps_data
                    except (ValueError, TypeError):
                        pass
                        
                return metadata

        except Exception as e:
            logger.warning(f"Failed to read metadata from {file_path}: {e}")
            return {}

    def write_metadata(self, file_path: Path, json_metadata: Dict[str, Any], dry_run: bool = False):
        """Writes metadata to the media file using PyExifTool."""
        if not json_metadata:
            return

        tags = {}
        is_video = file_path.suffix.lower() in self.VIDEO_EXTENSIONS
        
        # Date/Time
        if 'timestamp' in json_metadata:
            ts = json_metadata['timestamp']
            dt_local_str = datetime.fromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S")
            dt_utc_str = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y:%m:%d %H:%M:%S")
            
            if is_video:
                # Video Tags (QuickTime)
                # QuickTime tags often require UTC
                tags['QuickTime:CreateDate'] = dt_utc_str
                tags['QuickTime:ModifyDate'] = dt_utc_str
                tags['QuickTime:TrackCreateDate'] = dt_utc_str
                tags['QuickTime:MediaCreateDate'] = dt_utc_str
                # Also write XMP for broader compatibility
                tags['XMP:DateCreated'] = dt_utc_str
            else:
                # Image Tags (EXIF)
                tags['DateTimeOriginal'] = dt_local_str
                tags['CreateDate'] = dt_local_str
                tags['ModifyDate'] = dt_local_str

        # GPS
        if 'gps' in json_metadata:
            gps = json_metadata['gps']
            if is_video:
                # QuickTime:GPSCoordinates = "lat, lon, alt"
                if 'latitude' in gps and 'longitude' in gps:
                    lat = gps['latitude']
                    lon = gps['longitude']
                    alt = gps.get('altitude', 0)
                    tags['QuickTime:GPSCoordinates'] = f"{lat}, {lon}, {alt}"
            else:
                # EXIF GPS
                if 'latitude' in gps:
                    tags['GPSLatitude'] = gps['latitude']
                    tags['GPSLatitudeRef'] = gps['latitude']
                if 'longitude' in gps:
                    tags['GPSLongitude'] = gps['longitude']
                    tags['GPSLongitudeRef'] = gps['longitude']
                if 'altitude' in gps:
                    tags['GPSAltitude'] = gps['altitude']

        # People
        if 'people' in json_metadata:
            people = json_metadata['people']
            tags['XMP:Subject'] = people
            tags['XMP:PersonInImage'] = people
            if not is_video:
                tags['IPTC:Keywords'] = people

        # URL
        if 'url' in json_metadata:
            if is_video:
                tags['XMP:UserComment'] = json_metadata['url']
            else:
                tags['ExifIFD:UserComment'] = json_metadata['url']

        if not tags:
            logger.debug(f"No metadata to write for {file_path}")
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
    
    def _timestamp_to_str(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")