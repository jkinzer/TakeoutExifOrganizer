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
        if shutil.which("exiftool") is None:
             logger.error("ExifTool not found in PATH. Please install ExifTool.")
             sys.exit(1)
        self._et = None

    def __enter__(self):
        self._et = exiftool.ExifToolHelper()
        self._et.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._et:
            self._et.terminate()
            self._et = None

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

    def _parse_exif_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parses raw ExifTool data into our metadata format."""
        metadata = {}
        
        # Simpler approach: Check the data dict for our preferred keys
        found_date = None
        
        # Helper to find key in data
        keys = data.keys()
        
        # Let's try to find the best date string
        for priority_tag in ['DateTimeOriginal', 'CreateDate', 'ModifyDate']:
            # Find keys that end with this tag
            matches = [k for k in keys if k.endswith(priority_tag)]
            
            exif_match = next((k for k in matches if 'EXIF' in k), None)
            if exif_match:
                found_date = data[exif_match]
                break
            
            if matches:
                found_date = data[matches[0]]
                break
        
        if found_date:
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
        
        for k, v in data.items():
            if k.endswith('GPSLatitude'):
                lat = v
            elif k.endswith('GPSLongitude'):
                lon = v
            elif k.endswith('GPSAltitude'):
                alt = v
        
        if lat is not None and lon is not None:
            try:
                lat_float = float(lat)
                lon_float = float(lon)
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

    def read_metadata_batch(self, file_paths: list[Path]) -> Dict[Path, Dict[str, Any]]:
        """Reads metadata for multiple files in a batch."""
        if not file_paths:
            return {}

        tags_to_read = ['DateTimeOriginal', 'CreateDate', 'ModifyDate', 'GPSLatitude', 'GPSLongitude', 'GPSAltitude']
        results = {}
        
        try:
            # Use existing instance or create temporary one
            if self._et:
                et = self._et
                should_close = False
            else:
                et = exiftool.ExifToolHelper()
                et.run()
                should_close = True

            try:
                # ExifToolHelper.get_tags accepts a list of filenames
                file_strs = [str(f.resolve()) for f in file_paths]
                data_list = et.get_tags(file_strs, tags=tags_to_read, params=["-n"])
                
                # Map results by SourceFile
                # ExifTool returns 'SourceFile' which matches the input path (usually absolute if input was absolute)
                # We need to be careful about matching.
                
                # Create a map of resolved path string to original Path object
                path_map = {str(f.resolve()): f for f in file_paths}
                
                for data in data_list:
                    source_file = data.get('SourceFile')
                    if source_file:
                        # Try to find the matching Path object
                        # ExifTool usually returns the path exactly as passed, or absolute.
                        # We passed resolved absolute paths.
                        
                        # On Windows, ExifTool uses forward slashes. On Linux, it matches.
                        # Let's normalize just in case.
                        
                        original_path = path_map.get(source_file)
                        if not original_path:
                            # Try resolving/normalizing if direct match fails
                            try:
                                p = Path(source_file).resolve()
                                original_path = path_map.get(str(p))
                            except Exception:
                                pass
                        
                        if original_path:
                            results[original_path] = self._parse_exif_data(data)
                        else:
                            logger.warning(f"Could not map ExifTool result for {source_file} to input files.")
                    
            finally:
                if should_close:
                    et.terminate()

        except Exception as e:
            logger.error(f"Batch read failed: {e}")
            pass
            
        return results

    def read_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Reads metadata from the media file using PyExifTool."""
        # Re-use batch logic for single file
        results = self.read_metadata_batch([file_path])
        return results.get(file_path, {})

    def _prepare_write_tags(self, file_path: Path, json_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepares the tags dictionary for writing."""
        tags = {}
        is_video = file_path.suffix.lower() in self.VIDEO_EXTENSIONS
        
        # Date/Time
        if 'timestamp' in json_metadata:
            ts = json_metadata['timestamp']
            dt_local_str = datetime.fromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S")
            dt_utc_str = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y:%m:%d %H:%M:%S")
            
            if is_video:
                tags['QuickTime:CreateDate'] = dt_utc_str
                tags['QuickTime:ModifyDate'] = dt_utc_str
                tags['QuickTime:TrackCreateDate'] = dt_utc_str
                tags['QuickTime:MediaCreateDate'] = dt_utc_str
                tags['XMP:DateCreated'] = dt_utc_str
            else:
                tags['DateTimeOriginal'] = dt_local_str
                tags['CreateDate'] = dt_local_str
                tags['ModifyDate'] = dt_local_str

        # GPS
        if 'gps' in json_metadata:
            gps = json_metadata['gps']
            if is_video:
                if 'latitude' in gps and 'longitude' in gps:
                    lat = gps['latitude']
                    lon = gps['longitude']
                    alt = gps.get('altitude', 0)
                    tags['QuickTime:GPSCoordinates'] = f"{lat}, {lon}, {alt}"
            else:
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
                
        return tags

    def write_metadata_batch(self, write_ops: list[tuple[Path, Dict[str, Any]]], dry_run: bool = False):
        """Writes metadata for multiple files in a batch."""
        if not write_ops:
            return

        # Filter out empty ops and prepare tags
        valid_ops = []
        for file_path, metadata in write_ops:
            if not metadata:
                continue
            tags = self._prepare_write_tags(file_path, metadata)
            if tags:
                valid_ops.append((file_path, tags))

        if not valid_ops:
            return

        if dry_run:
            for file_path, tags in valid_ops:
                logger.info(f"[DRY RUN] Writing tags to {file_path}: {tags}")
            return

        try:
            if self._et:
                et = self._et
                should_close = False
            else:
                et = exiftool.ExifToolHelper()
                et.run()
                should_close = True
            
            try:
                # PyExifTool doesn't have a direct "batch set_tags with different tags per file" method easily accessible 
                # without using execute directly or looping.
                # However, we can loop efficiently if the process is open.
                for file_path, tags in valid_ops:
                    try:
                        et.set_tags(
                            [str(file_path)],
                            tags=tags,
                            params=["-overwrite_original"]
                        )
                        logger.debug(f"Updated metadata for {file_path}")
                    except Exception as e:
                        error_msg = f"ExifTool failed for {file_path}: {e}"
                        if hasattr(e, 'stderr') and e.stderr:
                            error_msg += f"\nStderr: {e.stderr}"
                        logger.error(error_msg)
                        
            finally:
                if should_close:
                    et.terminate()
                    
        except Exception as e:
             logger.error(f"Batch write setup failed: {e}")

    def write_metadata(self, file_path: Path, json_metadata: Dict[str, Any], dry_run: bool = False):
        """Writes metadata to the media file using PyExifTool."""
        self.write_metadata_batch([(file_path, json_metadata)], dry_run)
    
    def _timestamp_to_str(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")