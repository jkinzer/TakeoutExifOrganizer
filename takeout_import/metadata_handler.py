import shutil
import sys
import logging
import json
import exiftool
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from exiftool.exceptions import ExifToolExecuteException
from .media_type import MediaType

logger = logging.getLogger(__name__)

class MetadataHandler:
    """Handles parsing JSON sidecars and reading/writing metadata via PyExifTool."""
    
    def __init__(self):
        # PyExifTool will look for 'exiftool' in PATH by default.
        exif_tool_path = shutil.which("exiftool")
        if exif_tool_path is None:
            logger.error("ExifTool not found in PATH. Please install ExifTool.")
            sys.exit(1)
        self._exif_tool = exiftool.ExifToolHelper()
        logger.info(f"Using ExifTool at {exif_tool_path}")
        self._exif_tool.executable = exif_tool_path
        self._exif_tool.run()

    def __del__(self):
        if self._exif_tool:
            self._exif_tool.terminate()
            self._exif_tool = None

    def parse_json_sidecar(self, json_path: Path) -> Dict[str, Any]:
        """Parses the JSON sidecar file and extracts relevant metadata."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = {}
            
            ts = self._extract_timestamp_from_json(data)
            if ts is not None:
                metadata['timestamp'] = ts
                
            gps = self._extract_gps_from_json(data)
            if gps:
                metadata['gps'] = gps
                
            url = self._extract_url_from_json(data)
            if url:
                metadata['url'] = url
                
            people = self._extract_people_from_json(data)
            if people:
                metadata['people'] = people
                
            return metadata
        except Exception as e:
            logger.warning(f"Failed to parse JSON {json_path}: {e}")
            return {}

    def _extract_timestamp_from_json(self, data: Dict[str, Any]) -> Optional[int]:
        if 'photoTakenTime' in data and 'timestamp' in data['photoTakenTime']:
            return int(data['photoTakenTime']['timestamp'])
        return None

    def _extract_gps_from_json(self, data: Dict[str, Any]) -> Optional[Dict[str, float]]:
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
                    return gps_data
        return None

    def _extract_url_from_json(self, data: Dict[str, Any]) -> Optional[str]:
        if 'url' in data and data['url']:
            return data['url']
        return None

    def _extract_people_from_json(self, data: Dict[str, Any]) -> Optional[List[str]]:
        if 'people' in data and isinstance(data['people'], list):
            people_names = []
            for person in data['people']:
                if 'name' in person and person['name']:
                    people_names.append(person['name'])
            if people_names:
                return people_names
        return None

    def _parse_exif_data(self, data: Dict[str, Any], media_type: MediaType) -> Dict[str, Any]:
        """Parses raw ExifTool data into our metadata format."""
        metadata = {}
        
        ts = self._parse_date_from_exif(data, media_type)
        if ts is not None:
            metadata['timestamp'] = ts

        gps = self._parse_gps_from_exif(data)
        if gps:
            metadata['gps'] = gps
                
        people = self._parse_people_from_exif(data)
        if people:
            metadata['people'] = people
                
        return metadata

    def _parse_date_from_exif(self, data: Dict[str, Any], media_type: MediaType) -> Optional[float]:
        # Simpler approach: Check the data dict for our preferred keys
        found_date = None
        
        # Helper to find key in data
        keys = data.keys()
        
        # Let's try to find the best date string
        for priority_tag in ['DateTimeOriginal', 'CreateDate', 'ModifyDate', 'DateCreated']:
            # Find keys that end with this tag
            matches = [k for k in keys if k.endswith(priority_tag)]
            
            exif_match = next((k for k in matches if 'EXIF' in k), None)
            if exif_match:
                found_date = data[exif_match]
                break
            
            # Prefer XMP for DateCreated
            xmp_match = next((k for k in matches if 'XMP' in k), None)
            if xmp_match:
                found_date = data[xmp_match]
                break
            
            if matches:
                found_date = data[matches[0]]
                break
        
        if found_date:
            try:
                # Take first 19 chars for standard format
                clean_str = str(found_date)[:19]
                dt = datetime.strptime(clean_str, "%Y:%m:%d %H:%M:%S")
                
                if media_type.supports_qt:
                    # Treat as UTC
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt.timestamp()
                else:
                    # Treat as Local
                    return dt.timestamp()

            except ValueError:
                pass
        return None

    def _parse_gps_from_exif(self, data: Dict[str, Any]) -> Optional[Dict[str, float]]:
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
            elif k.endswith('GPSCoordinates'):
                # QuickTime:GPSCoordinates format: "lat, lon, alt" or "lat, lon"
                try:
                    parts = [float(x.strip()) for x in v.split(',')]
                    if len(parts) >= 2:
                        lat = parts[0]
                        lon = parts[1]
                        if len(parts) >= 3:
                            alt = parts[2]
                except (ValueError, AttributeError):
                    pass

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
                    return gps_data
            except (ValueError, TypeError):
                pass
        return None

    def _parse_people_from_exif(self, data: Dict[str, Any]) -> Optional[List[str]]:
        # Extract People
        people = []
        for tag in ['XMP:Subject', 'XMP:PersonInImage', 'IPTC:Keywords']:
            # Check for exact match or suffix match if needed, but we requested specific tags
            val = None
            if tag in data:
                val = data[tag]
            elif tag.split(':')[-1] in data:
                 val = data[tag.split(':')[-1]]
            
            if val:
                if isinstance(val, list):
                    people.extend(val)
                elif isinstance(val, str):
                    people.append(val)
        
        if people:
            # Deduplicate and sort
            return sorted(list(set(people)))
        return None

    def read_metadata_batch(self, file_paths: list[tuple[Path, MediaType]]) -> Dict[Path, Dict[str, Any]]:
        """Reads metadata for multiple files in a batch."""
        if not file_paths:
            return {}

        tags_to_read = [
            'DateTimeOriginal', 'CreateDate', 'ModifyDate', 'DateCreated', 
            'GPSLatitude', 'GPSLongitude', 'GPSAltitude', 'GPSCoordinates',
            'XMP:Subject', 'XMP:PersonInImage', 'IPTC:Keywords'
        ]

        results = {}
        
        try:
            # ExifToolHelper.get_tags accepts a list of filenames
            file_strs = [str(f.resolve()) for f, _ in file_paths]
            data_list = self._exif_tool.get_tags(file_strs, tags=tags_to_read, params=["-n"])
            
            # Map results by SourceFile
            # ExifTool returns 'SourceFile' which matches the input path (usually absolute if input was absolute)
            # We need to be careful about matching.
            
            # Create a map of resolved path string to (Path, MediaType)
            path_map = {str(f.resolve()): (f, mt) for f, mt in file_paths}

            
            for data in data_list:
                source_file = data.get('SourceFile')
                if source_file:
                    # Try to find the matching Path object
                    # ExifTool usually returns the path exactly as passed, or absolute.
                    # We passed resolved absolute paths.
                    
                    # On Windows, ExifTool uses forward slashes. On Linux, it matches.
                    # Let's normalize just in case.
                    
                    original_path_info = path_map.get(source_file)
                    if not original_path_info:
                        # Try resolving/normalizing if direct match fails
                        try:
                            p = Path(source_file).resolve()
                            original_path_info = path_map.get(str(p))
                        except Exception:
                            pass
                    
                    if original_path_info:
                        original_path, mt = original_path_info
                        results[original_path] = self._parse_exif_data(data, mt)
                    else:


                        logger.warning(f"Could not map ExifTool result for {source_file} to input files.")
        except Exception as e:
            logger.error(f"Batch read failed: {e}")
            pass
            
        return results

    def _prepare_write_tags(self, file_path: Path, media_type: MediaType, json_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepares the tags dictionary for writing."""
        tags = {}
        
        # Date/Time
        if 'timestamp' in json_metadata:
            tags.update(self._prepare_date_tags(media_type, json_metadata['timestamp']))

        # GPS
        if 'gps' in json_metadata:
            tags.update(self._prepare_gps_tags(media_type, json_metadata['gps']))

        # People
        if 'people' in json_metadata:
            tags.update(self._prepare_people_tags(media_type, json_metadata['people']))

        # URL
        if 'url' in json_metadata:
            tags.update(self._prepare_url_tags(media_type, json_metadata['url']))
                
        return tags

    def _prepare_date_tags(self, media_type: MediaType, ts: int) -> Dict[str, str]:
        tags = {}
        dt_local_str = datetime.fromtimestamp(ts).strftime("%Y:%m:%d %H:%M:%S")
        dt_utc_str = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y:%m:%d %H:%M:%S")
        
        if media_type.supports_qt:
            tags['QuickTime:CreateDate'] = dt_utc_str
            tags['QuickTime:ModifyDate'] = dt_utc_str
            tags['QuickTime:TrackCreateDate'] = dt_utc_str
            tags['QuickTime:MediaCreateDate'] = dt_utc_str
        if media_type.supports_xmp:
            if media_type.supports_qt:
                tags['XMP:DateCreated'] = dt_utc_str
            else:
                tags['XMP:DateCreated'] = dt_local_str
        if media_type.supports_exif:
            tags['DateTimeOriginal'] = dt_local_str
            tags['CreateDate'] = dt_local_str
            tags['ModifyDate'] = dt_local_str
        return tags

    def _prepare_gps_tags(self, media_type: MediaType, gps: Dict[str, float]) -> Dict[str, Any]:
        tags = {}
        if media_type.supports_qt:
            if 'latitude' in gps and 'longitude' in gps:
                lat = gps['latitude']
                lon = gps['longitude']
                alt = gps.get('altitude', 0)
                tags['GPSCoordinates'] = f"{lat}, {lon}, {alt}"
        else:
            if 'latitude' in gps:
                lat = gps['latitude']
                tags['GPSLatitude'] = abs(lat)
                tags['GPSLatitudeRef'] = 'N' if lat >= 0 else 'S'
            if 'longitude' in gps:
                lon = gps['longitude']
                tags['GPSLongitude'] = abs(lon)
                tags['GPSLongitudeRef'] = 'E' if lon >= 0 else 'W'
            if 'altitude' in gps:
                tags['GPSAltitude'] = gps['altitude']
        return tags

    def _prepare_people_tags(self, media_type: MediaType, people: List[str]) -> Dict[str, Any]:
        tags = {}
        if media_type.supports_xmp:
            tags['XMP:Subject'] = people
            tags['XMP:PersonInImage'] = people
        if media_type.supports_iptc:
            tags['IPTC:Keywords'] = people
        return tags

    def _prepare_url_tags(self, media_type: MediaType, url: str) -> Dict[str, str]:
        tags = {}
        if media_type.supports_xmp:
            tags['XMP:UserComment'] = url
        if media_type.supports_exif:
            tags['ExifIFD:UserComment'] = url
        return tags

    def write_metadata_batch(self, write_ops: list[tuple[Path, MediaType, Dict[str, Any]]], dry_run: bool = False):
        """Writes metadata for multiple files in a batch."""
        if not write_ops:
            return

        # Filter out empty ops and prepare tags
        valid_ops = []
        for file_path, media_type, metadata in write_ops:
            tags = self._prepare_write_tags(file_path, media_type, metadata)
            if tags:
                valid_ops.append((file_path, tags))

        if not valid_ops:
            return

        if dry_run:
            for file_path, tags in valid_ops:
                logger.info(f"[DRY RUN] Writing tags to {file_path}: {tags}")
            return

        try:
            # PyExifTool doesn't have a direct "batch set_tags with different tags per file" method easily accessible 
            # without using execute directly or looping.
            # However, we can loop efficiently if the process is open.
            for file_path, tags in valid_ops:
                try:
                    self._exif_tool.set_tags(
                        [str(file_path)],
                        tags=tags,
                        params=["-overwrite_original"]
                    )
                    logger.debug(f"Updated metadata for {file_path}")
                except ExifToolExecuteException as e:
                    logger.error(f"ExifTool failed for {file_path}: {e.stderr}")
                except Exception as e:
                    logger.error(f"ExifTool failed for {file_path}: {e}")
        except Exception as e:
            logger.error(f"Batch write setup failed: {e}")

    def write_metadata(self, file_path: Path, media_type: MediaType, json_metadata: Dict[str, Any], dry_run: bool = False):
        """Writes metadata to the media file using PyExifTool."""
        self.write_metadata_batch([(file_path, media_type, json_metadata)], dry_run)
    
    def _timestamp_to_str(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")