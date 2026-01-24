from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from .media_type import MediaType

@dataclass
class GpsData:
    latitude: float
    longitude: float
    altitude: Optional[float] = None

@dataclass
class MediaMetadata:
    timestamp: Optional[float] = None
    people: Optional[List[str]] = None
    gps: Optional[GpsData] = None
    url: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.gps, dict):
            self.gps = GpsData(**self.gps)

    READ_TAGS = [
        'DateTimeOriginal', 'CreateDate', 'ModifyDate', 'DateCreated', 
        'GPSLatitude', 'GPSLongitude', 'GPSAltitude', 'GPSCoordinates',
        'GPSLatitudeRef', 'GPSLongitudeRef',
        'XMP:Subject', 'XMP:PersonInImage', 'IPTC:Keywords',
        'ExifIFD:UserComment', 'XMP:UserComment'
    ]

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'MediaMetadata':
        """Parses a Google Takeout JSON dictionary into a MediaMetadata object."""
        metadata = cls()
        
        ts = cls._extract_timestamp_from_json(data)
        if ts is not None:
            metadata.timestamp = ts
            
        gps = cls._extract_gps_from_json(data)
        if gps:
            metadata.gps = gps
            
        url = cls._extract_url_from_json(data)
        if url:
            metadata.url = url
            
        people = cls._extract_people_from_json(data)
        if people is not None:
            metadata.people = people
            
        return metadata

    @staticmethod
    def _extract_timestamp_from_json(data: Dict[str, Any]) -> Optional[int]:
        if 'photoTakenTime' in data and 'timestamp' in data['photoTakenTime']:
            return int(data['photoTakenTime']['timestamp'])
        return None

    @staticmethod
    def _extract_gps_from_json(data: Dict[str, Any]) -> Optional[GpsData]:
        if 'geoData' in data:
            geo = data['geoData']
            if 'latitude' in geo and 'longitude' in geo and 'altitude' in geo:
                lat = geo['latitude']
                lon = geo['longitude']
                # Google Photos often exports 0.0, 0.0 for missing location data
                if not (lat == 0.0 and lon == 0.0):
                    return GpsData(
                        latitude=lat,
                        longitude=lon,
                        altitude=geo['altitude'] if geo['altitude'] != 0.0 else None
                    )
        return None

    @staticmethod
    def _extract_url_from_json(data: Dict[str, Any]) -> Optional[str]:
        if 'url' in data and data['url']:
            return data['url']
        return None

    @staticmethod
    def _extract_people_from_json(data: Dict[str, Any]) -> Optional[List[str]]:
        if 'people' in data and isinstance(data['people'], list):
            people_names = []
            for person in data['people']:
                if 'name' in person and person['name']:
                    people_names.append(person['name'])
            return people_names
        return None

    @classmethod
    def from_exif(cls, data: Dict[str, Any], media_type: MediaType) -> 'MediaMetadata':
        """Parses raw ExifTool data into a MediaMetadata object."""
        metadata = cls()
        
        ts = cls._parse_date_from_exif(data, media_type)
        if ts is not None:
            metadata.timestamp = ts

        gps = cls._parse_gps_from_exif(data)
        if gps:
            metadata.gps = gps
                
        people = cls._parse_people_from_exif(data)
        if people:
            metadata.people = people

        url = cls._parse_url_from_exif(data)
        if url:
            metadata.url = url
                
        return metadata

    @staticmethod
    def _parse_date_from_exif(data: Dict[str, Any], media_type: MediaType) -> Optional[float]:
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

    @staticmethod
    def _parse_gps_from_exif(data: Dict[str, Any]) -> Optional[GpsData]:
        # Extract GPS Data
        lat = None
        lon = None
        alt = None
        lat_ref = None
        lon_ref = None
        
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
            elif k.endswith('GPSLatitudeRef'):
                lat_ref = v
            elif k.endswith('GPSLongitudeRef'):
                lon_ref = v

        if lat is not None and lon is not None:
            try:
                lat_float = float(lat)
                lon_float = float(lon)
                
                # Apply Refs if available and needed
                if lat_ref and isinstance(lat_ref, str) and lat_ref.upper().startswith('S') and lat_float > 0:
                    lat_float = -lat_float
                
                if lon_ref and isinstance(lon_ref, str) and lon_ref.upper().startswith('W') and lon_float > 0:
                    lon_float = -lon_float

                if not (lat_float == 0.0 and lon_float == 0.0):
                    alt_float = None
                    if alt is not None:
                        try:
                            alt_float = float(alt)
                        except (ValueError, TypeError):
                            pass
                    
                    return GpsData(
                        latitude=lat_float,
                        longitude=lon_float,
                        altitude=alt_float
                    )
            except (ValueError, TypeError):
                pass
        return None

    @staticmethod
    def _parse_people_from_exif(data: Dict[str, Any]) -> Optional[List[str]]:
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

    @staticmethod
    def _parse_url_from_exif(data: Dict[str, Any]) -> Optional[str]:
        # Extract URL
        for tag in ['XMP:UserComment', 'ExifIFD:UserComment']:
             # Check for exact match or suffix match
            val = None
            if tag in data:
                val = data[tag]
            elif tag.split(':')[-1] in data:
                 val = data[tag.split(':')[-1]]
            
            if val and isinstance(val, str) and val.strip():
                return val.strip()
        return None

    def to_tags(self, media_type: MediaType) -> Dict[str, Any]:
        """Generates ExifTool tags based on the metadata and media type."""
        tags = {}
        
        if self.timestamp is not None:
            tags.update(self._prepare_date_tags(media_type, self.timestamp))
            
        if self.gps is not None:
            tags.update(self._prepare_gps_tags(media_type, self.gps))
            
        if self.people is not None:
            tags.update(self._prepare_people_tags(media_type, self.people))
            
        if self.url:
            tags.update(self._prepare_url_tags(media_type, self.url))
            
        return tags

    def _prepare_date_tags(self, media_type: MediaType, ts: float) -> Dict[str, str]:
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

    def _prepare_gps_tags(self, media_type: MediaType, gps: GpsData) -> Dict[str, Any]:
        tags = {}
        if media_type.supports_qt:
            lat = gps.latitude
            lon = gps.longitude
            alt = gps.altitude if gps.altitude is not None else 0
            tags['GPSCoordinates'] = f"{lat}, {lon}, {alt}"
        elif media_type.supports_exif:
            # Standard EXIF: abs + Ref
            lat = gps.latitude
            tags['GPSLatitude'] = abs(lat)
            tags['GPSLatitudeRef'] = 'N' if lat >= 0 else 'S'
            
            lon = gps.longitude
            tags['GPSLongitude'] = abs(lon)
            tags['GPSLongitudeRef'] = 'E' if lon >= 0 else 'W'
            
            if gps.altitude is not None:
                tags['GPSAltitude'] = gps.altitude
        elif media_type.supports_xmp:
            # XMP only (e.g. GIF) - Use signed values
            tags['XMP:GPSLatitude'] = gps.latitude
            tags['XMP:GPSLongitude'] = gps.longitude
            if gps.altitude is not None:
                tags['XMP:GPSAltitude'] = gps.altitude
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

    def is_identical(self, other: 'MediaMetadata') -> bool:
        """Checks if two metadata objects are effectively identical."""
        # Check Timestamp (allow small tolerance)
        ts1 = self.timestamp
        ts2 = other.timestamp
        if (ts1 is None) != (ts2 is None):
            return False
        if ts1 is not None and ts2 is not None:
             if abs(ts1 - ts2) > 1.0:
                 return False

        # Check GPS
        gps1 = self.gps
        gps2 = other.gps
        if (gps1 is None) != (gps2 is None):
            return False
        if gps1 and gps2:
            if abs(gps1.latitude - gps2.latitude) > 0.0001:
                return False
            if abs(gps1.longitude - gps2.longitude) > 0.0001:
                return False
        
        # Check People
        p1 = self.people
        p2 = other.people
        if (p1 is None) != (p2 is None):
            return False
        if p1 is not None and p2 is not None:
            if sorted(p1) != sorted(p2):
                return False

        # Check URL
        if self.url != other.url:
            return False

        return True
