import os
import logging
import glob
import re
from pathlib import Path
from typing import Optional
from datetime import datetime
from .metadata_handler import MetadataHandler
from .file_organizer import FileOrganizer

logger = logging.getLogger(__name__)

class MediaProcessor:
    """Main processor class."""
    
    SUPPORTED_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.heic', '.gif', '.tif', '.tiff', '.bmp', '.webp',
        '.mp4', '.mov', '.avi', '.wmv', '.3gp', '.m4v', '.mkv', '.mp'
    }

    JSON = '.json'

    def __init__(self, source_dir: Path, dest_dir: Path, dry_run: bool = False, max_workers: int = 4):
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.dry_run = dry_run
        self.max_workers = max_workers
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
        Uses glob to find candidates matching the filename pattern,
        and specific logic for duplicates and edited files.
        """
        # 1. Glob Search for sidecar JSON files
        # 1. Glob Search for sidecar JSON files
        stems_to_check = [media_path.stem]
        if media_path.stem.endswith("-edited"):
            stems_to_check.append(media_path.stem[:-7])

        candidates = []
        for stem in stems_to_check:
            escaped_stem = glob.escape(stem)
            candidates.extend(list(media_path.parent.glob(f"{escaped_stem}.*json")))

        if len(candidates) > 1:
            def score_candidate(path: Path):
                name = path.name
                # Priority 1: filename.ext.json
                if name == media_path.name + self.JSON:
                    return 0
                # Priority 2: filename.json
                if name == media_path.stem + self.JSON:
                    return 1
                # Priority 3: filename.ext.*.json (e.g. filename.ext.supplemental.json)
                if name.startswith(media_path.name + "."):
                    return 2
                # Priority 4: Others (e.g. filename.jp.json)
                return 3

            candidates.sort(key=score_candidate)
            logger.debug(f"Multiple candidate JSON sidecars found for {media_path}")
        
        # 2. Handle Duplicates: filename(n).ext -> filename.ext(n).json
        match = re.search(r'(\(\d+\))$', media_path.stem)
        if match:
            duplicate_suffix = match.group(1) # e.g. "(1)"
            base_stem = media_path.stem[:-len(duplicate_suffix)] # e.g. "IMG_1234"
            
            # Construct: IMG_1234.jpg(1).json
            potential_name = f"{base_stem}{media_path.suffix}{duplicate_suffix}{self.JSON}"
            candidates.append(media_path.with_name(potential_name))
            
            # Also try: filename(n).json (less common but possible)
            legacy_duplicate = media_path.with_name(media_path.stem + self.JSON)
            if legacy_duplicate not in candidates:
                candidates.append(legacy_duplicate)

        # Check all candidates
        for candidate in candidates:
            if candidate.exists():
                return candidate
        
        return None

    def process(self):
        """Scans and processes all files."""
        files_to_process = []
        skipped_files = 0
        for root, _, files in os.walk(self.source_dir):
            for file in files:
                file_path = Path(root) / file
                if not self._should_process(file_path):
                    if not file_path.suffix.lower() == self.JSON:
                        logger.info(f"Skipping {file_path}")
                        skipped_files += 1
                    continue
                files_to_process.append(file_path)
        
        total_files = len(files_to_process)
        logger.info(f"Found {total_files} files to process.")
        logger.info(f"Skipped {skipped_files} files.")
        logger.info(f"Processing with {self.max_workers} workers.")

        import concurrent.futures
        count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.process_file, fp): fp for fp in files_to_process}
            for future in concurrent.futures.as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                    count += 1
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
        
        logger.info(f"Processed {count} files.")

    def process_file(self, file_path: Path):
        """Processes a single media file."""
        logger.debug(f"Processing {file_path}")
        
        # 1. Find JSON
        json_path = self.find_json_sidecar(file_path)
        json_metadata = {}
        if json_path:
            json_metadata = self.metadata_handler.parse_json_sidecar(json_path)
        else:
            logger.warning(f"No JSON found for {file_path}")
        
        # 2. Determine Timestamp
        # Priority 1: Media Metadata (EXIF/IPTC/XMP)
        media_metadata = self.metadata_handler.read_metadata(file_path)
        media_timestamp = media_metadata.get('timestamp')
        
        timestamp = None
        
        if media_timestamp and self.is_valid_timestamp(media_timestamp):
            timestamp = media_timestamp
            logger.debug(f"Using Media Metadata timestamp: {self._timestamp_to_str(timestamp)} ({timestamp})")
        
        # Priority 2: JSON Metadata
        if not timestamp:
            json_timestamp = json_metadata.get('timestamp')
            if json_timestamp and self.is_valid_timestamp(json_timestamp):
                timestamp = json_timestamp
                logger.debug(f"Using JSON timestamp: {self._timestamp_to_str(timestamp)} ({timestamp})")
        
        # Fallback: File Modification Time
        if not timestamp:
            timestamp = file_path.stat().st_mtime
            logger.debug(f"Using File Mtime: {self._timestamp_to_str(timestamp)} ({timestamp})")
        
        # 3. Determine Target Path
        target_path = self.file_organizer.get_target_path(timestamp, file_path.name)
        final_path = self.file_organizer.resolve_collision(target_path)
        
        # 4. Copy File
        self.file_organizer.copy_file(file_path, final_path, timestamp)
        
        # 5. Write Metadata (to the destination file)
        if json_path:
            # Don't overwrite valid media timestamp with JSON timestamp
            # If we found a valid media timestamp, we should preserve it.
            if media_timestamp and self.is_valid_timestamp(media_timestamp):
                if 'timestamp' in json_metadata:
                    del json_metadata['timestamp']
            
            # Don't overwrite valid GPS with JSON GPS
            if 'gps' in media_metadata:
                if 'gps' in json_metadata:
                    logger.debug(f"Preserving existing GPS metadata for {file_path}")
                    del json_metadata['gps']
                    
            self.metadata_handler.write_metadata(final_path, json_metadata, self.dry_run)

    def _should_process(self, file_path: Path) -> bool:
        """Checks if the file should be processed."""
        return (not file_path.stem.endswith("-edited")) and (file_path.suffix.lower() in self.SUPPORTED_EXTENSIONS)
    
    def _timestamp_to_str(self, timestamp):
        """Converts timestamp to human-readable string."""
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
