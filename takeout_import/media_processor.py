import os
import logging
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
        files_to_process = []
        for root, _, files in os.walk(self.source_dir):
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
                    continue
                files_to_process.append(file_path)
        
        total_files = len(files_to_process)
        logger.info(f"Found {total_files} files to process. Using {self.max_workers} workers.")

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
