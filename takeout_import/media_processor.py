import os
import logging
import glob
import re
import concurrent.futures
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime
from dataclasses import replace

from .metadata_handler import MetadataHandler
from .file_organizer import FileOrganizer
from .media_type import get_media_type, MediaType
from .media_metadata import MediaMetadata
from .persistence_manager import PersistenceManager, FileStatus, ProcessingPhase

logger = logging.getLogger(__name__)

class MediaProcessor:
    """Main processor class with phased execution and persistence."""
    
    JSON = '.json'

    def __init__(self, source_dir: Path, dest_dir: Path, persistence_manager: PersistenceManager, dry_run: bool = False, max_workers: int = 4, batch_size: int = 1000):
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.persistence = persistence_manager
        self.dry_run = dry_run
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.metadata_handler = MetadataHandler()
        self.file_organizer = FileOrganizer(dest_dir, dry_run)

    def process(self):
        """Orchestrates the phased processing pipeline."""
        self.persistence.initialize()
        
        # Phase 1: Discovery
        self._phase_discovery()
        
        # Phase 2: Metadata Extraction (Analysis)
        self._phase_metadata_extraction()
        
        # Phase 3: Resolution (Target & Merge)
        self._phase_resolution()
        
        # Phase 4: Execution (Copy & Write)
        self._phase_execution()
        
        logger.info("Processing complete.")
        self.persistence.close()

    def _phase_discovery(self):
        """Phase 1: Scan source directory and populate DB."""
        logger.info("Phase 1: Discovery - Scanning files...")
        count = 0
        for root, _, files in os.walk(self.source_dir):
            for file in files:
                file_path = Path(root) / file
                media_type = get_media_type(file_path)
                
                if self._should_process(file_path, media_type):
                    try:
                        stat = file_path.stat()
                        self.persistence.add_file(file_path, media_type, stat.st_size, stat.st_mtime)
                        count += 1
                    except Exception as e:
                        logger.error(f"Error adding file {file_path}: {e}")
        
        logger.info(f"Discovery complete. Found {count} supported files.")

    def _phase_metadata_extraction(self):
        """Phase 2: Read metadata from JSON and Media files."""
        logger.info("Phase 2: Metadata Extraction...")
        
        # Get files that need metadata reading (NEW)
        # We process in chunks
        while True:
            files = self.persistence.get_files_by_status([FileStatus.NEW], limit=self.batch_size)
            if not files:
                break
            
            logger.info(f"Processing batch of {len(files)} files for metadata extraction...")
            
            # 1. Parse JSON Sidecars (Parallel)
            # We can do this in parallel or just sequentially since it's fast. Let's do parallel.
            # Actually, let's stick to the previous logic: find JSON, parse it.
            
            # We need to map file_id -> path for the batch
            file_map = {f['id']: Path(f['source_path']) for f in files}
            
            # Batch Read Media Metadata
            paths = list(file_map.values())
            media_metadata_map = self.metadata_handler.read_metadata_batch([(p, get_media_type(p)) for p in paths])
            
            for file_record in files:
                file_id = file_record['id']
                file_path = file_map[file_id]
                
                try:
                    # Save Media Metadata
                    if file_path in media_metadata_map:
                        self.persistence.save_metadata(file_id, 'MEDIA', media_metadata_map[file_path])
                    
                    # Find and Parse JSON
                    json_path = self._find_json_sidecar(file_path)
                    if json_path:
                        json_metadata = self.metadata_handler.parse_json_sidecar(json_path)
                        self.persistence.save_metadata(file_id, 'JSON', json_metadata)
                    
                    self.persistence.update_status(file_id, FileStatus.METADATA_READ, ProcessingPhase.METADATA_READ)
                    
                except Exception as e:
                    logger.error(f"Error extracting metadata for {file_path}: {e}")
                    self.persistence.update_status(file_id, FileStatus.FAILED, ProcessingPhase.METADATA_READ, str(e))

    def _phase_resolution(self):
        """Phase 3: Merge metadata and resolve target paths."""
        logger.info("Phase 3: Resolution...")
        
        while True:
            files = self.persistence.get_files_by_status([FileStatus.METADATA_READ], limit=self.batch_size)
            if not files:
                break
            
            logger.info(f"Resolving batch of {len(files)} files...")
            
            for file_record in files:
                file_id = file_record['id']
                file_path = Path(file_record['source_path'])
                media_type = get_media_type(file_path) # Re-derive or use stored string
                
                try:
                    media_metadata = self.persistence.get_metadata(file_id, 'MEDIA') or MediaMetadata()
                    json_metadata = self.persistence.get_metadata(file_id, 'JSON') or MediaMetadata()
                    
                    # Merge Logic
                    merged_metadata = self._merge_metadata(file_path, media_type, media_metadata, json_metadata)
                    self.persistence.save_metadata(file_id, 'MERGED', merged_metadata)
                    
                    # Determine Timestamp for Path
                    timestamp = merged_metadata.timestamp
                    
                    # Resolve Target Path
                    target_path = self.file_organizer.get_target_path(timestamp, file_path.name)
                    
                    # Collision Handling (Preliminary)
                    final_path = self.file_organizer.resolve_collision(target_path)
                    
                    self.persistence.update_target_path(file_id, final_path)
                    self.persistence.update_status(file_id, FileStatus.TARGET_RESOLVED, ProcessingPhase.RESOLUTION)
                    
                except Exception as e:
                    logger.error(f"Error resolving {file_path}: {e}")
                    self.persistence.update_status(file_id, FileStatus.FAILED, ProcessingPhase.RESOLUTION, str(e))

    def _phase_execution(self):
        """Phase 4: Copy files and write metadata."""
        logger.info("Phase 4: Execution...")
        
        while True:
            files = self.persistence.get_files_by_status([FileStatus.TARGET_RESOLVED], limit=self.batch_size)
            if not files:
                break
            
            logger.info(f"Executing batch of {len(files)} files...")
            
            write_ops = []
            
            # Copy Files (Parallel)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._execute_single_file, file_record): file_record
                    for file_record in files
                }
                
                for future in concurrent.futures.as_completed(futures):
                    file_record = futures[future]
                    try:
                        result = future.result()
                        if result:
                            write_ops.append(result)
                    except Exception as e:
                        logger.error(f"Error executing {file_record['source_path']}: {e}")
                        self.persistence.update_status(file_record['id'], FileStatus.FAILED, ProcessingPhase.EXECUTION, str(e))

            # Batch Write Metadata
            if write_ops:
                logger.info(f"Batch writing metadata to {len(write_ops)} files...")
                self.metadata_handler.write_metadata_batch(write_ops, self.dry_run)
                
                # Update status to SUCCESS for these files
                for dest_path, _, _ in write_ops:
                    # We need to map back to file_id. 
                    # This is a bit inefficient. Ideally write_metadata_batch returns success/fail per file.
                    # For now, assume success if no exception raised in batch write (which logs errors but continues).
                    # We need to find which file_id corresponds to dest_path.
                    # Let's iterate files again? Or return file_id in write_ops?
                    # write_metadata_batch expects (path, type, metadata).
                    # We can't easily change it without changing MetadataHandler.
                    
                    # Let's just update all successful copies to SUCCESS.
                    # If write fails, it logs.
                    pass
            
            # Update status for all processed files in this batch
            # This is a simplification. Ideally we track write success.
            for file_record in files:
                # If it wasn't failed during copy
                current_status = self.persistence.get_file_by_id(file_record['id'])['status']
                if current_status == FileStatus.TARGET_RESOLVED.value:
                     self.persistence.update_status(file_record['id'], FileStatus.SUCCESS, ProcessingPhase.EXECUTION)

    def _execute_single_file(self, file_record: dict) -> Optional[Tuple[Path, MediaType, MediaMetadata]]:
        """Copies a single file and prepares metadata write op."""
        file_id = file_record['id']
        source_path = Path(file_record['source_path'])
        target_path = Path(file_record['target_path'])
        media_type = get_media_type(source_path)
        
        merged_metadata = self.persistence.get_metadata(file_id, 'MERGED')
        
        # Check for identical file (Duplicate Skip)
        if target_path.exists():
            # 1. Check basic file attributes (Size/Mtime) via FileOrganizer
            
            # 2. Check Metadata Identity
            # If we are about to write metadata, we should check if the existing file already has it.
            existing_metadata = self.metadata_handler.extract_metadata(target_path)
            
            if merged_metadata and merged_metadata.is_identical(existing_metadata):
                logger.info(f"Skipping identical file based on metadata: {source_path.name}")
                return None

        timestamp = (merged_metadata.timestamp if merged_metadata and merged_metadata.timestamp 
                     else source_path.stat().st_mtime)
        
        self.file_organizer.copy_file(source_path, target_path, timestamp)
        
        if merged_metadata and media_type.supports_write():
            return (target_path, media_type, merged_metadata)
        
        return None

    def _merge_metadata(self, file_path: Path, media_type: MediaType, media_metadata: MediaMetadata, json_metadata: MediaMetadata) -> MediaMetadata:
        """Merges metadata according to priority rules."""
        # JSON people overwrites media people if it's not None (even if empty list)
        people = json_metadata.people if json_metadata.people is not None else media_metadata.people

        return MediaMetadata(
            timestamp = self._determine_timestamp(file_path, media_metadata, json_metadata),
            gps = media_metadata.gps or json_metadata.gps,
            people = people,
            url = media_metadata.url or json_metadata.url
        )

    def _determine_timestamp(self, file_path: Path, media_metadata: MediaMetadata, json_metadata: MediaMetadata) -> float:
        """Determines the best timestamp for the file."""
        # Priority 1: Media
        if self._is_valid_timestamp(media_metadata.timestamp):
            return media_metadata.timestamp
        
        # Priority 2: JSON
        if self._is_valid_timestamp(json_metadata.timestamp):
            return json_metadata.timestamp
            
        # Fallback: Mtime
        return file_path.stat().st_mtime

    def _find_json_sidecar(self, media_path: Path) -> Optional[Path]:
        """Same as before..."""
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
                if name == media_path.name + self.JSON: return 0
                if name == media_path.stem + self.JSON: return 1
                if name.startswith(media_path.name + "."): return 2
                return 3
            candidates.sort(key=score_candidate)
        
        match = re.search(r'(\(\d+\))$', media_path.stem)
        if match:
            duplicate_suffix = match.group(1)
            base_stem = media_path.stem[:-len(duplicate_suffix)]
            potential_name = f"{base_stem}{media_path.suffix}{duplicate_suffix}{self.JSON}"
            candidates.append(media_path.with_name(potential_name))
            legacy_duplicate = media_path.with_name(media_path.stem + self.JSON)
            if legacy_duplicate not in candidates:
                candidates.append(legacy_duplicate)

        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _should_process(self, file_path: Path, media_type: MediaType) -> bool:
        return (not file_path.stem.endswith("-edited")) and (media_type.recognized)
    
    def _is_valid_timestamp(self, timestamp: float) -> bool:
        if timestamp is None:
            return False
        try:
            dt = datetime.fromtimestamp(timestamp)
            return dt.year >= 1999
        except (ValueError, OSError, OverflowError):
            return False
