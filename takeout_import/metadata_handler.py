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
from .media_metadata import MediaMetadata, GpsData

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

    def parse_json_sidecar(self, json_path: Path) -> MediaMetadata:
        """Parses the JSON sidecar file and extracts relevant metadata."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = MediaMetadata.from_json(data)
            return metadata
        except Exception as e:
            logger.warning(f"Failed to parse JSON {json_path}: {e}")
            return MediaMetadata()

    def read_metadata_batch(self, file_paths: list[tuple[Path, MediaType]]) -> Dict[Path, MediaMetadata]:
        """Reads metadata for multiple files in a batch."""
        if not file_paths:
            return {}

        results = {}
        
        try:
            # ExifToolHelper.get_tags accepts a list of filenames
            file_strs = [str(f.resolve()) for f, _ in file_paths]
            data_list = self._exif_tool.get_tags(file_strs, tags=MediaMetadata.READ_TAGS, params=["-n"])
            
            # Map results by SourceFile
            # ExifTool returns 'SourceFile' which matches the input path (usually absolute if input was absolute)
            # We need to be careful about matching.
            
            # Create a map of resolved path string to (Path, MediaType)
            path_map = {str(file_path.resolve()): (file_path, media_type) for file_path, media_type in file_paths}

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
                        original_path, media_type = original_path_info
                        results[original_path] = MediaMetadata.from_exif(data, media_type)
                    else:


                        logger.warning(f"Could not map ExifTool result for {source_file} to input files.")
        except Exception as e:
            logger.error(f"Batch read failed: {e}")
            pass
            
        return results

    def write_metadata_batch(self, write_ops: list[tuple[Path, MediaType, MediaMetadata]], dry_run: bool = False):
        """Writes metadata for multiple files in a batch."""
        if not write_ops:
            return

        # Filter out empty ops and prepare tags
        valid_ops = []
        for file_path, media_type, metadata in write_ops:
            tags = metadata.to_tags(media_type)
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

    def extract_metadata(self, file_path: Path) -> MediaMetadata:
        """Extracts metadata from a single file."""
        from .media_type import get_media_type
        media_type = get_media_type(file_path)
        results = self.read_metadata_batch([(file_path, media_type)])
        return results.get(file_path, MediaMetadata())
