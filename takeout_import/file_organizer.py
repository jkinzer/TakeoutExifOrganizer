import shutil
import os
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

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
            logger.debug(f"Copied {src.name} to {dest}")
        except Exception as e:
            logger.error(f"Failed to copy {src} to {dest}: {e}")
