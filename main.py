#!/usr/bin/env python3
import sys
import logging
import argparse
from pathlib import Path
from takeout_import.media_processor import MediaProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Organize Google Takeout Photos")
    parser.add_argument("source", type=Path, help="Source directory (Takeout/Google Photos)")
    parser.add_argument("dest", type=Path, help="Destination directory")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually move/write files")
    
    args = parser.parse_args()
    
    if not args.source.exists():
        logger.error("Source directory does not exist")
        sys.exit(1)

    if not args.dest.exists():
        try:
            args.dest.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created destination directory: {args.dest}")
        except Exception as e:
            logger.error(f"Could not create destination directory {args.dest}: {e}")
            sys.exit(1)
    elif not args.dest.is_dir():
        logger.error(f"Destination {args.dest} exists but is not a directory")
        sys.exit(1)
        
    processor = MediaProcessor(args.source, args.dest, args.dry_run)
    processor.process()

if __name__ == "__main__":
    main()
