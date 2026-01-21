import cv2
import numpy as np
from PIL import Image
from pathlib import Path
import logging
import pillow_heif

pillow_heif.register_heif_opener()

logger = logging.getLogger(__name__)

def create_dummy_image(path: Path):
    """Creates a valid image using Pillow based on extension."""
    ext = path.suffix.lower()
    
    # Map extension to Pillow format
    format_map = {
        '.jpg': 'JPEG',
        '.jpeg': 'JPEG',
        '.jpe': 'JPEG',
        '.png': 'PNG',
        '.tif': 'TIFF',
        '.tiff': 'TIFF',
        '.webp': 'WEBP',
        '.gif': 'GIF',
        '.bmp': 'BMP',
        '.heic': 'HEIF',
        '.heif': 'HEIF',
    }
    
    fmt = format_map.get(ext)
    
    if fmt:
        img = Image.new('RGB', (100, 100), color = 'red')
        img.save(path, fmt)
    else:
        # Default to JPEG for unknown
        img = Image.new('RGB', (100, 100), color = 'red')
        img.save(path, 'JPEG')

def create_dummy_video(path: Path):
    """Creates a valid video using OpenCV based on extension."""
    ext = path.suffix.lower()
    height, width = 64, 64
    
    # OpenCV FourCC codes: https://softron.zendesk.com/hc/en-us/articles/207695697-List-of-FourCC-codes-for-Video-Codecs
    fourcc_map = {
        '.mp4': 'mp4v',
        '.mov': 'mp4v',
        '.m4v': 'mp4v',
        '.3gp': 'mp4v',
        '.avi': 'MJPG',
        '.mkv': 'mp4v',
        '.wmv': 'mp4v',
        '.mp': 'mp4v',
    }
    
    codec = fourcc_map.get(ext, 'mp4v')
    
    temp_path = None
    if ext == '.mp':
        temp_path = path.with_suffix('.mp4')
        write_path = str(temp_path)
    else:
        write_path = str(path)
    
    try:
        fourcc = cv2.VideoWriter_fourcc(*codec)
        out = cv2.VideoWriter(write_path, fourcc, 10.0, (width, height))
        
        if not out.isOpened():
            # Try fallback codec
            logger.warning(f"Failed to open video writer for {ext} with {codec}, trying MJPG")
            fourcc = cv2.VideoWriter_fourcc(*'MJPG')
            out = cv2.VideoWriter(write_path, fourcc, 10.0, (width, height))
            
        if not out.isOpened():
             raise RuntimeError(f"Could not open VideoWriter for {path}")

        # Create a few frames
        for _ in range(5):
            frame = np.zeros((height, width, 3), dtype=np.uint8)
            frame[:] = (255, 0, 0) # Blue
            out.write(frame)
        
        out.release()

        if temp_path and temp_path.exists():
            if path.exists():
                path.unlink()
            temp_path.rename(path)
    except Exception as e:
        logger.error(f"Failed to create video {path}: {e}")
        # Create a 0-byte file so tests don't crash on 'not found', but they might fail on 'invalid'
        path.touch()

def create_dummy_media(path: Path):
    """Creates a valid media file (image or video) based on extension."""
    ext = path.suffix.lower()
    video_extensions = {'.mp4', '.mov', '.m4v', '.3gp', '.avi', '.mkv', '.wmv', '.mp'}
    
    if ext in video_extensions:
        create_dummy_video(path)
    else:
        create_dummy_image(path)
