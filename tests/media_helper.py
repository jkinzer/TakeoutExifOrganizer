import cv2
import numpy as np
from PIL import Image
from pathlib import Path

def create_dummy_image(path: Path):
    """Creates a valid JPEG image using Pillow."""
    img = Image.new('RGB', (100, 100), color = 'red')
    img.save(path, 'JPEG')

def create_dummy_video(path: Path):
    """Creates a valid MP4 video using OpenCV."""
    height, width = 64, 64
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    # Use a safe temporary path for video creation if needed, but direct write should be fine
    out = cv2.VideoWriter(str(path), fourcc, 10.0, (width, height))
    
    # Create a few frames
    for _ in range(5):
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        frame[:] = (255, 0, 0) # Blue
        out.write(frame)
    
    out.release()
