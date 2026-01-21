# Google Takeout Exif Import

A Python tool to organize media files from a Google Takeout export and restore missing metadata (EXIF/IPTC/XMP) using the accompanying JSON sidecar files.

## Features

-   **Organize**: Moves files into a `YEAR/MONTH` directory structure based on the photo taken time.
-   **Restore Metadata**: Reads timestamps, GPS coordinates, and descriptions from Google's JSON files and writes them to the media files using `exiftool`.
-   **Duplicate Handling**: Safely handles duplicate filenames by renaming them (e.g., `image_1.jpg`).
-   **Smart Matching**: Handles various Google Takeout naming quirks (e.g., `file(1).jpg`, `file-edited.jpg`).

## Requirements

-   **Python 3.8+**
-   **ExifTool**: The underlying engine for metadata operations.
-   **PyExifTool**: Python wrapper for ExifTool.
-   **Pillow**: Python Imaging Library for image processing.
-   **pillow-heif**: HEIF support for Pillow.
-   **opencv-python-headless**: Computer Vision library for video processing.

## Setup

1.  **Install ExifTool**:
    -   **Linux (Debian/Ubuntu)**: `sudo apt install libimage-exiftool-perl`
    -   **Fedora**: `sudo dnf install perl-Image-ExifTool`
    -   **macOS**: `brew install exiftool`
    -   **Windows**: Download from [exiftool.org](https://exiftool.org/)

2.  **Create and Activate Virtual Environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the Package (Editable Mode)**:
    ```bash
    pip install -e .
    ```

## Installation

1.  Clone this repository.
2.  **Install the Package (Editable Mode)**:
    ```bash
    pip install -e .
    ```
3.  Ensure `exiftool` is installed and in your system PATH.

## Usage

### Using the Shell Script (Recommended)
The included shell script handles virtual environment activation for you.
```bash
./organize_photos.sh /path/to/takeout/source /path/to/destination
```

### Manual Usage
```bash
source venv/bin/activate
python3 main.py /path/to/takeout/source /path/to/destination
```

### Options
-   `--dry-run`: Simulate operations without moving files or writing metadata.
-   `--debug`: Enable debug logging for more detailed output.
-   `--workers`: Number of worker threads to use for processing (default: 4).
-   `--batch-size`: Number of files to process in a single ExifTool batch (default: 1000). Useful for large datasets.

### Example

```bash
# Dry run to check what will happen
./organize_photos.sh ~/Downloads/Takeout/Google\ Photos ~/Pictures/Organized --dry-run

# Actual execution
./organize_photos.sh ~/Downloads/Takeout/Google\ Photos ~/Pictures/Organized
```

## Testing

Run the unit tests to verify logic:

```bash
python3 -m unittest discover tests
```
