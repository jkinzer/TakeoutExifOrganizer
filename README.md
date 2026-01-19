# Google Takeout Exif Import

A Python tool to organize media files from a Google Takeout export and restore missing metadata (EXIF/IPTC/XMP) using the accompanying JSON sidecar files.

## Features

-   **Organize**: Moves files into a `YEAR/MONTH` directory structure based on the photo taken time.
-   **Restore Metadata**: Reads timestamps, GPS coordinates, and descriptions from Google's JSON files and writes them to the media files using `exiftool`.
-   **Duplicate Handling**: Safely handles duplicate filenames by renaming them (e.g., `image_1.jpg`).
-   **Smart Matching**: Handles various Google Takeout naming quirks (e.g., `file(1).jpg`, `file-edited.jpg`).

## Requirements

-   **Python 3.6+**
-   **ExifTool**: The underlying engine for metadata operations.
-   **PyExifTool**: Python wrapper for ExifTool.

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

3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Installation

1.  Clone this repository.
2.  Install the Python dependencies:
    ```bash
    pip install -r requirements.txt
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

### Example

```bash
# Dry run to check what will happen
python3 organize_photos.py ~/Downloads/Takeout/Google\ Photos ~/Pictures/Organized --dry-run

# Actual execution
python3 organize_photos.py ~/Downloads/Takeout/Google\ Photos ~/Pictures/Organized
```

## Testing

Run the unit tests to verify logic:

```bash
python3 -m unittest tests/test_processor.py
```
