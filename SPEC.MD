# Design Specification: Google Takeout Media Processor

## Overview
This tool is a Python script designed to process a Google Takeout export of Google Photos. Its primary goals are to organize media files into a chronological directory structure and to restore missing metadata (EXIF/IPTC/XMP) using the accompanying JSON sidecar files provided by Google.

## Goals
1.  **Organize**: Move media files into a `YEAR/MONTH` directory structure based on their creation date.
2.  **Restore Metadata**: Read metadata from JSON sidecars and write it to the media files if the metadata is missing in the file itself.
3.  **Fix Dates**: Update the file modification time to match the actual photo taken time.

## Scope & Requirements

### Input
-   **Source Directory**: A directory containing the extracted Google Takeout data (e.g., `.../Takeout/Google Photos`).
-   **Recursive Scan**: The tool must scan the source directory and all subdirectories.
-   **Parallel Processing**: The tool supports parallel processing of files using threads. The number of workers can be configured via the `--workers` argument (default: 4).
-   **Dry Run**: The tool supports a `--dry-run` flag to simulate operations without making changes.
-   **Debug Logging**: The tool supports a `--debug` flag to enable verbose logging.

### Output
-   **Destination Directory**: A clean directory where organized files will be placed.
-   **Structure**: `DESTINATION/YYYY/MM/filename.ext`.

### Supported File Types
The tool will support the following extensions (case-insensitive):
-   **Images**: JPG, JPEG, JPE, PNG, HEIC, HEIF, GIF, TIF, TIFF, BMP, WEBP
-   **Videos**: MP4, MOV, AVI, WMV, 3GP, M4V, MKV
-   **Other**: MP (Motion Photo) - *Note: `.mp` files will be renamed to `.mp4`.*

The compatibility of features for tags (EXIF, IPTC, XMP, and QuickTime) is derived from the Supported File Types at https://exiftool.org/#supported for the above file types. File types that have explicit lack of support for writability are not supported for metadata writing (for example, BMP, AVI and others listed at https://exiftool.org/#supported).

*Note: Files without extensions (e.g., `IMG_1234`) should be inspected (e.g., using `python-magic` or `file` command logic) or skipped with a warning.*

### Dependencies
-   **Python 3.x**
-   **ExifTool**: The script requires `exiftool` to be installed on the system.
-   **PyExifTool**: A Python wrapper library to interact with ExifTool efficiently.
-   **Standard Libraries**: `pathlib`, `json`, `datetime`, `shutil`, `logging`, `concurrent.futures`.

### Code Structure
The application is structured into modular classes:
-   `MediaProcessor`: Main controller that orchestrates the scanning and processing logic. Handles parallel execution.
-   `MetadataHandler`: Handles parsing JSON sidecars and reading/writing metadata using `PyExifTool`.
-   `FileOrganizer`: Handles file system operations, path resolution, and duplicate handling.

### Shell Wrapper
A shell script `organize_photos.sh` is provided to simplify execution. It automatically activates the virtual environment and runs the Python script with the provided arguments.
Usage: `./organize_photos.sh [SOURCE] [DEST] [ARGS]`

## Detailed Logic

### 1. Scanning & Batch Processing
The script uses a chunk-based pipeline for efficiency:
1.  **Scan**: Iterate through all files in the source directory to identify supported media files.
2.  **Chunk Loop**: Process files in batches (default 1000).
    *   **Batch Read**: Read metadata for the entire chunk using `exiftool` (one command if possible, though it will fallback to multiple commands if needed).
    *   **Process**: For each file in the chunk (parallelized):
        *   Find JSON sidecar.
        *   Determine target date and path.
        *   Copy file to destination.
        *   Prepare metadata write operations.
    *   **Batch Write**: Write metadata updates for the chunk using `exiftool` (one command if possible, though it will fallback to multiple commands if needed).

### 2. Matching Logic
1.  Check if it is a supported media file.
2.  Attempt to locate its corresponding JSON sidecar file.
    *   **Matching Rules**:
        *   `filename.ext` -> `filename.ext.json` (Common)
        *   `filename.ext` -> `filename.json` (Legacy/Some formats)
        *   `filename.ext` -> `filename.ext.supplemental-metadata.json` (Supplemental Metadata)
        *   `filename.ext` -> `filename.supplemental-metadata.json` (Supplemental Metadata)
        *   `filename(n).ext` -> `filename.ext(n).json` (Duplicate handling quirk)
        *   `filename(n).ext` -> `filename(n).json` (Duplicate handling fallback)
    *   *If no JSON is found, log a warning and use existing file metadata if available.*

### 2. Metadata Extraction (from JSON)
From the JSON file, extract:
-   **Timestamp**: `photoTakenTime.timestamp` (Unix timestamp).
-   **GPS**: `geoData.latitude`, `geoData.longitude`, `geoData.altitude`.
-   **People**: `people` (List of names).
-   **URL**: `url` (Google Photos link).

### 4. Metadata Reading (from Media)
Using `PyExifTool`, read the media file's metadata.
*   **Date/Time**:
    *   Use `DateTimeOriginal`, `CreateDate` (EXIF).
    *   Use `QuickTime:CreateDate` (QuickTime/Video).
    *   Use `XMP:DateCreated` (XMP).
    *   *Timezones*: QuickTime tags are written in UTC; others in local time.
    *   *Priority*: Use JSON timestamp if media metadata is missing or invalid (Year < 1999).
*   **GPS**:
    *   Use `GPSLatitude`, `GPSLatitudeRef`, `GPSLongitude`, `GPSLongitudeRef`, `GPSAltitude` (EXIF).
    *   Use `GPSCoordinates` (QuickTime).
*   **People**:
    *   Use `XMP:Subject`, `IPTC:Keywords`, `XMP:PersonInImage`.
*   **URL**:
    *   Use `ExifIFD:UserComment`, `XMP:UserComment`.

### 5. Merge Resolved Metadata
Using the extracted metadata from the JSON and the read metadata from the media file, merge the metadata.
1.  Determine the **Target Date**:
    *   **Priority 1**: Media Metadata (EXIF/IPTC/XMP).
        *   *Condition*: Must be a valid timestamp (Year >= 1999).
    *   **Priority 2**: JSON `photoTakenTime`.
        *   *Condition*: Must be a valid timestamp (Year >= 1999).
    *   **Fallback**: Use file modification time (`mtime`).
2.  Determine the **Target GPS**:
    *   **Priority 1**: Media Metadata (EXIF/IPTC/XMP).
    *   **Priority 2**: JSON `geoData`.
3.  Determine the **Target People**:
    *   **Merge and Deduplicate**: 
    *   -   Media Metadata (EXIF/IPTC/XMP).
    *   -   JSON `people`.
4.  Determine the **Target URL**:
    *   **Priority 1**: Media Metadata (EXIF/IPTC/XMP).
    *   **Priority 2**: JSON `url`.

### 6. Metadata Writing (to Media)
Using `PyExifTool`, update the media file **only if the target metadata is missing** (or as configured).
*   Using the same extraction mappings as in Step #4, write the resolved metadata from Step #5 to the destination media file.

*Command Construction*:
The tool uses `PyExifTool`'s `get_tags` method to read tags in batch, which is more efficient and safer than constructing raw command strings.

### 7. File Organization

2.  Construct **Target Path**: `DESTINATION/YYYY/MM/filename.ext`.
3.  **Handle Duplicates**:
    *   If `filename.ext` exists in the destination:
        *   Check if files are identical (resolved metadata). If so, skip.
        *   If different, rename to `filename_1.ext`, `filename_2.ext`, etc.
4.  **Copy/Move**:
    *   Copy the file to the target.
    *   Update the **File Modification Time** (`mtime`) of the destination file to the Target Date.

## Grounding
Ground the implementation details in the following projects and resources:
-   https://github.com/mattwilson1024/google-photos-exif
-   https://github.com/TheLastGimbus/GooglePhotosTakeoutHelper
-   https://github.com/alexdachin/gophix
-   https://github.com/paulmrsn/google-takeout-image-parser
-   https://github.com/nveloso/google-takeout-photos-recover
-   https://exiftool.org/

## Error Handling
-   **Corrupt Files**: Log and skip.
-   **Missing JSON**: Log and proceed with best effort (use existing EXIF).
-   **ExifTool Failures**: Log error and continue.

## Future Considerations
-   Handle albums differently than photo directories.
-   Prevent processing/overwriting existing files.
-   Option to move instead of copy (to save space).
