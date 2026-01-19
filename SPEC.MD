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
-   **Dry Run**: The tool supports a `--dry-run` flag to simulate operations without making changes.

### Output
-   **Destination Directory**: A clean directory where organized files will be placed.
-   **Structure**: `DESTINATION/YYYY/MM/filename.ext`.

### Supported File Types
The tool will support the following extensions (case-insensitive):
-   **Images**: JPG, JPEG, PNG, HEIC, GIF, TIF, TIFF, BMP, WEBP
-   **Videos**: MP4, MOV, AVI, WMV, 3GP, M4V, MKV
-   **Other**: MP (Motion Photo) - *Note: `.mp` files will be renamed to `.mp4`.*

*Note: Files without extensions (e.g., `IMG_1234`) should be inspected (e.g., using `python-magic` or `file` command logic) or skipped with a warning.*

### Dependencies
-   **Python 3.x**
-   **ExifTool**: The script requires `exiftool` to be installed on the system.
-   **PyExifTool**: A Python wrapper library to interact with ExifTool efficiently.
-   **Standard Libraries**: `pathlib`, `json`, `datetime`, `shutil`, `logging`.

## Detailed Logic

### 1. Scanning & Matching
The script will iterate through all files in the source directory. For each file:
1.  Check if it is a supported media file.
2.  Attempt to locate its corresponding JSON sidecar file.
    *   **Matching Rules**:
        *   `filename.ext` -> `filename.ext.json` (Common)
        *   `filename.ext` -> `filename.json` (Legacy/Some formats)
        *   `filename.ext` -> `filename.ext.supplemental-metadata.json` (Supplemental Metadata)
        *   `filename.ext` -> `filename.supplemental-metadata.json` (Supplemental Metadata)
        *   `filename(n).ext` -> `filename.ext(n).json` (Duplicate handling quirk)
        *   `filename(n).ext` -> `filename(n).json` (Duplicate handling fallback)
        *   `filename-edited.ext` -> `filename.ext.json` or `filename.json` (Edited versions often share original JSON)
    *   *If no JSON is found, log a warning and use existing file metadata if available.*

### 2. Metadata Extraction (from JSON)
From the JSON file, extract:
-   **Timestamp**: `photoTakenTime.timestamp` (Unix timestamp).
-   **GPS**: `geoData.latitude`, `geoData.longitude`, `geoData.altitude`.
-   **Timestamp**: `photoTakenTime.timestamp` (Unix timestamp).
-   **GPS**: `geoData.latitude`, `geoData.longitude`, `geoData.altitude`.
-   **People**: `people` (List of names).
-   **URL**: `url` (Google Photos link).

### 3. Metadata Writing (to Media)
Using `PyExifTool`, update the media file **only if the target metadata is missing** (or as configured).
*   **Date/Time**:
    *   Write `DateTimeOriginal`, `CreateDate`, `ModifyDate`.
    *   *Priority*: Use JSON timestamp if media metadata is missing or invalid (Year < 1999).
*   **GPS**:
    *   Write `GPSLatitude`, `GPSLatitudeRef`, `GPSLongitude`, `GPSLongitudeRef`, `GPSAltitude`.
*   **People**:
    *   Write `XMP:Subject`, `IPTC:Keywords`, `XMP:PersonInImage`.
*   **URL**:
    *   Write `ExifIFD:UserComment`.

*Command Construction*:
The tool uses `PyExifTool`'s `set_tags` method to apply changes in batch, which is more efficient and safer than constructing raw command strings.

### 4. File Organization
1.  Determine the **Target Date**:
    *   **Priority 1**: Media Metadata (EXIF/IPTC/XMP).
        *   *Condition*: Must be a valid timestamp (Year >= 1999).
    *   **Priority 2**: JSON `photoTakenTime`.
        *   *Condition*: Must be a valid timestamp (Year >= 1999).
    *   **Fallback**: Use file modification time (`mtime`).
2.  Construct **Target Path**: `DESTINATION/YYYY/MM/filename.ext`.
3.  **Handle Duplicates**:
    *   If `filename.ext` exists in the destination:
        *   Check if files are identical (hash or size+mtime). If so, skip.
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
-   Option to move instead of copy (to save space).
