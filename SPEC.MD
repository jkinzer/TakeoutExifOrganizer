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

## Persistence & State Management
To support large datasets (100k+ files) and resumability, the tool uses a persistence layer to track the state of every file.
-   **PersistenceManager**: A class implementing a persistence layer for file state management.
-   Supports both in-memory and file-based persistence.
-   File-based persistence is the default, with in-memory useful for transient runs or testing.

### Database Schema
The database tracks two main entities:
1.  **Files**: Represents a source file and its processing state.
    -   `id`, `source_path`, `media_type`, `file_size`, `mtime`
    -   `status`: `NEW`, `PENDING`, `IN_PROGRESS`, `SUCCESS`, `FAILED`, `SKIPPED`
    -   `phase`: `DISCOVERY`, `METADATA_READ`, `TARGET_RESOLVED`, 'COPY', `METADATA_WRITE`
    -   `target_path`: Resolved destination path.
    -   `error_message`: Failure reason.
2.  **Metadata**: Stores metadata from different sources for a file.
    -   `file_id`
    -   `source`: `JSON`, `MEDIA`, `MERGED`
    -   `data`: JSON blob containing timestamp, gps, people, url, etc.

## Phased Processing Workflow
The processing logic is broken down into distinct, resumable phases. The tool can stop and resume at any phase.

### Phase 1: Discovery
-   **Goal**: Identify all files to be processed.
-   **Action**: Walk the source directory.
-   **Logic**:
    -   Insert new files into the DB with status `NEW`.
    -   If a file already exists in the DB, skip (unless forced re-scan).
    -   Identify `MediaType`.

### Phase 2: Metadata Extraction (Analysis)
-   **Goal**: Gather all available metadata for `NEW` files.
-   **Action**:
    -   **JSON Sidecar**: Locate and parse the JSON sidecar. Store as `source='JSON'`.
    -   **Media Metadata**: Batch read EXIF/IPTC/XMP from media files using `PyExifTool`. Store as `source='MEDIA'`.
-   **State Transition**: `NEW` -> `METADATA_READ`.

### Phase 3: Resolution (Target & Merge)
-   **Goal**: Determine where the file should go and what metadata it should have.
-   **Action**:
    -   **Merge Metadata**: Apply priority rules (Media > JSON > File) to create a `MERGED` metadata set.
    -   **Resolve Target**: Determine the destination path (`YYYY/MM/filename.ext`).
    -   **Collision Handling**: Check DB and filesystem for collisions. Resolve duplicates (e.g., `_1.jpg`).
-   **State Transition**: `METADATA_READ` -> `TARGET_RESOLVED`.

### Phase 4: Execution (Copy & Write)
-   **Goal**: Perform the physical file operations.
-   **Action**:
    -   **Copy**: Copy source file to `target_path`.
    -   **Write Metadata**: Use `PyExifTool` to write the `MERGED` metadata to the destination file.
    -   **Verify**: Check if file exists and has expected metadata (optional).
-   **State Transition**: `TARGET_RESOLVED` -> `SUCCESS` (or `FAILED`).

## Grounding
Ground the implementation details in the following projects and resources:
-   https://github.com/mattwilson1024/google-photos-exif
-   https://github.com/TheLastGimbus/GooglePhotosTakeoutHelper
-   https://github.com/alexdachin/gophix
-   https://github.com/paulmrsn/google-takeout-image-parser
-   https://github.com/nveloso/google-takeout-photos-recover
-   https://exiftool.org/

## Error Handling
-   **Granular Tracking**: Errors are recorded in the DB (`error_message`) per file.
-   **Resume**: Failed files can be retried by resetting their status.
-   **Corrupt Files**: Logged as `FAILED` with specific error.
-   **Missing JSON**: Logged, proceeds with available metadata.

## Bugs:
-  Merged timestamp is null.
-  /backup/photos/google/jeff/Takeout/Google Photos/Photos from 2010/IMG_7096.JPG was not processed because target was IMG_7096_1.JPG which did not exist.

## Future Considerations
-   **UI/TUI**: A progress bar or terminal UI showing counts per phase.
-   **Verification Phase**: A separate phase to verify checksums of copied files.
-   **Undo**: Ability to rollback operations based on DB state.
