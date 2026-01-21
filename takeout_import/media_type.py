from pathlib import Path

class MediaType:

    def __init__(self, extensions: set[str], supports_exif: bool = False, supports_iptc: bool = False, supports_xmp: bool = False, supports_qt: bool = False, recognized: bool = True):
        self.extensions = extensions
        self.supports_exif = supports_exif
        self.supports_iptc = supports_iptc
        self.supports_xmp = supports_xmp
        self.supports_qt = supports_qt
        self.recognized = recognized

    def supports_write(self) -> bool:
        return self.supports_exif or self.supports_iptc or self.supports_xmp or self.supports_qt

UNKNOWN = MediaType(extensions=set(), recognized=False)

SUPPORTED_MEDIA: dict[str, MediaType] = {}

for media_type in [
    MediaType(
        {'.jpg', '.jpeg', '.jpe', '.png', '.tif', '.tiff'},
        supports_exif=True,
        supports_iptc=True,
        supports_xmp=True
    ),
    MediaType(
        {'.heic', '.heif', '.webp'},
        supports_exif=True,
        supports_xmp=True
    ),
    MediaType(
        {'.mp4', '.mov', '.m4v', '.3gp', '.mp'},
        supports_xmp=True,
        supports_qt=True
    ),
    MediaType(
        {'.gif'},
        supports_xmp=True
    ),
    MediaType(
        {'.bmp', '.avi', '.wmv', '.mkv'}
    )
]:
    for ext in media_type.extensions:
        SUPPORTED_MEDIA[ext] = media_type

def get_media_type(file_path: Path) -> MediaType:
    media_type: MediaType = SUPPORTED_MEDIA.get(file_path.suffix.lower())
    if media_type is None:
        return UNKNOWN
    return media_type