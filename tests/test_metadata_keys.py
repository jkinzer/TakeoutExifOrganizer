import pytest
import json


from takeout_import.metadata_handler import MetadataHandler
from takeout_import.media_type import SUPPORTED_MEDIA
from takeout_import.media_metadata import MediaMetadata
from tests.media_helper import create_dummy_image

@pytest.fixture
def handler():
    return MetadataHandler()

def test_parse_json_sidecar_new_keys(handler, tmp_path):
    json_file = tmp_path / "test.json"
    with open(json_file, 'w') as f:
        json.dump({
            "photoTakenTime": {"timestamp": "1672531200"},
            "title": "My Vacation Photo",
            "description": "A beautiful view",
            "url": "https://photos.google.com/share/...",
            "people": [
                {"name": "John Doe"},
                {"name": "Jane Smith"}
            ]
        }, f)
    
    metadata = handler.parse_json_sidecar(json_file)
    
    # Verify new keys
    assert metadata.url == "https://photos.google.com/share/..."
    assert metadata.people == ["John Doe", "Jane Smith"]
    
    # Verify Title and Description are NOT present (attributes don't exist or are None)
    assert not hasattr(metadata, 'title')
    assert not hasattr(metadata, 'description')
    
    # Verify existing keys still work
    assert metadata.timestamp == 1672531200

def test_write_metadata_new_keys(handler, tmp_path):
    metadata = MediaMetadata(
        url="https://photos.google.com/share/...",
        people=["John Doe", "Jane Smith"]
    )
    
    file_path = tmp_path / "dummy.jpg"
    create_dummy_image(file_path)
    
    mt = SUPPORTED_MEDIA.get('.jpg')
    
    handler.write_metadata_batch([(file_path, mt, metadata)])
    
    # Read back all tags
    tags_list = handler._exif_tool.get_tags([str(file_path)], tags=None, params=["-G1"])

    tags = tags_list[0]
    
    # People mappings
    # XMP:Subject
    assert 'XMP:Subject' in tags
    subject = tags['XMP:Subject']
    # It might be a list or a single string if only one item, but here we have two.
    if isinstance(subject, list):
        assert sorted(subject) == ["Jane Smith", "John Doe"]
    else:
        # Should be list
        # Order might vary?
        assert sorted(subject) == ["Jane Smith", "John Doe"]
        
    # IPTC:Keywords
    if 'IPTC:Keywords' in tags:
        keywords = tags['IPTC:Keywords']
        if isinstance(keywords, list):
            assert sorted(keywords) == ["Jane Smith", "John Doe"]
    
    # XMP:PersonInImage
    if 'XMP:PersonInImage' in tags:
        person = tags['XMP:PersonInImage']
        if isinstance(person, list):
            assert sorted(person) == ["Jane Smith", "John Doe"]
    
    # URL mapping
    # ExifIFD:UserComment might be reported as EXIF:UserComment
    if 'ExifIFD:UserComment' in tags:
        assert tags['ExifIFD:UserComment'] == "https://photos.google.com/share/..."
    else:
        assert 'EXIF:UserComment' in tags
        assert tags['EXIF:UserComment'] == "https://photos.google.com/share/..."
    
    # Verify Title and Description are NOT written
    assert 'XMP:Title' not in tags
    assert 'IPTC:Caption-Abstract' not in tags
    assert 'XMP:Description' not in tags
