"""
tests/test_upload.py
====================
Unit tests for file upload handling and security sanitization.

Tests save_uploaded_file with valid files, path traversal attempts,
invalid extensions, empty content, oversize content, and intra-second collisions.
Pure Python unit tests; does NOT require Apache Spark.
"""

from pathlib import Path
import sys
import unittest.mock as mock

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import UPLOAD_DIR  # noqa: E402
from src.upload import UploadError, save_uploaded_file  # noqa: E402


def test_save_uploaded_file_valid(tmp_path, monkeypatch):
    """Verify that a valid CSV file is saved with timestamped name and matching content."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)

    sample_content = b"date,location,temperature\n2024-01-01,Mumbai,25.5\n"
    saved_path = save_uploaded_file(sample_content, "mumbai_weather.csv")

    assert saved_path.exists()
    assert saved_path.parent == tmp_path
    assert saved_path.suffix == ".csv"
    assert "mumbai_weather" in saved_path.stem
    assert saved_path.read_bytes() == sample_content


@pytest.mark.parametrize("malicious_name", [
    "../../evil.csv",
    "..\\..\\evil.csv",
    "/etc/shadow.csv",
    "D:\\malicious\\payload.csv",
    "subdir/another.csv",
])
def test_save_uploaded_file_prevents_path_traversal(malicious_name, tmp_path, monkeypatch):
    """Ensure directory traversal characters are stripped and the file stays in UPLOAD_DIR."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)

    content = b"header1,header2\nval1,val2\n"
    saved_path = save_uploaded_file(content, malicious_name)

    assert saved_path.parent == tmp_path
    assert saved_path.exists()
    assert not saved_path.name.startswith("..")
    assert "/" not in saved_path.name
    assert "\\" not in saved_path.name


@pytest.mark.parametrize("invalid_name", [
    "data.xlsx",
    "malicious.exe",
    "notes.txt",
    "script.py",
    "archive.tar.gz",
    "dataset",
])
def test_save_uploaded_file_rejects_invalid_extension(invalid_name, tmp_path, monkeypatch):
    """Ensure unsupported file extensions raise UploadError."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)

    with pytest.raises(UploadError) as exc_info:
        save_uploaded_file(b"some,data\n1,2\n", invalid_name)

    assert "Unsupported file extension" in str(exc_info.value) or "Allowed extensions" in str(exc_info.value)


def test_save_uploaded_file_rejects_empty_content(tmp_path, monkeypatch):
    """Ensure 0-byte uploads raise UploadError."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)

    with pytest.raises(UploadError, match="Uploaded file is empty"):
        save_uploaded_file(b"", "empty.csv")


def test_save_uploaded_file_rejects_oversize_content(tmp_path, monkeypatch):
    """Ensure uploads exceeding MAX_UPLOAD_MB raise UploadError."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)
    # Mock MAX_UPLOAD_MB to 1 MB for testing without allocating 200 MB
    monkeypatch.setattr("src.upload.MAX_UPLOAD_MB", 1)

    oversize_bytes = b"x" * (1 * 1024 * 1024 + 100)
    with pytest.raises(UploadError, match="exceeds maximum allowed limit"):
        save_uploaded_file(oversize_bytes, "large.csv")


def test_save_uploaded_file_consecutive_saves_distinct_names(tmp_path, monkeypatch):
    """Ensure rapid consecutive saves of the same filename do not collide or overwrite."""
    monkeypatch.setattr("src.upload.UPLOAD_DIR", tmp_path)

    # Freeze timestamp so both calls get identical datetime string
    frozen_time = "20261110_120000"
    with mock.patch("src.upload.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = frozen_time

        path1 = save_uploaded_file(b"col1\nval1\n", "test.csv")
        path2 = save_uploaded_file(b"col1\nval2\n", "test.csv")

    assert path1.exists()
    assert path2.exists()
    assert path1 != path2
    assert path1.read_bytes() == b"col1\nval1\n"
    assert path2.read_bytes() == b"col1\nval2\n"
