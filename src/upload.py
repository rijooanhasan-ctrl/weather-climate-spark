"""
src/upload.py
=============
Safe file upload handling for weather dataset CSVs.

Pure Python module with no Spark dependency. Handles file storage,
sanitization, path traversal prevention, and upload size/extension limits.
"""

from datetime import datetime
from pathlib import Path
import re

from config.settings import (
    ALLOWED_UPLOAD_EXTENSIONS,
    MAX_UPLOAD_MB,
    UPLOAD_DIR,
)


class UploadError(Exception):
    """Raised when an uploaded file violates security or format constraints."""
    pass


def save_uploaded_file(file_bytes: bytes, original_name: str) -> Path:
    """
    Save raw uploaded bytes to the project upload directory with a sanitized,
    timestamped filename.

    Parameters
    ----------
    file_bytes : bytes
        The raw content of the uploaded file.
    original_name : str
        The original filename reported by the client.

    Returns
    -------
    Path
        The absolute path to the saved file.

    Raises
    ------
    UploadError
        If the file is empty, exceeds MAX_UPLOAD_MB, has an unsupported extension,
        or fails validation.
    """
    # Guard against empty payloads
    if not file_bytes or len(file_bytes) == 0:
        raise UploadError("Uploaded file is empty (0 bytes).")

    # Guard against oversize payloads
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    if len(file_bytes) > max_bytes:
        size_mb = len(file_bytes) / (1024 * 1024)
        raise UploadError(
            f"File size ({size_mb:.1f} MB) exceeds maximum allowed limit of {MAX_UPLOAD_MB} MB."
        )

    # Sanitize base filename and prevent path traversal attacks
    pure_name = Path(original_name).name
    if not pure_name:
        raise UploadError("Invalid filename provided.")

    ext = Path(pure_name).suffix.lower()
    if ext not in [e.lower() for e in ALLOWED_UPLOAD_EXTENSIONS]:
        allowed_str = ", ".join(ALLOWED_UPLOAD_EXTENSIONS)
        raise UploadError(
            f"Unsupported file extension '{ext}'. Allowed extensions: {allowed_str}"
        )

    stem = Path(pure_name).stem
    safe_stem = re.sub(r"[^A-Za-z0-9_-]", "_", stem).strip("_")
    if not safe_stem:
        safe_stem = "upload"

    # Create destination folder if not present
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Build safe destination filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate_name = f"{timestamp}_{safe_stem}{ext}"
    dest_path = UPLOAD_DIR / candidate_name

    # Handle intra-second naming collisions gracefully
    counter = 1
    while dest_path.exists():
        candidate_name = f"{timestamp}_{safe_stem}_{counter}{ext}"
        dest_path = UPLOAD_DIR / candidate_name
        counter += 1

    dest_path.write_bytes(file_bytes)
    return dest_path

