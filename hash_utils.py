"""
DJ Library Manager — Hash Utilities
SHA256 hashing for transfer verification.

Provides chunk-based file hashing suitable for large audio files without
loading the entire file into memory. Designed to be called from background
threads — no Qt imports, no UI dependencies.
"""
import hashlib
import os
from pathlib import Path
from typing import Optional

# ── USED BY OTHER ENGINES ────────────────────────────────────────────────────
# sha256_file() is imported by:
#   - engine/transfer_engine.py  →  post-copy hash verification
# If you change the return type or raise behaviour of sha256_file(), update
# transfer_engine.py accordingly and test both first-transfer and incremental
# verification paths.
# ─────────────────────────────────────────────────────────────────────────────

# Read in 4 MB chunks — large enough to amortise syscall overhead without
# holding excessive memory for a 24-bit FLAC or uncompressed WAV.
_CHUNK_BYTES = 4 * 1024 * 1024


def sha256_file(path: str) -> str:
    """Return the lowercase hex SHA256 digest of the file at `path`.

    Reads the file in chunks so large audio files (FLAC, WAV) are handled
    without loading the entire file into memory.

    Raises:
        FileNotFoundError  — if `path` does not exist or is not a file.
        PermissionError    — if the file cannot be read.
        OSError            — for other filesystem errors.

    These are intentionally not caught here — the caller (transfer_engine)
    decides how to handle and log each failure.
    """
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Not a file: {path}")

    h = hashlib.sha256()
    with open(p, "rb") as f:
        while True:
            chunk = f.read(_CHUNK_BYTES)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_matches(src_path: str, dst_path: str) -> bool:
    """Return True if SHA256 digests of both files match.

    Both files must exist and be readable. Any error returns False — the
    caller should treat a False result as a verification failure and log it.
    """
    try:
        return sha256_file(src_path) == sha256_file(dst_path)
    except Exception:
        return False


def file_size(path: str) -> Optional[int]:
    """Return file size in bytes, or None on error."""
    try:
        return os.path.getsize(path)
    except Exception:
        return None


def mtime(path: str) -> Optional[float]:
    """Return file modification time as a float (seconds since epoch), or None on error."""
    try:
        return os.path.getmtime(path)
    except Exception:
        return None
