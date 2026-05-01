"""
Health and corruption checks for audio files.

Provides lightweight wrappers that try cross-platform tools when present:
- `mp3val` (if installed) for MP3 health checks
- `ffmpeg` as a general decoder-based corruption probe

These functions are designed to be called from a background thread.
"""
from pathlib import Path
from typing import List, Optional
import shutil
import subprocess
import os

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/library_clean.py  →  LibraryCleaner.detect_non_audio()
#
# ⚠ REQUIRED BEFORE WIRING THE UI (v0.6.0):
#   check_with_ffmpeg() walks the full library root and runs ffmpeg against
#   every file. Non-audio files (cover art, log files, playlists, etc.) will
#   fail ffmpeg decoding and be incorrectly flagged as corrupted.
#
#   Fix: call LibraryCleaner.detect_non_audio(root) first, build a set of
#   non-audio paths, and skip those paths inside the ffmpeg walk. Only files
#   confirmed as audio should be passed to ffmpeg.
#
#   mp3val scope: check_with_mp3val() is MP3-only and correct for that scope.
#   The UI should make clear it will not scan FLAC, WAV, M4A, etc.
#   Recommended approach: mp3val for .mp3 files, ffmpeg for all other audio.
#
# Import when wiring (do not import until then — avoids circular dependency
# risk during the current pre-UI phase):
#   from .library_clean import LibraryCleaner
# ─────────────────────────────────────────────────────────────────────────────


class HealthChecker:
    @staticmethod
    def mp3val_available() -> bool:
        return shutil.which("mp3val") is not None

    @staticmethod
    def ffmpeg_available() -> bool:
        return shutil.which("ffmpeg") is not None

    @staticmethod
    def check_with_mp3val(root: str) -> List[str]:
        """Run `mp3val` recursively under `root`. Returns list of file paths reported as bad.

        `mp3val` prints a summary to stdout; this function parses lines that include the filename.
        """
        bad = []
        if not HealthChecker.mp3val_available():
            return bad
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.lower().endswith('.mp3'):
                    path = str(Path(dirpath) / fn)
                    try:
                        # mp3val exits 0 even if warnings — parse output
                        proc = subprocess.run(["mp3val", path], capture_output=True, text=True)
                        out = proc.stdout + proc.stderr
                        if "ERROR" in out or "BAD" in out or "CRC" in out:
                            bad.append(path)
                    except Exception:
                        pass
        return bad

    @staticmethod
    def check_with_ffmpeg(root: str) -> List[str]:
        """Use `ffmpeg` to attempt decoding each file; failures are considered corrupted.

        This is slower but cross-platform where `mp3val` is not available.
        """
        bad = []
        if not HealthChecker.ffmpeg_available():
            return bad
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                path = str(Path(dirpath) / fn)
                try:
                    cmd = ["ffmpeg", "-v", "error", "-i", path, "-f", "null", "-"]
                    proc = subprocess.run(cmd, capture_output=True, text=True)
                    if proc.returncode != 0 or proc.stderr:
                        bad.append(path)
                except Exception:
                    pass
        return bad
