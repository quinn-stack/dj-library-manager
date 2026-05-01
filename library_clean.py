"""
Library cleaning utilities with quarantine support.

Provides detection for non-audio files under a library root and helpers
to preview or move those files into a quarantine directory. Designed for
use from the UI via `TaskRunner` so operations are synchronous/simple.
"""
from pathlib import Path
from typing import List, Iterable, Tuple
import os
import shutil
import mimetypes

# Optional python-magic import for robust MIME sniffing
try:
    import magic  # type: ignore
    _HAS_MAGIC = True
except Exception:
    magic = None
    _HAS_MAGIC = False


# ── USED BY OTHER ENGINES ────────────────────────────────────────────────────
# DEFAULT_AUDIO_EXTS and LibraryCleaner are imported by:
#   - engine/duplicate_finder.py  →  DEFAULT_AUDIO_EXTS, LibraryCleaner.move_to_quarantine()
#   - engine/tagging.py           →  DEFAULT_AUDIO_EXTS
#   - engine/health_check.py      →  LibraryCleaner.detect_non_audio()
#                                    ⚠ NOT YET IMPORTED — dependency annotated at v0.5.3,
#                                    import added when health check UI is wired (v0.6.0).
#                                    detect_non_audio() must be called first to exclude
#                                    non-audio files from the ffmpeg corruption scan.
# If you change DEFAULT_AUDIO_EXTS (add/remove an extension), update and test
# all callers — duplicate scanning, tag-based renaming, and health checking all
# depend on this list to decide what counts as an audio file.
# If you change LibraryCleaner.move_to_quarantine() signature or behaviour,
# update duplicate_finder.py accordingly and test both quarantine code paths.
# If you change LibraryCleaner.detect_non_audio() signature or behaviour,
# update health_check.py accordingly when the UI is wired at v0.6.0.
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_AUDIO_EXTS = {
    "mp3", "flac", "wav", "m4a", "aac", "ogg", "opus",
    "wma", "aiff", "aif", "aifc", "alac", "ape", "wv", "mp4"
}


def _is_audio_by_mime(path: str) -> bool:
    try:
        if _HAS_MAGIC:
            m = magic.from_file(path, mime=True)
            if m and isinstance(m, str) and m.startswith("audio"):
                return True
        mime, _ = mimetypes.guess_type(path)
        if mime and mime.startswith("audio"):
            return True
    except Exception:
        return False
    return False


class LibraryCleaner:
    """Detect and optionally move/remove non-audio files.

    Methods are synchronous and return values suitable to be emitted by
    `TaskRunner.finished_signal`.
    """

    @staticmethod
    def detect_non_audio(root: str, audio_exts: Iterable[str] = None) -> List[str]:
        """Return list of file paths that are likely non-audio files.

        Uses a fast extension whitelist as a quick path; when an extension
        is unknown we fall back to MIME sniffing (python-magic) or
        `mimetypes` as a fallback.
        """
        if audio_exts is None:
            audio_exts = DEFAULT_AUDIO_EXTS
        allowed_exts = {e.lower().lstrip('.') for e in audio_exts}

        p = Path(root)
        if not p.exists():
            return []

        non_audio = []
        for dirpath, _, filenames in os.walk(p):
            for fn in filenames:
                fpath = str(Path(dirpath) / fn)
                ext = Path(fn).suffix.lower().lstrip('.')
                if ext and ext in allowed_exts:
                    continue
                try:
                    if _is_audio_by_mime(fpath):
                        continue
                except Exception:
                    pass
                non_audio.append(fpath)
        return non_audio

    @staticmethod
    def remove_paths(paths: Iterable[str]) -> int:
        """Remove the given file paths. Returns count of removed items."""
        removed = 0
        for p in paths:
            try:
                Path(p).unlink()
                removed += 1
            except Exception:
                try:
                    if Path(p).is_dir():
                        shutil.rmtree(p)
                        removed += 1
                except Exception:
                    pass
        return removed

    @staticmethod
    def move_to_quarantine(root: str, paths: Iterable[str], quarantine_dir: str, dry_run: bool = True) -> List[Tuple[str, str]]:
        """Move `paths` into `quarantine_dir`, preserving relative paths when possible.

        - `root` is used to compute a relative path for preservation; if a path
          is not under `root`, its basename is used.
        - If `dry_run` is True the function will not perform file operations and
          instead return the list of (orig, dest) tuples that *would* be moved.
        - Returns list of (orig, dest) destination pairs. On errors, entries may
          be omitted.
        """
        q = Path(quarantine_dir)
        # Do not create the quarantine directory for dry runs. Only create
        # when actually moving files (dry_run == False).

        moved = []
        root_p = Path(root)
        for p in paths:
            try:
                src = Path(p)
                try:
                    rel = src.relative_to(root_p)
                except Exception:
                    rel = Path(src.name)
                dest = q / rel
                if dry_run:
                    moved.append((str(src), str(dest)))
                    continue
                # Ensure quarantine root exists only when performing the move
                try:
                    q.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
                # ensure parent exists
                dest.parent.mkdir(parents=True, exist_ok=True)
                # move file (handles across-filesystem moves)
                shutil.move(str(src), str(dest))
                moved.append((str(src), str(dest)))
            except Exception:
                # skip problematic files
                continue
        return moved
