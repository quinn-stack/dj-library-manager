"""
DJ Library Manager — Fuzzy Find / Duplicate Finder Engine (v0.4.12)
Performance-optimised for large (10k+) libraries.

v0.4.12 Changes:
  - Ambiguous filename detection (_is_ambiguous)
  - Mutagen tag fallback for ambiguous files (_read_tags)
  - find_duplicates() now returns (GroupList, AmbiguousList) tuple
    *** BREAKING CHANGE from v0.4.10 — callers must unpack the tuple ***
    Only known caller: ui/duplicate_finder_page.py _on_scan_done()

v0.4.10 Changes:
  - Automatically skips _QUARANTINE directories during walk (case-insensitive)
"""

import os
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Callable

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/library_clean.py  →  LibraryCleaner, DEFAULT_AUDIO_EXTS
# Why: LibraryCleaner.move_to_quarantine() is the single authoritative
#      implementation for moving files to quarantine with relative-path
#      preservation. Duplicating that logic here would create two diverging
#      code paths for the same operation. DEFAULT_AUDIO_EXTS is the shared
#      source-of-truth for which file extensions count as audio — defining it
#      twice would cause silent drift if one copy is updated and the other is not.
# If you modify library_clean.py, check that this file still works correctly.
# ─────────────────────────────────────────────────────────────────────────────
from .library_clean import DEFAULT_AUDIO_EXTS, LibraryCleaner

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/tag_utils.py  →  looks_reasonable()
# Why: looks_reasonable() is the single authoritative check for whether a tag
#      value is real metadata or a placeholder. Previously duplicated here and
#      in tagging.py — extracted to tag_utils.py at v0.5.3 so both engines
#      share one definition.
# If you change the placeholder set or matching logic in tag_utils.py, verify
# that _read_tags() and ambiguous file bucketing still work correctly.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .tag_utils import looks_reasonable as _looks_reasonable
except ImportError:
    from tag_utils import looks_reasonable as _looks_reasonable

# Optional mutagen import for tag-based fallback on ambiguous filenames.
# mutagen is a project-wide dependency (see requirements / installation docs).
# If it is not installed, tag fallback is silently disabled and ambiguous files
# are held for manual review rather than crashing the scan.
try:
    from mutagen import File as MutagenFile
    from mutagen.id3 import ID3, ID3NoHeaderError
    _HAS_MUTAGEN = True
except ImportError:
    _HAS_MUTAGEN = False
    MutagenFile = None


# ── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR  = Path.home() / ".dj_library_manager" / "logs"
LOG_FILE = LOG_DIR / "duplicates.log"


def _write_log_batch(messages: List[str]):
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            for msg in messages:
                f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


# ── Quarantine Skip ───────────────────────────────────────────────────────────

_QUARANTINE_DIR_NAME = "_quarantine"


def _is_quarantine_dir(dirname: str) -> bool:
    return dirname.lower() == _QUARANTINE_DIR_NAME


# ── Normalisation ─────────────────────────────────────────────────────────────

_COLLISION_RE    = re.compile(r"\s*[(\[]\s*\d+\s*[)\]]\s*$")
_COPY_RE         = re.compile(
    r"\s*[(\[]\s*(?:[\w-]+\s+)?copy(?:\s+[\w-]+)?\s*[)\]]\s*$|\s*[-_.]\s*copy\s*$",
    re.IGNORECASE
)
# ── INTER-ENGINE NAMING CONVENTION ───────────────────────────────────────────
# transfer_engine.py RENAME collision mode produces _N suffixes (e.g. Track_1.mp3,
# Track_2.mp3). _TRANSFER_SUFFIX_RE below strips these during normalisation so
# collision-renamed files are correctly identified as duplicates of the original.
# If transfer_engine.py ever changes its collision-rename suffix format, this
# regex MUST be updated to match. See "Inter-Engine Naming Convention Contract"
# in the roadmap.
# ─────────────────────────────────────────────────────────────────────────────

# Strips transfer-engine collision suffixes: _1, _2, _99 etc appended directly
# to the stem without spaces or brackets. This is the exact format produced by
# TransferEngine._resolve_collision() in RENAME mode, so files copied multiple
# times (e.g. "Track_1.mp3", "Track_2.mp3") are correctly identified as
# duplicates of the original "Track.mp3".
# Only matches at end-of-string and requires the underscore to be immediately
# followed by digits — does not strip mid-stem underscores.
_TRANSFER_SUFFIX_RE = re.compile(r"_\d+$")
_TRACK_NUMBER_RE = re.compile(r"^\d+\s*[-_.]\s*")
_PREFIX_RE       = re.compile(r"^(?:y2mate\.is|youtube|yt)\s*[-_.]\s*", re.IGNORECASE)
_PUNCT_RE        = re.compile(r"[^\w\s]")
_SPACE_RE        = re.compile(r"\s+")


def _normalise(stem: str) -> str:
    s = stem.lower().strip()
    if not s:
        return ""
    s = _PREFIX_RE.sub("", s)
    s = _TRACK_NUMBER_RE.sub("", s)
    while True:
        original = s
        s = _COLLISION_RE.sub("", s)
        s = _COPY_RE.sub("", s)
        s = _TRANSFER_SUFFIX_RE.sub("", s)
        s = s.strip()
        if s == original:
            break
    clean = _PUNCT_RE.sub(" ", s)
    return _SPACE_RE.sub(" ", clean).strip()


# ── Ambiguity Detection ───────────────────────────────────────────────────────

# Placeholder values that appear in untagged/unnamed files.
# Matched against individual words or the full normalised stem.
_PLACEHOLDER_STEMS = frozenset({
    # Generic track placeholders
    "track", "audio", "file", "clip", "recording", "untitled",
    "untitled track", "no title", "unknown track",
    # Artist placeholders
    "various", "various artists", "unknown", "unknown artist",
    "va", "v a",
    # Combined
    "unknown unknown", "various unknown", "unknown various",
})

# Matches stems that are purely a word + optional number, e.g. "track 01",
# "audio 003", "file 1" — after _normalise() has already run.
_GENERIC_NUMBERED_RE = re.compile(
    r"^(?:track|audio|file|clip|recording|untitled|unknown)\s*\d*$",
    re.IGNORECASE
)


def _is_ambiguous(normalised_stem: str) -> bool:
    """Return True if normalised_stem is a meaningless placeholder.

    A stem is ambiguous if:
      - It is 3 characters or fewer after normalisation
      - It matches a known placeholder string exactly
      - It matches the generic-numbered pattern (track 01, audio 003, etc.)
      - When split on ' - ', both halves are individually ambiguous
        (catches 'various - track 01', 'unknown - untitled', etc.)

    Ambiguous files are not bucketed for duplicate comparison — they are
    held separately for user review with an optional tag-based fallback.
    """
    s = normalised_stem.strip()

    if not s:
        return True

    if len(s) <= 3:
        return True

    if s in _PLACEHOLDER_STEMS:
        return True

    if _GENERIC_NUMBERED_RE.match(s):
        return True

    # Split on ' - ' and check if both sides are independently ambiguous.
    # Only triggers if there is exactly one separator (avoids over-splitting
    # legitimate titles that contain ' - ').
    parts = s.split(" - ", 1)
    if len(parts) == 2:
        left  = parts[0].strip()
        right = parts[1].strip()
        if (left  in _PLACEHOLDER_STEMS or _GENERIC_NUMBERED_RE.match(left)) and \
           (right in _PLACEHOLDER_STEMS or _GENERIC_NUMBERED_RE.match(right)):
            return True

    return False


# ── Tag Reading ───────────────────────────────────────────────────────────────

def _read_tags(path: str) -> Tuple[Optional[str], Optional[str]]:
    """Attempt to read artist and title tags from an audio file via mutagen.

    Returns (artist, title) strings, or (None, None) if tags are unavailable,
    unreadable, or do not pass the _looks_reasonable() check.

    Tries ID3 first (MP3), then falls back to mutagen generic reader for
    other formats (FLAC, M4A, OGG, etc.).
    """
    if not _HAS_MUTAGEN:
        return (None, None)

    artist = title = None

    try:
        # ID3 path — most reliable for MP3
        try:
            id3 = ID3(path)
            a = id3.get("TPE1")
            t = id3.get("TIT2")
            artist = str(a.text[0]) if a and getattr(a, "text", None) else None
            title  = str(t.text[0]) if t and getattr(t, "text", None) else None
        except (ID3NoHeaderError, Exception):
            pass

        # Generic mutagen fallback for non-MP3 formats
        if not artist and not title and MutagenFile is not None:
            m = MutagenFile(path, easy=True)
            if m and m.tags:
                def _first(keys):
                    for k in keys:
                        v = m.tags.get(k)
                        if v:
                            try:
                                return str(v[0]) if isinstance(v, list) else str(v)
                            except Exception:
                                pass
                    return None
                artist = _first(["artist", "ARTIST", "TPE1"])
                title  = _first(["title",  "TITLE",  "TIT2"])
    except Exception:
        return (None, None)

    # Validate both fields — we need both to construct a reliable match key
    artist = artist.strip() if artist else None
    title  = title.strip()  if title  else None

    if _looks_reasonable(artist) and _looks_reasonable(title):
        return (artist, title)

    return (None, None)


# ── Sorting ───────────────────────────────────────────────────────────────────

_SIZE_SIMILARITY_PCT = 2.0
_PROGRESS_INTERVAL   = 500


def _sort_group(entries: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """Sort a duplicate group so index 0 is the best candidate to keep.

    Sorting priority:
      1. Files within SIZE_SIMILARITY_PCT of the largest are treated as
         equally sized (size_bucket = 0); others ranked by descending size.
      2. Within size bucket: shorter filename preferred (less likely to be
         a collision-renamed copy).
      3. Tiebreak: alphabetical by filename.
    """
    max_size = max(sz for _, sz in entries) if entries else 1

    def _key(entry):
        path_str, size = entry
        pct_diff    = abs(max_size - size) / max_size * 100 if max_size else 0
        size_bucket = 0 if pct_diff <= _SIZE_SIMILARITY_PCT else -size
        name        = os.path.basename(path_str)
        return (size_bucket, len(name), name.lower())

    return sorted(entries, key=_key)


# ── Type Aliases ──────────────────────────────────────────────────────────────

GroupList     = List[List[Tuple[str, int]]]   # [(path, size_kb), ...]
AmbiguousList = List[str]                      # [path, ...]


# ── Main Entry Point ──────────────────────────────────────────────────────────

def find_duplicates(
    root: str,
    stop_event=None,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[GroupList, AmbiguousList]:
    """Scan root for duplicate audio files using fuzzy filename normalisation.

    Returns a tuple: (groups, ambiguous_files)

        groups          — list of duplicate groups, each group is a list of
                          (path, size_kb) tuples sorted best-candidate-first.
        ambiguous_files — list of file paths whose filename was ambiguous AND
                          whose tags were also missing or unreliable. These
                          files could not be matched and are held for manual
                          user review.

    *** BREAKING CHANGE from v0.4.10 ***
    Previously returned GroupList only. All callers must now unpack the tuple.
    Only known caller: ui/duplicate_finder_page.py _on_scan_done()

    Quarantine directories (_QUARANTINE, case-insensitive) are pruned from
    the walk before descent so previously quarantined files are never re-scanned.
    """
    p = Path(root)
    _write_log_batch([f"--- Starting Duplicate Scan: {root} ---"])

    if not p.exists():
        return ([], [])

    audio:      List[Tuple[str, int]] = []
    file_count  = 0
    valid_exts  = {e.lower().lstrip(".") for e in DEFAULT_AUDIO_EXTS}

    # ── Walk ──────────────────────────────────────────────────────────
    for dirpath, dirnames, filenames in os.walk(p):
        if stop_event and stop_event.is_set():
            return ([], [])

        # Prune quarantine directories before descent
        before = len(dirnames)
        dirnames[:] = [d for d in dirnames if not _is_quarantine_dir(d)]
        pruned = before - len(dirnames)
        if pruned:
            _write_log_batch([
                f"SKIP: pruned {pruned} quarantine "
                f"director{'y' if pruned == 1 else 'ies'} under {dirpath}"
            ])

        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower().lstrip(".")
            if ext in valid_exts:
                full = os.path.join(dirpath, fn)
                try:
                    size = os.stat(full).st_size
                    audio.append((full, size))
                    file_count += 1
                    if progress_cb and file_count % _PROGRESS_INTERVAL == 0:
                        progress_cb(f"Scanning… {file_count:,} audio files found")
                except OSError:
                    continue

    # ── Bucket by normalised key ───────────────────────────────────────
    buckets:         Dict[str, List[Tuple[str, int]]] = {}
    ambiguous_files: AmbiguousList                    = []
    log_buffer:      List[str]                        = []

    for path_str, size in audio:
        if stop_event and stop_event.is_set():
            return ([], [])

        stem = os.path.splitext(os.path.basename(path_str))[0]
        norm = _normalise(stem)

        if _is_ambiguous(norm):
            # Filename is a placeholder — attempt tag-based fallback
            artist, title = _read_tags(path_str)

            if artist and title:
                # Good tags found — use normalised "artist - title" as key
                tag_key = _normalise(f"{artist} - {title}")
                if tag_key:
                    buckets.setdefault(tag_key, []).append((path_str, size))
                    if len(log_buffer) < 500:
                        log_buffer.append(
                            f"AMBIGUOUS→TAG: {os.path.basename(path_str)} "
                            f"| stem='{norm}' | tag_key='{tag_key}'"
                        )
                    continue

            # No usable tags — hold for manual review
            ambiguous_files.append(path_str)
            if len(log_buffer) < 500:
                log_buffer.append(
                    f"HELD: {os.path.basename(path_str)} "
                    f"| stem='{norm}' | no usable tags"
                )
            continue

        # Normal path — bucket by normalised filename stem
        if norm:
            buckets.setdefault(norm, []).append((path_str, size))

        if len(log_buffer) < 500:
            log_buffer.append(
                f"FILE: {os.path.basename(path_str)} | NORM: '{norm}'"
            )

    _write_log_batch(log_buffer)

    if ambiguous_files:
        _write_log_batch([
            f"HELD SUMMARY: {len(ambiguous_files)} file(s) held for manual review "
            f"(ambiguous filename + no usable tags)"
        ])

    # ── Build groups ───────────────────────────────────────────────────
    groups: GroupList = []
    for norm_name, entries in buckets.items():
        if len(entries) < 2:
            continue
        sorted_entries = _sort_group(entries)
        groups.append([(p, sz // 1024) for p, sz in sorted_entries])

    groups.sort(key=lambda g: os.path.basename(g[0][0]).lower())

    _write_log_batch([
        f"Scan Finished. {len(groups)} duplicate groups, "
        f"{len(ambiguous_files)} held files."
    ])

    return (groups, ambiguous_files)


# ── Apply Actions ─────────────────────────────────────────────────────────────

def apply_duplicate_actions(
    actions: Dict[str, str],
    quarantine_dir: str,
    root: str,
) -> Dict[str, int]:
    """Apply quarantine/delete actions to a dict of {path: action}.

    ── ENGINE DEPENDENCY ────────────────────────────────────────────────────────
    Depends on: engine/library_clean.py  →  LibraryCleaner.move_to_quarantine()
    Why: See module-level ENGINE DEPENDENCY comment above.
    ─────────────────────────────────────────────────────────────────────────────
    """
    summary = {"quarantined": 0, "deleted": 0, "skipped": 0, "errors": 0}

    to_q = [p for p, a in actions.items() if a == "quarantine"]
    to_d = [p for p, a in actions.items() if a == "delete"]
    summary["skipped"] = sum(1 for a in actions.values() if a == "nothing")

    if to_q:
        try:
            moved = LibraryCleaner.move_to_quarantine(
                root, to_q, quarantine_dir, dry_run=False
            )
            summary["quarantined"] = len(moved)
            summary["errors"]     += len(to_q) - len(moved)
        except Exception as e:
            _write_log_batch([f"Quarantine Error: {e}"])
            summary["errors"] += len(to_q)

    for path in to_d:
        try:
            if os.path.exists(path):
                os.unlink(path)
                summary["deleted"] += 1
        except Exception as e:
            _write_log_batch([f"Delete Error: {e}"])
            summary["errors"] += 1

    return summary
