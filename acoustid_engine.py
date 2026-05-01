"""
DJ Library Manager — AcoustID Engine
Replaces beets entirely for fingerprint-based tagging.

Pipeline:
  1. Walk source directory, collect all audio files (MP3, WMA, FLAC, M4A, …)
  2. Fingerprint in parallel using fpcalc (4 workers, CPU-bound, no network)
  3. Lookup AcoustID API at configurable RPS (default 3, hard max 3) via token bucket
  4. Apply tags (Artist, Title, Album, Year) via mutagen — format-aware writer
  5. Log all low-confidence matches to:
       logs/tagging/low_confidence_<ts>.txt   (human-readable)
       logs/tagging/low_confidence_<ts>.json  (machine-readable, loadable by UI)
  6. Log all API-error files to logs/tagging/tagging_errors_<ts>.txt for retry
  7. Log all tag-write failures to logs/tagging/tag_write_failures_<ts>.txt
     with the actual exception message for diagnosis

── TAG WRITE FAILURE ROOT CAUSE (documented 2026-02-24) ────────────────────────
Previous write_tags() used mutagen.id3.ID3 directly — an MP3-only class.
The library contained WMA, FLAC, M4A etc. Writing ID3 frames to a WMA file
throws a mutagen exception that was silently swallowed, producing 1,330
"tag_write_failed" in the 2026-02-24 test run on Test Lib 2 (6,548 files).

Fix: write_tags() now uses format detection via file extension and routes
to the correct mutagen class per format. The return value is now a
(bool, reason_str | None) tuple — callers receive the failure reason and
log it immediately, ending the silent failure problem.
────────────────────────────────────────────────────────────────────────────────

── RETRY MODE ─────────────────────────────────────────────────────────────────
Pass `retry_files` to AcoustIDRunner.__init__() to skip file discovery and run
the pipeline only on those specific paths. The tag_finder_page passes the
error_files list from the previous run's finished() signal dict.
───────────────────────────────────────────────────────────────────────────────
"""

# ── USED BY OTHER MODULES ────────────────────────────────────────────────────
# AcoustIDRunner and AcoustIDEngine are imported by:
#   - ui/tag_finder_page.py  (sole UI surface driving the tagging pipeline)
#
# If you change any of the following, update tag_finder_page.py accordingly:
#   - AcoustIDRunner.__init__ signature (parameters: rps, retry_files,
#     skip_tagged, use_cache)
#   - AcoustIDRunner signals: progress(int,int,str), lookup_progress(int,int),
#     scanning(str), tag_check_progress(int,int),
#     result(str,str,str) — status values: tagged|cached|skipped|no_match|error
#     log(str), finished(dict)
#   - stats dict keys in finished() — especially: error_files, api_errors,
#     cache_hits, skipped_already_tagged, error_report, rps_used, tagged,
#     skipped_low_confidence, no_match, tag_write_failed, tag_write_failed_files,
#     low_confidence_report, low_confidence_json, write_failure_report, cancelled
#   - AcoustIDEngine.check_dependencies() return shape
#
# low_confidence_json is also consumed by:
#   - engine/low_confidence_manager.py (loads batch for quarantine / tag apply)
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import time
import subprocess
import shutil
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

try:
    from mutagen import File as MutagenFile
    from mutagen.mp3 import MP3, HeaderNotFoundError
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, UFID, ID3NoHeaderError
    from mutagen.flac import FLAC
    from mutagen.mp4 import MP4
    from mutagen.asf import ASF, ASFHeaderError   # WMA
    from mutagen.oggvorbis import OggVorbis
    from mutagen.aiff import AIFF
    HAS_MUTAGEN = True
except ImportError:
    HAS_MUTAGEN = False

from PySide6.QtCore import QThread, Signal


# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

ACOUSTID_API_URL       = "https://api.acoustid.org/v2/lookup"
FINGERPRINT_WORKERS    = 4
RATE_LIMIT_RPS_MAX     = 3          # AcoustID hard limit — never exceed
RATE_LIMIT_RPS_DEFAULT = 3          # Default — full speed
AUDIO_EXTENSIONS       = {
    ".mp3", ".m4a", ".flac", ".wav", ".aac",
    ".ogg", ".opus", ".wma", ".aiff",
}

# Distinguish lookup outcomes for error tracking
_OUTCOME_OK        = "ok"
_OUTCOME_NO_MATCH  = "no_match"      # Valid response but no results
_OUTCOME_NO_META   = "no_metadata"   # Matched acoustically but nothing in DB
_OUTCOME_API_ERROR = "api_error"     # HTTP error, timeout, bad JSON, status != ok
_OUTCOME_CACHED    = "cached"        # Result loaded from fingerprint cache — no API call

# Fingerprint cache — persists tagged results across runs
# Key: absolute file path.  Value: {fingerprint, duration, artist, title, album,
#      year, mbid, score, tagged_at (ISO), mtime (float)}
# Cache is only written for files where tags were successfully written.
# On load, each entry's mtime is compared to the file's current mtime —
# a changed file is evicted and re-fingerprinted.
CACHE_PATH = Path.home() / ".dj_library_manager" / "fingerprint_cache.json"


class _ScanCancelled(Exception):
    """Raised internally by collect_files() when the stop_event is set.

    Used as a clean control-flow mechanism so run() can distinguish a
    user-initiated cancellation during the file walk from a real exception.
    Never propagates outside of AcoustIDRunner.run().
    """


# MP4/M4A iTunes atom names for the four target fields
_MP4_TAG_MAP = {
    "artist": "\xa9ART",
    "title":  "\xa9nam",
    "album":  "\xa9alb",
    "year":   "\xa9day",
}

# ASF (WMA) attribute names
_ASF_TAG_MAP = {
    "artist": "Author",
    "title":  "Title",
    "album":  "WM/AlbumTitle",
    "year":   "WM/Year",
}

# Vorbis Comment field names (FLAC, OGG, Opus)
_VORBIS_TAG_MAP = {
    "artist": "artist",
    "title":  "title",
    "album":  "album",
    "year":   "date",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Token Bucket Rate Limiter
# ═══════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    """Simple token bucket for rate limiting API calls.

    Allows bursts up to `capacity` then enforces `rate` tokens/sec.
    Thread-safe for use across the lookup loop.

    `rate` is clamped to [0.1, RATE_LIMIT_RPS_MAX] at construction — the caller
    should never exceed the AcoustID hard limit but we enforce it here as a
    safety net in case a future UI control goes wrong.
    """

    def __init__(self, rate: float = RATE_LIMIT_RPS_DEFAULT,
                 capacity: float = None):
        rate           = max(0.1, min(float(rate), float(RATE_LIMIT_RPS_MAX)))
        self._rate     = rate
        self._capacity = capacity if capacity is not None else rate
        self._tokens   = self._capacity
        self._last     = time.monotonic()

    def consume(self):
        """Block until a token is available, then consume it."""
        while True:
            now          = time.monotonic()
            elapsed      = now - self._last
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last   = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            time.sleep((1.0 - self._tokens) / self._rate)


# ═══════════════════════════════════════════════════════════════════════════════
# Core Engine
# ═══════════════════════════════════════════════════════════════════════════════

class AcoustIDEngine:
    """
    Stateless engine — all methods are classmethods or staticmethods.
    State lives in AcoustIDRunner (QThread).
    """

    # ── Dependency checks ───────────────────────────────────────────────────

    @staticmethod
    def fpcalc_available() -> bool:
        return shutil.which("fpcalc") is not None

    @staticmethod
    def mutagen_available() -> bool:
        return HAS_MUTAGEN

    @staticmethod
    def check_dependencies() -> dict:
        """Return dict of dependency name -> bool. UI can display this."""
        return {
            "fpcalc":  AcoustIDEngine.fpcalc_available(),
            "mutagen": AcoustIDEngine.mutagen_available(),
        }

    # ── File discovery ───────────────────────────────────────────────────────

    @staticmethod
    def collect_files(root: str, extensions: set = None,
                      stop_event=None, progress_callback=None) -> list:
        """Walk root and return sorted list of audio file paths.

        Args:
            stop_event:        threading.Event — if set, raises _ScanCancelled
                               after the current directory finishes. Checked
                               every 200 directories so the overhead is minimal
                               even on deeply nested libraries.
            progress_callback: optional callable(n_found, current_dir) for
                               live count updates during the walk.
        """
        if extensions is None:
            extensions = AUDIO_EXTENSIONS
        found     = []
        dir_count = 0
        for dirpath, _, filenames in os.walk(root):
            dir_count += 1
            # Check stop every 200 directories — cheap enough to be invisible,
            # responsive enough to cancel within a second on any real library.
            if stop_event is not None and dir_count % 200 == 0:
                if stop_event.is_set():
                    raise _ScanCancelled()
            for fn in filenames:
                if Path(fn).suffix.lower() in extensions:
                    found.append(os.path.join(dirpath, fn))
            if progress_callback is not None:
                progress_callback(len(found), dirpath)
        # Final stop check before the expensive sort
        if stop_event is not None and stop_event.is_set():
            raise _ScanCancelled()
        return sorted(found)

    # ── Fingerprint cache ────────────────────────────────────────────────────

    @staticmethod
    def load_cache() -> dict:
        """Load the fingerprint cache from disk. Returns {} on any error."""
        try:
            if CACHE_PATH.exists():
                with open(CACHE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    @staticmethod
    def save_cache(cache: dict) -> None:
        """Persist the fingerprint cache to disk. Silent on failure."""
        try:
            CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
        except Exception:
            pass

    @staticmethod
    def cache_entry_valid(path: str, entry: dict) -> bool:
        """Return True if a cache entry is still valid for the given file.

        An entry is invalid if:
          - The file no longer exists
          - The file's mtime has changed since the entry was written
            (meaning the file was replaced or re-encoded)
        """
        try:
            current_mtime = os.path.getmtime(path)
            return abs(current_mtime - entry.get("mtime", 0)) < 1.0
        except OSError:
            return False

    # ── Already-tagged check ─────────────────────────────────────────────────

    @staticmethod
    def has_tags(path: str) -> bool:
        """Return True if the file already has non-empty Artist AND Title tags.

        Uses mutagen's universal File() reader — format-agnostic.
        Returns False on any read error so the file is included in the run.

        Field names checked per format:
          ID3 (MP3/AIFF)  : TPE1 (artist), TIT2 (title)
          Vorbis (FLAC/OGG/Opus): 'artist', 'title'
          MP4/M4A         : ©ART, ©nam
          ASF/WMA         : Author, Title
        """
        if not HAS_MUTAGEN:
            return False
        try:
            audio = MutagenFile(path)
            if audio is None:
                return False
            tags = audio.tags
            if tags is None:
                return False

            ext = Path(path).suffix.lower()

            if ext in (".mp3", ".aiff"):
                artist = str(tags.get("TPE1", "")).strip()
                title  = str(tags.get("TIT2", "")).strip()
            elif ext in (".flac", ".ogg", ".opus"):
                artist = "".join(tags.get("artist", [])).strip()
                title  = "".join(tags.get("title",  [])).strip()
            elif ext in (".m4a", ".aac"):
                artist = "".join(str(v) for v in tags.get("\xa9ART", [])).strip()
                title  = "".join(str(v) for v in tags.get("\xa9nam", [])).strip()
            elif ext == ".wma":
                artist = "".join(str(v) for v in tags.get("Author",  [])).strip()
                title  = "".join(str(v) for v in tags.get("Title",   [])).strip()
            else:
                # Generic fallback — try common Vorbis-style keys
                artist = "".join(str(v) for v in tags.get("artist", [])).strip()
                title  = "".join(str(v) for v in tags.get("title",  [])).strip()

            return bool(artist) and bool(title)
        except Exception:
            return False

    # ── Fingerprinting ───────────────────────────────────────────────────────

    @staticmethod
    def fingerprint_file(path: str) -> dict | None:
        """
        Run fpcalc on a single file. Returns dict with keys:
            file, duration (int), fingerprint (str)
        Returns None on failure.
        """
        try:
            result = subprocess.run(
                ["fpcalc", "-json", path],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                return None
            data = json.loads(result.stdout)
            return {
                "file":        path,
                "duration":    int(data.get("duration", 0)),
                "fingerprint": data.get("fingerprint", ""),
            }
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
            return None

    @staticmethod
    def fingerprint_batch(files: list, stop_event: Event = None,
                          progress_callback=None) -> list:
        """
        Fingerprint a list of files using a thread pool (FINGERPRINT_WORKERS workers).
        Returns list of successful fingerprint dicts (failed files are omitted).

        progress_callback(current, total, filename) called after each file completes.
        stop_event is checked between submissions — set it to cancel.
        """
        results   = []
        total     = len(files)
        completed = 0

        with ThreadPoolExecutor(max_workers=FINGERPRINT_WORKERS) as pool:
            futures = {pool.submit(AcoustIDEngine.fingerprint_file, f): f for f in files}
            for future in as_completed(futures):
                if stop_event and stop_event.is_set():
                    for f in futures:
                        f.cancel()
                    break
                completed += 1
                result     = future.result()
                filename   = os.path.basename(futures[future])
                if result:
                    results.append(result)
                if progress_callback:
                    progress_callback(completed, total, filename)

        return results

    # ── API Lookup ───────────────────────────────────────────────────────────

    @staticmethod
    def lookup_fingerprint(fingerprint_data: dict, api_key: str,
                            bucket: TokenBucket) -> dict:
        """
        Look up a single fingerprint against the AcoustID API.
        Enforces rate limit via bucket.consume().

        Always returns a dict with at minimum:
            file (str), outcome (str — one of _OUTCOME_* constants)

        On _OUTCOME_OK also: score, artist, title, album, year, mbid
        On _OUTCOME_NO_META also: score
        On _OUTCOME_API_ERROR also: error_detail (str)
        """
        bucket.consume()
        file_path = fingerprint_data["file"]

        params  = urllib.parse.urlencode({
            "client":      api_key,
            "duration":    fingerprint_data["duration"],
            "fingerprint": fingerprint_data["fingerprint"],
            "format":      "json",
        })
        params += "&meta=recordings+releasegroups+compress"
        url     = f"{ACOUSTID_API_URL}?{params}"

        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": f"HTTP {exc.code} {exc.reason}"}
        except urllib.error.URLError as exc:
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": f"Network error: {exc.reason}"}
        except TimeoutError:
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": "Request timed out"}
        except json.JSONDecodeError:
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": "Invalid JSON in API response"}
        except Exception as exc:
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": str(exc)}

        if data.get("status") != "ok":
            return {"file": file_path, "outcome": _OUTCOME_API_ERROR,
                    "error_detail": f"API status: {data.get('status', 'unknown')}"}

        results = data.get("results", [])
        if not results:
            return {"file": file_path, "outcome": _OUTCOME_NO_MATCH}

        best       = max(results, key=lambda r: r.get("score", 0))
        score      = best.get("score", 0.0)
        recordings = best.get("recordings", [])
        artist = title = album = year = mbid = ""

        if recordings:
            rec    = recordings[0]
            mbid   = rec.get("id", "")
            title  = rec.get("title", "")
            artists = rec.get("artists", [])
            if artists:
                artist = artists[0].get("name", "")
            rgs = rec.get("releasegroups", [])
            if rgs:
                album    = rgs[0].get("title", "")
                releases = rgs[0].get("releases", [])
                if releases:
                    date = releases[0].get("date", {})
                    year = str(date.get("year", "")) if isinstance(date, dict) else ""

        if not artist and not title:
            return {"file": file_path, "outcome": _OUTCOME_NO_META, "score": score}

        return {
            "file":    file_path,
            "outcome": _OUTCOME_OK,
            "score":   score,
            "artist":  artist,
            "title":   title,
            "album":   album,
            "year":    year,
            "mbid":    mbid,
        }

    # ── Tag Writing ──────────────────────────────────────────────────────────

    @staticmethod
    def _patch_and_reload_id3(path: str):
        """
        Fix an ID3v2 header whose 4-byte size field contains non-synchsafe bytes
        (any byte with its high bit set), then reload and return the ID3 object
        with all existing tags intact.

        Background: ID3v2.3 requires bytes 6-9 of the file to be "synchsafe" —
        each byte must have its high bit clear (values 0x00-0x7F). CD rippers and
        encoders from ~2000-2005 commonly wrote a plain 32-bit big-endian int
        instead. mutagen refuses to parse such files, raising:
            ValueError: Header size not synchsafe

        The actual tag frames that follow the 10-byte header are valid ID3v2.3
        data and are fully recoverable by writing a corrected size field.

        Strategy:
          1. Read the 4 raw size bytes from offset 6.
          2. Decode as plain big-endian int (how the old encoder wrote it).
          3. Re-encode as a proper synchsafe int (7 bits per byte, MSB first).
          4. Patch bytes 6-9 in-place (10-byte header only — no audio touched).
          5. Re-open with ID3() — now succeeds, all frames intact.

        If patching fails for any reason, returns an empty ID3() so the caller
        can still write fresh tags without crashing.
        """
        import struct as _struct
        try:
            with open(path, "r+b") as fh:
                fh.seek(6)
                raw = fh.read(4)
                if len(raw) < 4:
                    return ID3()
                size = _struct.unpack(">I", raw)[0]
                # Re-encode as synchsafe: 7 bits per byte, most-significant first
                b0 = (size >> 21) & 0x7F
                b1 = (size >> 14) & 0x7F
                b2 = (size >>  7) & 0x7F
                b3 =  size        & 0x7F
                fh.seek(6)
                fh.write(bytes([b0, b1, b2, b3]))
            try:
                return ID3(path)
            except Exception:
                return ID3()
        except Exception:
            return ID3()

    @staticmethod
    def write_tags(match: dict, partial: bool = False) -> tuple[bool, str | None]:
        """
        Write Artist, Title, Album, Year, MBID tags to an audio file via mutagen.
        Format-aware — routes to the correct mutagen class per file extension.

        partial=False (default):
            Write each field from match if it has a value (existing tags for those
            fields are overwritten).
        partial=True:
            Only write a field if it is ABSENT or EMPTY in the existing file AND
            present in match.  Used for _OUTCOME_NO_META results where the DB has
            no artist/title but may have album/year/mbid — we fill in the gaps
            without touching anything the file already has.

        Returns:
            (True,  None)        on success
            (True,  warning_str) on success via format fallback
            (False, reason_str)  on failure — caller logs the reason

        ── Format routing ────────────────────────────────────────────────────
        .mp3  / .aiff  → mutagen.id3.ID3  (ID3v2.3, UTF-8)
        .flac          → mutagen.flac.FLAC  (Vorbis Comments)
        .ogg  / .opus  → mutagen.oggvorbis.OggVorbis  (Vorbis Comments)
        .m4a  / .aac   → mutagen.mp4.MP4  (iTunes atoms)
        .wma           → mutagen.asf.ASF  (Windows Media attributes)
        other          → mutagen.File(easy=True) fallback
        ─────────────────────────────────────────────────────────────────────
        """
        if not HAS_MUTAGEN:
            return False, "mutagen not installed"

        path = match["file"]
        ext  = Path(path).suffix.lower()

        def _tag_empty(existing, key) -> bool:
            """True if key is absent or its value is an empty/whitespace string."""
            try:
                v = existing.get(key)
                if v is None:
                    return True
                text = str(v).strip() if not isinstance(v, (list, tuple))                        else "".join(str(x) for x in v).strip()
                return not text
            except Exception:
                return True  # conservative: treat unreadable as absent

        def _should_write(existing, key, value) -> bool:
            """Return True if this field should be written."""
            return bool(value) and (not partial or _tag_empty(existing, key))

        try:
            if ext in (".mp3", ".aiff"):
                try:
                    tags = ID3(path)
                except ID3NoHeaderError:
                    tags = ID3()
                except ValueError:
                    # "Header size not synchsafe" — older CD rippers (~2000-2005)
                    # wrote a plain big-endian int for the ID3 tag size instead of
                    # the required synchsafe encoding. The tag payload is valid;
                    # only the 4 size bytes in the 10-byte header need patching.
                    # Patch in-place and re-open so existing tags are preserved.
                    tags = AcoustIDEngine._patch_and_reload_id3(path)

                id3_fields = {
                    "artist": (TPE1, "TPE1"),
                    "title":  (TIT2, "TIT2"),
                    "album":  (TALB, "TALB"),
                    "year":   (TDRC, "TDRC"),
                }
                for field, (frame_cls, frame_id) in id3_fields.items():
                    val = match.get(field)
                    if _should_write(tags, frame_id, val):
                        tags.add(frame_cls(encoding=3, text=str(val)))

                mbid = match.get("mbid")
                if mbid:
                    ufid_key = "UFID:http://musicbrainz.org"
                    if not partial or ufid_key not in tags:
                        tags.add(UFID(owner="http://musicbrainz.org",
                                      data=mbid.encode("utf-8")))
                tags.save(path, v2_version=3)

            elif ext == ".flac":
                audio = FLAC(path)
                for field, tag_key in _VORBIS_TAG_MAP.items():
                    if _should_write(audio, tag_key, match.get(field)):
                        audio[tag_key] = [str(match[field])]
                audio.save()

            elif ext in (".ogg", ".opus"):
                audio = OggVorbis(path)
                for field, tag_key in _VORBIS_TAG_MAP.items():
                    if _should_write(audio, tag_key, match.get(field)):
                        audio[tag_key] = [str(match[field])]
                audio.save()

            elif ext in (".m4a", ".aac"):
                audio = MP4(path)
                for field, atom in _MP4_TAG_MAP.items():
                    if _should_write(audio, atom, match.get(field)):
                        audio[atom] = [str(match[field])]
                audio.save()

            elif ext == ".wma":
                # First attempt: treat as a genuine ASF/WMA container.
                # Some files carry a .wma extension but are actually MP3, MP4,
                # or another format internally (common with early-2000s rips).
                # When mutagen raises ASFHeaderError it means the magic bytes
                # don't match ASF — fall back to format-sniffing via MutagenFile()
                # which ignores the extension and reads the actual container type.
                # We do NOT rename the file: the wrong extension is harmless to
                # players and renaming would break any DJ software playlist or
                # crate entry that references the current filename.
                _wma_format_note = None
                try:
                    audio = ASF(path)
                    for field, attr_name in _ASF_TAG_MAP.items():
                        if _should_write(audio, attr_name, match.get(field)):
                            audio[attr_name] = [str(match[field])]
                    audio.save()
                except ASFHeaderError:
                    # File is not actually ASF — sniff real format and retry
                    audio = MutagenFile(path, easy=True)
                    if audio is None:
                        return (False,
                                "ASFHeaderError: not an ASF file, and mutagen "
                                "could not identify the true format. File may "
                                "be corrupt or an unsupported container.")
                    _wma_format_note = type(audio).__name__
                    try:
                        for field, tag_key in _VORBIS_TAG_MAP.items():
                            if _should_write(audio, tag_key, match.get(field)):
                                audio[tag_key] = [str(match[field])]
                        audio.save()
                    except Exception:
                        # Vorbis-style keys failed — try ID3 (file may be MP3).
                        try:
                            try:
                                tags = ID3(path)
                            except ID3NoHeaderError:
                                tags = ID3()
                            except HeaderNotFoundError:
                                return (False,
                                        f"ASFHeaderError: not an ASF file. "
                                        f"Fallback ({_wma_format_note}) also failed: "
                                        f"HeaderNotFoundError — not an MP3. "
                                        f"File format unrecognised or unsupported.")
                            id3_fields = {
                                "artist": (TPE1, "TPE1"),
                                "title":  (TIT2, "TIT2"),
                                "album":  (TALB, "TALB"),
                                "year":   (TDRC, "TDRC"),
                            }
                            for field, (frame_cls, frame_id) in id3_fields.items():
                                if _should_write(tags, frame_id, match.get(field)):
                                    tags.add(frame_cls(encoding=3, text=str(match[field])))
                            tags.save(path, v2_version=3)
                        except Exception as id3_exc:
                            return (False,
                                    f"ASFHeaderError: not an ASF file. "
                                    f"Fallback ({_wma_format_note}) also failed: "
                                    f"{type(id3_exc).__name__}: {id3_exc}")
                if _wma_format_note:
                    return (True, f"⚠ extension mismatch — file is {_wma_format_note}, "
                                  f"not ASF. Tags written via fallback. "
                                  f"Extension left unchanged.")

            else:
                # Fallback for .wav and any other extensions
                audio = MutagenFile(path, easy=True)
                if audio is None:
                    return (False,
                            f"mutagen.File() returned None for '{ext}' — format not supported")
                for field, tag_key in _VORBIS_TAG_MAP.items():
                    if _should_write(audio, tag_key, match.get(field)):
                        try:
                            audio[tag_key] = [str(match[field])]
                        except Exception:
                            pass
                audio.save()

            return True, None

        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    # ── Reporting ────────────────────────────────────────────────────────────

    @staticmethod
    def write_low_confidence_report(low_confidence: list, out_dir: str = None,
                                     cutoff: float = None) -> tuple[str | None, str | None]:
        """
        Write timestamped low-confidence reports in two formats.

        1. Human-readable  .txt  — for quick inspection
        2. Machine-readable .json — for loading into the Low Confidence Manager UI

        Both share the same timestamp stem so they can be paired.

        Returns (txt_path, json_path) — either may be None on write failure.

        JSON schema:
        {
            "schema_version": 1,
            "generated": "20260224T223013Z",
            "cutoff": 0.90,
            "count": 47,
            "entries": [
                {
                    "file":   "/full/path/to/file.wma",
                    "score":  0.947,
                    "artist": "Cyndi Lauper",
                    "title":  "Girls Just Want to Have Fun",
                    "album":  "She's So Unusual",
                    "year":   "1983",
                    "mbid":   "...",
                    "action": null    // set by Low Confidence Manager:
                                      // "apply" | "quarantine" | "skip"
                }
            ]
        }
        """
        if not low_confidence:
            return None, None

        if out_dir is None:
            out_dir = str(Path.home() / ".dj_library_manager" / "logs" / "tagging")

        txt_path  = None
        json_path = None
        now       = datetime.now()
        ts_iso    = now.strftime("%Y%m%dT%H%M%SZ")   # kept for .txt / internal use
        ts_human  = now.strftime("%d-%m-%y_%H-%M-%S") # human-readable for .json filename
        sorted_lc = sorted(low_confidence, key=lambda x: x.get("score", 0))

        # ── Human-readable TXT ───────────────────────────────────────────────
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            txt_fpath = Path(out_dir) / f"low_confidence_{ts_iso}.txt"
            with open(txt_fpath, "w", encoding="utf-8") as f:
                f.write("# AcoustID Low Confidence Report\n")
                f.write(f"# Generated: {ts_iso} UTC\n")
                if cutoff is not None:
                    f.write(f"# Threshold cutoff: {cutoff:.2f}\n")
                f.write(f"# Tracks listed: {len(low_confidence)}\n\n")
                for entry in sorted_lc:
                    score_pct = f"{entry.get('score', 0) * 100:.1f}%"
                    f.write(f"[{score_pct}]  {os.path.basename(entry['file'])}\n")
                    f.write(f"         Best match: {entry.get('artist', '—')} — "
                            f"{entry.get('title', '—')}\n")
                    f.write(f"         Path: {entry['file']}\n\n")
            txt_path = str(txt_fpath)
        except Exception:
            pass

        # ── Machine-readable JSON ────────────────────────────────────────────
        # LC_BATCH files get their own subdirectory so they're kept separate from
        # the human-readable .txt reports and the run summary .json files.
        try:
            lc_batch_dir = Path(out_dir) / "lc_batches"
            lc_batch_dir.mkdir(parents=True, exist_ok=True)
            # Filename: LC_BATCH_DD-MM-YY_hh-mm-ss.json
            json_fpath = lc_batch_dir / f"LC_BATCH_{ts_human}.json"
            batch = {
                "schema_version": 1,
                "generated":      ts_iso,
                "cutoff":         cutoff,
                "count":          len(low_confidence),
                "entries": [
                    {
                        "file":   e["file"],
                        "score":  round(e.get("score", 0.0), 4),
                        "artist": e.get("artist", ""),
                        "title":  e.get("title", ""),
                        "album":  e.get("album", ""),
                        "year":   e.get("year", ""),
                        "mbid":   e.get("mbid", ""),
                        "action": None,   # populated by Low Confidence Manager
                    }
                    for e in sorted_lc
                ],
            }
            with open(json_fpath, "w", encoding="utf-8") as f:
                json.dump(batch, f, indent=2, ensure_ascii=False)
            json_path = str(json_fpath)
        except Exception:
            pass

        return txt_path, json_path

    @staticmethod
    def write_tag_failure_report(failures: list, out_dir: str = None) -> str | None:
        """
        Write a timestamped report of tag write failures including the actual
        exception message per file, grouped by format extension.

        Each entry in failures is:
            { "file": str, "ext": str, "reason": str }

        Returns path to report file, or None on failure.

        This is a diagnostic report — not a retry list. Write failures are
        typically format or permissions issues, not transient network errors.
        The extension breakdown at the top of the file makes it immediately
        obvious if the cause is a particular format (e.g., all WMA).
        """
        if not failures:
            return None

        if out_dir is None:
            out_dir = str(Path.home() / ".dj_library_manager" / "logs" / "tagging")

        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            ts    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fpath = Path(out_dir) / f"tag_write_failures_{ts}.txt"

            by_ext: dict[str, list] = {}
            for entry in failures:
                by_ext.setdefault(entry.get("ext", "unknown"), []).append(entry)

            with open(fpath, "w", encoding="utf-8") as f:
                f.write("# AcoustID Tag Write Failure Report\n")
                f.write(f"# Generated: {ts} UTC\n")
                f.write(f"# Total failures: {len(failures)}\n\n")
                f.write("# Breakdown by format:\n")
                for ext, entries in sorted(by_ext.items(), key=lambda kv: -len(kv[1])):
                    f.write(f"#   {ext:8s}  {len(entries):,} file(s)\n")
                f.write("\n")
                f.write("# Common causes:\n")
                f.write("#   .wma — ASF tag write failed. Check mutagen>=1.45 and file permissions.\n")
                f.write("#   .wav — WAV has limited mutagen write support (format-dependant).\n")
                f.write("#   Any  — 'Permission denied' = file is read-only.\n")
                f.write("#   Any  — 'No space left' = destination disk full.\n\n")

                for entry in failures:
                    f.write(f"[FAIL]  {os.path.basename(entry['file'])}\n")
                    f.write(f"         Format: {entry.get('ext', '?')}\n")
                    f.write(f"         Reason: {entry.get('reason', 'unknown')}\n")
                    f.write(f"         Path:   {entry['file']}\n\n")

            return str(fpath)
        except Exception:
            return None

    @staticmethod
    def write_error_report(error_entries: list, out_dir: str = None,
                            rps_used: float = None) -> str | None:
        """
        Write a timestamped report of API lookup errors (timeouts, HTTP errors,
        bad JSON). These files can be retried via AcoustIDRunner(retry_files=...).

        Returns path to report file, or None on failure.
        """
        if not error_entries:
            return None

        if out_dir is None:
            out_dir = str(Path.home() / ".dj_library_manager" / "logs" / "tagging")

        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            ts    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fpath = Path(out_dir) / f"tagging_errors_{ts}.txt"

            with open(fpath, "w", encoding="utf-8") as f:
                f.write("# AcoustID API Error Report\n")
                f.write(f"# Generated: {ts} UTC\n")
                if rps_used is not None:
                    f.write(f"# Rate limit used: {rps_used:.1f} RPS\n")
                    if rps_used >= RATE_LIMIT_RPS_MAX:
                        f.write("# TIP: Reduce RPS in Settings if errors are consistent.\n")
                    else:
                        f.write(f"# TIP: Already reduced to {rps_used:.1f} RPS — "
                                "server may be temporarily unavailable.\n")
                f.write(f"# Files with errors: {len(error_entries)}\n\n")

                for entry in error_entries:
                    f.write(f"[ERROR]  {os.path.basename(entry['file'])}\n")
                    f.write(f"         Reason: {entry.get('error_detail', 'unknown')}\n")
                    f.write(f"         Path: {entry['file']}\n\n")

            return str(fpath)
        except Exception:
            return None

    @staticmethod
    def write_summary_report(stats: dict, out_dir: str = None) -> str | None:
        """Write a timestamped summary JSON report for this run."""
        if out_dir is None:
            out_dir = str(Path.home() / ".dj_library_manager" / "logs" / "tagging")
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            ts    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fpath = Path(out_dir) / f"tagging_summary_{ts}.json"
            stats["timestamp"] = ts
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            return str(fpath)
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# QThread Runner
# ═══════════════════════════════════════════════════════════════════════════════

class AcoustIDRunner(QThread):
    """
    Runs the full AcoustID pipeline in a background thread.

    ── Normal mode ──────────────────────────────────────────────────────────────
    AcoustIDRunner(source_path, api_key, ...)
    Discovers all audio files under source_path, fingerprints, looks up, tags.

    ── Retry mode ───────────────────────────────────────────────────────────────
    AcoustIDRunner(source_path, api_key, ..., retry_files=["/path/a.mp3", ...])
    Skips file discovery. Only processes the explicit list.

    ── RPS control ──────────────────────────────────────────────────────────────
    rps= is clamped to [0.1, RATE_LIMIT_RPS_MAX=3].

    Signals:
        progress(current, total, filename)
        result(filename, status, score_pct)
            status: 'tagged' | 'skipped' | 'no_match' | 'error'
        log(message)
        finished(summary_dict)

    finished() stats dict keys:
        total_files, fingerprinted, fingerprint_failed
        tagged, skipped_low_confidence, no_match, no_metadata
        tag_write_failed          — count of write failures
        tag_write_failed_files    — list of {file, ext, reason} dicts
        api_errors                — count of API errors
        error_files               — list of paths (for retry_files=)
        low_confidence_report     — path to .txt (or None)
        low_confidence_json       — path to .json batch (or None)
        write_failure_report      — path to write failure .txt (or None)
        error_report              — path to API error .txt (or None)
        summary_report            — path to run summary .json (or None)
        cancelled, rps_used
    """

    progress          = Signal(int, int, str)   # Stage 1: (current, total, filename)
    lookup_progress   = Signal(int, int)         # Stage 2: (idx, total) — one per lookup
    scanning          = Signal(str)              # Pre-fingerprint phase label
    tag_check_progress = Signal(int, int)        # Skip-tagged check: (current, total)
    result            = Signal(str, str, str)
    log               = Signal(str)
    finished          = Signal(dict)

    def __init__(self, source_path: str, api_key: str,
                 strong_thresh: float = 0.95,
                 medium_thresh: float = 0.90,
                 rps: float = RATE_LIMIT_RPS_DEFAULT,
                 retry_files: list = None,
                 skip_tagged: bool = False,
                 use_cache: bool = True):
        super().__init__()
        self.source_path   = source_path
        self.api_key       = api_key
        self.strong_thresh = strong_thresh
        self.medium_thresh = medium_thresh
        self.rps           = max(0.1, min(float(rps), float(RATE_LIMIT_RPS_MAX)))
        self.retry_files   = retry_files or []
        self.skip_tagged   = skip_tagged
        self.use_cache     = use_cache
        self._stop_event   = Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        is_retry = bool(self.retry_files)

        stats = {
            "total_files":            0,
            "skipped_already_tagged": 0,
            "cache_hits":             0,
            "fingerprinted":          0,
            "fingerprint_failed":     0,
            "tagged":                 0,
            "skipped_low_confidence": 0,
            "no_match":               0,
            "no_metadata":            0,
            "tag_write_failed":       0,
            "tag_write_failed_files": [],
            "api_errors":             0,
            "error_files":            [],
            "low_confidence_report":  None,
            "low_confidence_json":    None,
            "write_failure_report":   None,
            "error_report":           None,
            "summary_report":         None,
            "cancelled":              False,
            "rps_used":               self.rps,
        }

        # ── Dependency check ────────────────────────────────────────────────
        deps = AcoustIDEngine.check_dependencies()
        if not deps["fpcalc"]:
            self.log.emit("✘  fpcalc not found. Install chromaprint:\n"
                          "    Linux:  sudo apt install libchromaprint-tools\n"
                          "    macOS:  brew install chromaprint")
            self.finished.emit(stats)
            return
        if not deps["mutagen"]:
            self.log.emit("✘  mutagen not installed. Run: pip install mutagen")
            self.finished.emit(stats)
            return

        # ── File list ────────────────────────────────────────────────────────
        if is_retry:
            files   = [f for f in self.retry_files if os.path.isfile(f)]
            skipped = len(self.retry_files) - len(files)
            self.log.emit(
                f"↩  RETRY MODE — {len(files):,} file(s) queued"
                + (f" ({skipped} skipped — file(s) missing)" if skipped else "")
                + "."
            )
        else:
            self.scanning.emit("Scanning for audio files…")
            self.log.emit(f"Scanning for audio files in:\n  {self.source_path}")
            try:
                files = AcoustIDEngine.collect_files(
                    self.source_path, stop_event=self._stop_event
                )
            except _ScanCancelled:
                self.log.emit("⚠  File scan cancelled by user.")
                stats["cancelled"] = True
                self.finished.emit(stats)
                return

        stats["total_files"] = len(files)
        if not files:
            self.log.emit("⚠  No audio files found.")
            self.finished.emit(stats)
            return

        # ── Skip already-tagged ──────────────────────────────────────────────
        # Applied before cache check — if a file has tags we don't need to
        # touch it at all, regardless of whether it's in the cache.
        if self.skip_tagged and not is_retry:
            n_files = len(files)
            self.scanning.emit(f"Checking tags on {n_files:,} files…")
            self.log.emit("Checking for already-tagged files...")
            untagged = []
            # Emit progress every 500 files (not every file) to avoid flooding
            # the Qt signal queue on large libraries (65k+ files = 65k cross-thread
            # signal deliveries visible as a multi-minute apparent freeze).
            # The UI timer-driven ETA label updates once per second anyway, so
            # sub-second granularity here is wasted work.
            _EMIT_STRIDE = 500
            for i, path in enumerate(files, 1):
                if i == 1 or i % _EMIT_STRIDE == 0 or i == n_files:
                    self.tag_check_progress.emit(i, n_files)
                    # Check stop at the same stride so we never spin more
                    # than _EMIT_STRIDE mutagen reads before honouring STOP.
                    if self._stop_event.is_set():
                        self.log.emit("⚠  Tag check cancelled by user.")
                        stats["cancelled"] = True
                        self.finished.emit(stats)
                        return
                if AcoustIDEngine.has_tags(path):
                    stats["skipped_already_tagged"] += 1
                else:
                    untagged.append(path)
            if stats["skipped_already_tagged"]:
                self.log.emit(
                    f"  Skipped {stats['skipped_already_tagged']:,} file(s) "
                    f"that already have Artist + Title tags."
                )
            files = untagged

        # ── Fingerprint cache ────────────────────────────────────────────────
        # Load cache and partition files into hits (skip fpcalc + API) and
        # misses (need full fingerprint + lookup pipeline).
        if self.use_cache and not is_retry:
            self.scanning.emit("Checking fingerprint cache…")
        cache          = AcoustIDEngine.load_cache() if self.use_cache else {}
        cache_hits     = []   # fp dicts reconstructed from cache — injected into results
        cache_misses   = []   # files that need fpcalc + lookup

        if cache and not is_retry:
            for path in files:
                entry = cache.get(path)
                if entry and AcoustIDEngine.cache_entry_valid(path, entry):
                    # Reconstruct a fingerprint-style dict so it can flow through
                    # the same result-emit path as a live lookup
                    cache_hits.append({
                        "file":        path,
                        "outcome":     _OUTCOME_CACHED,
                        "score":       entry.get("score", 1.0),
                        "artist":      entry.get("artist", ""),
                        "title":       entry.get("title", ""),
                        "album":       entry.get("album", ""),
                        "year":        entry.get("year", ""),
                        "mbid":        entry.get("mbid", ""),
                        "fingerprint": entry.get("fingerprint", ""),
                        "duration":    entry.get("duration", 0),
                    })
                    stats["cache_hits"] += 1
                else:
                    cache_misses.append(path)

            if stats["cache_hits"]:
                self.log.emit(
                    f"  Cache: {stats['cache_hits']:,} file(s) already tagged on a "
                    f"previous run — skipping fingerprint + lookup."
                )
            files = cache_misses
        else:
            files = list(files)   # ensure mutable copy

        if not files and not cache_hits:
            self.log.emit("⚠  No files to process after applying filters.")
            self.finished.emit(stats)
            return

        rps_note = (
            f" (reduced to {self.rps:.1f} RPS — server stress mode)"
            if self.rps < RATE_LIMIT_RPS_MAX else f" (max {int(self.rps)} RPS)"
        )
        if files:
            self.log.emit(
                f"Found {len(files):,} file(s) to fingerprint "
                f"({FINGERPRINT_WORKERS} parallel workers)..."
            )

        # ── Stage 1: Fingerprint ─────────────────────────────────────────────
        def on_fp_progress(current, total, filename):
            self.progress.emit(current, total, filename)
            if current % 100 == 0 or current == total:
                self.log.emit(f"  Fingerprinted {current:,}/{total:,} — {filename}")

        if files:
            fingerprints = AcoustIDEngine.fingerprint_batch(
                files, stop_event=self._stop_event, progress_callback=on_fp_progress,
            )
        else:
            fingerprints = []
            # Emit a synthetic progress signal so the UI transitions cleanly to Stage 2
            self.progress.emit(0, 0, "")

        stats["fingerprinted"]      = len(fingerprints)
        stats["fingerprint_failed"] = len(files) - len(fingerprints)

        if self._stop_event.is_set():
            self.log.emit("⚠  Fingerprinting cancelled by user.")
            stats["cancelled"] = True
            self.finished.emit(stats)
            return

        if files:
            self.log.emit(
                f"\n✔  Fingerprinting complete: {len(fingerprints):,} succeeded, "
                f"{stats['fingerprint_failed']:,} failed.\n"
                f"Starting AcoustID lookups{rps_note}..."
            )

        # ── Stage 2: Lookup + Tag ────────────────────────────────────────────
        bucket         = TokenBucket(rate=self.rps, capacity=self.rps)
        low_confidence = []
        error_entries  = []
        # Combine: cache hits go first (instant, no API), then live lookups
        all_work       = cache_hits + fingerprints
        total_fp       = len(all_work)

        def _do_write(m: dict) -> bool:
            """Write tags; record the failure detail if it fails. Returns success bool.

            write_tags() returns (True, None) on clean success.
            write_tags() returns (True, warning_str) when tags were written via
            fallback (e.g. a .wma that was not actually ASF). We log the warning
            but still count the file as successfully tagged.
            write_tags() returns (False, reason_str) on genuine failure.
            """
            ok, reason = AcoustIDEngine.write_tags(m)
            if not ok:
                stats["tag_write_failed"] += 1
                stats["tag_write_failed_files"].append({
                    "file":   m["file"],
                    "ext":    Path(m["file"]).suffix.lower(),
                    "reason": reason or "unknown",
                })
                self.log.emit(
                    f"  ✘ Write failed — {os.path.basename(m['file'])} "
                    f"({Path(m['file']).suffix.lower()}): {reason}"
                )
            elif reason:
                # Success via fallback — log the format mismatch warning
                self.log.emit(
                    f"  ⚠ {os.path.basename(m['file'])}: {reason}"
                )
            return ok

        for idx, fp_data in enumerate(all_work, 1):
            if self._stop_event.is_set():
                self.log.emit("⚠  Lookup cancelled by user.")
                stats["cancelled"] = True
                break

            self.lookup_progress.emit(idx, total_fp)
            filename = os.path.basename(fp_data["file"])
            outcome  = fp_data.get("outcome")

            # ── Cache hit: tags already written on a previous run ────────────
            if outcome == _OUTCOME_CACHED:
                score_pct = f"{fp_data.get('score', 1.0) * 100:.1f}%"
                stats["tagged"] += 1
                self.result.emit(filename, "cached", score_pct)
                continue

            # ── Live lookup ──────────────────────────────────────────────────
            match   = AcoustIDEngine.lookup_fingerprint(fp_data, self.api_key, bucket)
            outcome = match.get("outcome", _OUTCOME_API_ERROR)

            if outcome == _OUTCOME_API_ERROR:
                stats["api_errors"] += 1
                stats["error_files"].append(fp_data["file"])
                error_entries.append(match)
                self.result.emit(filename, "error", "—")
                self.log.emit(
                    f"  [{idx:,}/{total_fp:,}] ✘ {filename} — "
                    f"API error: {match.get('error_detail', 'unknown')}"
                )
                continue

            if outcome == _OUTCOME_NO_MATCH:
                stats["no_match"] += 1
                self.result.emit(filename, "no_match", "—")
                if idx % 50 == 0:
                    self.log.emit(f"  [{idx:,}/{total_fp:,}] {filename} → no match")
                continue

            if outcome == _OUTCOME_NO_META:
                # Fingerprint matched acoustically but the DB has no artist/title.
                # Attempt a partial write: fill in album, year, or mbid if the
                # file doesn't already have them, without touching any existing tags.
                stats["no_metadata"] += 1
                score_pct = f"{match.get('score', 0) * 100:.1f}%"
                has_any_partial = any(match.get(k) for k in ("album", "year", "mbid"))
                if has_any_partial:
                    partial_ok, _ = AcoustIDEngine.write_tags(match, partial=True)
                    if partial_ok:
                        self.result.emit(filename, "partial", score_pct)
                        if idx % 50 == 0:
                            extras = ", ".join(
                                k for k in ("album", "year", "mbid") if match.get(k)
                            )
                            self.log.emit(
                                f"  [{idx:,}/{total_fp:,}] ~ {filename} — "
                                f"no artist/title in DB ({score_pct}), "
                                f"wrote supplemental: {extras}"
                            )
                    else:
                        self.result.emit(filename, "no_match", "—")
                        if idx % 50 == 0:
                            self.log.emit(
                                f"  [{idx:,}/{total_fp:,}] {filename} — "
                                f"no artist/title in DB, partial write failed"
                            )
                else:
                    self.result.emit(filename, "no_match", "—")
                    if idx % 50 == 0:
                        self.log.emit(
                            f"  [{idx:,}/{total_fp:,}] {filename} → "
                            f"no artist/title in DB ({score_pct})"
                        )
                continue

            score     = match["score"]
            score_pct = f"{score * 100:.1f}%"

            if score >= self.strong_thresh:
                ok = _do_write(match)
                if ok:
                    stats["tagged"] += 1
                    self.result.emit(filename, "tagged", score_pct)
                    if idx % 50 == 0:
                        self.log.emit(
                            f"  [{idx:,}/{total_fp:,}] ✔ {filename} ({score_pct}) → "
                            f"{match.get('artist')} — {match.get('title')}"
                        )
                    if self.use_cache:
                        try:
                            cache[fp_data["file"]] = {
                                "fingerprint": fp_data.get("fingerprint", ""),
                                "duration":    fp_data.get("duration", 0),
                                "artist":      match.get("artist", ""),
                                "title":       match.get("title", ""),
                                "album":       match.get("album", ""),
                                "year":        match.get("year", ""),
                                "mbid":        match.get("mbid", ""),
                                "score":       score,
                                "tagged_at":   datetime.now().isoformat(),
                                "mtime":       os.path.getmtime(fp_data["file"]),
                            }
                        except OSError:
                            pass
                else:
                    self.result.emit(filename, "error", score_pct)

            elif score >= self.medium_thresh:
                ok = _do_write(match)
                if ok:
                    stats["tagged"] += 1
                    low_confidence.append(match)
                    self.result.emit(filename, "tagged", score_pct)
                    if self.use_cache:
                        try:
                            cache[fp_data["file"]] = {
                                "fingerprint": fp_data.get("fingerprint", ""),
                                "duration":    fp_data.get("duration", 0),
                                "artist":      match.get("artist", ""),
                                "title":       match.get("title", ""),
                                "album":       match.get("album", ""),
                                "year":        match.get("year", ""),
                                "mbid":        match.get("mbid", ""),
                                "score":       score,
                                "tagged_at":   datetime.now().isoformat(),
                                "mtime":       os.path.getmtime(fp_data["file"]),
                            }
                        except OSError:
                            pass
                else:
                    self.result.emit(filename, "error", score_pct)

            else:
                stats["skipped_low_confidence"] += 1
                low_confidence.append(match)
                self.result.emit(filename, "skipped", score_pct)
                if idx % 50 == 0:
                    self.log.emit(
                        f"  [{idx:,}/{total_fp:,}] ✘ {filename} "
                        f"({score_pct}) — below threshold, skipped"
                    )

        # ── Persist cache ────────────────────────────────────────────────────
        if self.use_cache and cache:
            AcoustIDEngine.save_cache(cache)

        # ── Stage 3: Reports ─────────────────────────────────────────────────
        if low_confidence:
            txt_path, json_path = AcoustIDEngine.write_low_confidence_report(
                low_confidence, cutoff=self.medium_thresh,
            )
            stats["low_confidence_report"] = txt_path
            stats["low_confidence_json"]   = json_path
            if txt_path:
                self.log.emit(f"\n📋 Low-confidence report (.txt): {txt_path}")
            if json_path:
                self.log.emit(f"📋 Low-confidence batch (.json — load in UI): {json_path}")

        if stats["tag_write_failed_files"]:
            fail_report = AcoustIDEngine.write_tag_failure_report(
                stats["tag_write_failed_files"]
            )
            stats["write_failure_report"] = fail_report
            if fail_report:
                self.log.emit(f"📋 Tag write failure report: {fail_report}")

        if error_entries:
            error_report_path = AcoustIDEngine.write_error_report(
                error_entries, rps_used=self.rps,
            )
            stats["error_report"] = error_report_path
            if error_report_path:
                self.log.emit(f"📋 API error report: {error_report_path}")
            tip = (
                f"If the server was busy, try reducing the rate limit in Settings "
                f"(currently {self.rps:.0f} RPS) and use RETRY ERRORS."
                if self.rps >= RATE_LIMIT_RPS_MAX else
                f"Server may be temporarily unavailable at {self.rps:.1f} RPS. "
                "Use RETRY ERRORS to try again later."
            )
            self.log.emit(f"\n⚠  {stats['api_errors']:,} file(s) returned API errors. {tip}")

        summary_path = AcoustIDEngine.write_summary_report(stats)
        stats["summary_report"] = summary_path

        # ── Final summary ────────────────────────────────────────────────────
        write_fail_line = (
            f"  Write failures:        {stats['tag_write_failed']:,}  "
            f"← see tag_write_failures_*.txt\n"
            if stats["tag_write_failed"] else ""
        )
        retry_line = (
            f"  API errors (retry):    {stats['api_errors']:,}\n"
            if stats["api_errors"] else ""
        )
        lc_count = len(low_confidence)
        lc_line  = (
            f"  Low confidence (review): {lc_count:,}  ← load .json in UI\n"
            if lc_count else ""
        )
        self.log.emit(
            f"\n{'─' * 50}\n"
            f"  {'RETRY ' if is_retry else ''}RUN COMPLETE\n"
            f"{'─' * 50}\n"
            f"  Files scanned:         {stats['total_files']:,}\n"
            + (f"  Already tagged (skip): {stats['skipped_already_tagged']:,}\n"
               if stats['skipped_already_tagged'] else "")
            + (f"  Cache hits (skipped):  {stats['cache_hits']:,}\n"
               if stats['cache_hits'] else "")
            + f"  Fingerprinted:         {stats['fingerprinted']:,}\n"
            f"  Fingerprint failures:  {stats['fingerprint_failed']:,}\n"
            f"  Tags written:          {stats['tagged']:,}\n"
            f"  Below threshold:       {stats['skipped_low_confidence']:,}\n"
            f"  No match found:        {stats['no_match']:,}\n"
            f"  Matched, no metadata:  {stats['no_metadata']:,}\n"
            f"{write_fail_line}"
            f"{lc_line}"
            f"{retry_line}"
            f"{'─' * 50}"
        )

        self.finished.emit(stats)
