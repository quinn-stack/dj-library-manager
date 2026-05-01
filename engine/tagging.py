"""Tag-based filename renaming utilities.

Provides a safe, best-effort rename of audio files to the form:
    Artist Name - Track Title.ext

The goal is to avoid catastrophic mis-tags: we only rename when both
artist and title tags are present and look reasonable. The functions
support a dry-run mode that returns a list of (orig, dest) pairs without
performing filesystem changes.
"""
from pathlib import Path
from typing import List, Tuple
import os
import re
import json
from datetime import datetime
from pathlib import Path as _Path

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/library_clean.py  →  DEFAULT_AUDIO_EXTS
# Why: DEFAULT_AUDIO_EXTS is the single shared source-of-truth for which file
#      extensions count as audio across all engines. Defining a local copy here
#      would silently diverge if library_clean.py is updated.
# If you add or remove extensions in library_clean.DEFAULT_AUDIO_EXTS, verify
# that rename_files_to_tags() still handles all intended formats correctly.
# ─────────────────────────────────────────────────────────────────────────────

# ── USED BY OTHER ENGINES / UI ───────────────────────────────────────────────
# The following functions are imported by:
#   - ui/tag_finder_page.py  →  rename_files_to_tags(), apply_renames(),
#                                revert_from_report()
# If you change the signature or return type of any of these functions, update
# tag_finder_page.py accordingly and test the full rename → undo flow.
# Note: apply_renames() and revert_from_report() both return a
# (results_list, report_path_or_none) tuple — callers depend on this contract.
# Do not revert to returning a bare list.
# ─────────────────────────────────────────────────────────────────────────────
from .library_clean import DEFAULT_AUDIO_EXTS

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/tag_utils.py  →  looks_reasonable()
# Why: looks_reasonable() is the single authoritative check for whether a tag
#      value is real metadata or a placeholder. Previously duplicated here and
#      in duplicate_finder.py — extracted to tag_utils.py at v0.5.3 so both
#      engines share one definition.
# If you change the placeholder set or matching logic in tag_utils.py, verify
# that rename_files_to_tags() still correctly skips untagged files.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .tag_utils import looks_reasonable as _looks_reasonable
except ImportError:
    from tag_utils import looks_reasonable as _looks_reasonable


def _safe_text(s: str) -> str:
    if not s:
        return ""
    # remove problematic filesystem characters
    s = s.strip()
    s = re.sub(r"[\\/:*?\"<>|]\n?", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _get_tag_vals(mfile) -> Tuple[str, str]:
    """Return (artist, title) from a Mutagen file object or (None, None)."""
    if mfile is None:
        return (None, None)
    # try common keys
    tags = getattr(mfile, "tags", None)
    if not tags:
        return (None, None)

    def first(keys):
        for k in keys:
            v = tags.get(k)
            if v:
                # mutagen frames may return list-like or objects
                try:
                    if isinstance(v, (list, tuple)):
                        return str(v[0])
                    return str(v)
                except Exception:
                    try:
                        return str(v[0])
                    except Exception:
                        return str(v)
        return None

    artist = first(["artist", "ARTIST", "TPE1"])
    title = first(["title", "TITLE", "TIT2"])
    return (artist, title)


def rename_files_to_tags(
    root: str,
    dry_run: bool = True,
    progress_cb=None,
    stop_event=None,
) -> List[Tuple[str, str]]:
    """Walk `root`, read tags, and rename files to 'Artist - Title.ext'.

    Args:
        root:        Root directory to walk recursively.
        dry_run:     If True, return planned pairs without touching the FS.
        progress_cb: Optional callable(current, total, filename) emitted
                     once per file processed (after tag read, before append).
                     ``total`` is 0 during the initial walk phase while the
                     file count is still unknown; set to the real count once
                     all filenames are collected.
        stop_event:  Optional threading.Event. When set, the function returns
                     the partial results collected so far at the next file
                     boundary. Checked during both the walk and tag-read phases.
    """
    moved = []
    skipped = []
    global MutagenFile
    if MutagenFile is None:
        try:
            from mutagen import File as MutagenFile  # type: ignore
        except Exception:
            MutagenFile = None
    if MutagenFile is None:
        return moved

    p = Path(root)
    if not p.exists():
        return moved

    # ── Collect audio files first so we know total for progress reporting ──
    all_files  = []
    dir_count  = 0
    for dirpath, _, filenames in os.walk(p):
        dir_count += 1
        if stop_event is not None and dir_count % 200 == 0 and stop_event.is_set():
            return moved   # empty list — cancelled during walk phase
        for fn in filenames:
            src = Path(dirpath) / fn
            if src.suffix.lower().lstrip('.') in DEFAULT_AUDIO_EXTS:
                all_files.append(src)

    if stop_event is not None and stop_event.is_set():
        return moved

    total = len(all_files)

    from mutagen.id3 import ID3, ID3NoHeaderError

    for idx, src in enumerate(all_files, 1):
        if stop_event is not None and stop_event.is_set():
            break   # return partial results collected so far
        fn = src.name
        if progress_cb is not None:
            progress_cb(idx, total, fn)

        try:
            try:
                id3 = ID3(str(src))
            except ID3NoHeaderError:
                id3 = None
            if id3 is not None:
                a = id3.get('TPE1')
                t = id3.get('TIT2')
                artist = str(a.text[0]) if a is not None and getattr(a, 'text', None) else None
                title  = str(t.text[0]) if t is not None and getattr(t, 'text', None) else None
            else:
                # fallback: try mutagen generic reader
                m = MutagenFile(str(src), easy=True)
                if m:
                    artist, title = _get_tag_vals(m)
                else:
                    artist = title = None
        except Exception:
            artist = title = None

        artist = _safe_text(artist) if artist else None
        title = _safe_text(title) if title else None

        if not (_looks_reasonable(artist) and _looks_reasonable(title)):
            skipped.append(f"{fn} | artist={repr(artist)} title={repr(title)}")
            continue

        new_name = f"{artist} - {title}{src.suffix}"
        new_name = re.sub(r"\s+", " ", new_name).strip()
        dest = src.with_name(new_name)
        counter = 1
        final_dest = dest
        while final_dest.exists() and final_dest != src:
            final_dest = dest.with_name(f"{dest.stem} ({counter}){dest.suffix}")
            counter += 1

        moved.append((str(src), str(final_dest)))
        if not dry_run:
            try:
                os.rename(str(src), str(final_dest))
            except Exception:
                try:
                    import shutil
                    shutil.move(str(src), str(final_dest))
                except Exception:
                    moved.pop()
                    continue

    # Write diagnostic log
    try:
        log_dir = _Path.home() / ".dj_library_manager" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        diag_path = log_dir / f"rename_scan_diag_{ts}.txt"
        with open(diag_path, "w", encoding="utf-8") as f:
            f.write(f"rename_files_to_tags: root={root}  dry_run={dry_run}\n")
            f.write(f"candidates found: {len(moved)}\n")
            f.write(f"skipped (bad/missing tags): {len(skipped)}\n\n")
            if skipped:
                f.write("--- SKIPPED (tag read failure or unreasonable tags) ---\n")
                for s in skipped:
                    f.write(f"  {s}\n")
            f.write("\n--- CANDIDATES ---\n")
            for o, d in moved:
                f.write(f"  {Path(o).name}  ->  {Path(d).name}\n")
    except Exception:
        pass

    return moved


def apply_renames(
    pairs: List[Tuple[str, str]],
    dry_run: bool = True,
    progress_cb=None,
    stop_event=None,
) -> List[Tuple[str, str]]:
    """Apply or simulate renames for the provided (orig, dest) pairs.

    Args:
        pairs:       List of (orig_path, dest_path) tuples.
        dry_run:     If True, return planned pairs without touching the FS.
        progress_cb: Optional callable(current, total, filename) emitted
                     once per pair processed.
        stop_event:  Optional threading.Event. When set, returns partial
                     results at the next file boundary. Already-executed
                     renames are included in the partial report.
    """
    results = []
    import shutil

    total = len(pairs)
    for idx, (orig, dest) in enumerate(pairs, 1):
        if stop_event is not None and stop_event.is_set():
            break   # return partial results; already-done renames are in results
        if progress_cb is not None:
            progress_cb(idx, total, Path(orig).name)
        try:
            src = Path(orig)
            dest_p = Path(dest)

            if not src.exists():
                continue

            if src == dest_p:
                continue

            # avoid overwriting: compute a safe final_dest
            final = dest_p
            counter = 1
            while final.exists() and final != src:
                final = dest_p.with_name(f"{dest_p.stem} ({counter}){dest_p.suffix}")
                counter += 1

            if dry_run:
                results.append((str(src), str(final)))
                continue

            final.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.rename(str(src), str(final))
            except Exception:
                try:
                    shutil.move(str(src), str(final))
                except Exception:
                    continue
            results.append((str(src), str(final)))
        except Exception:
            continue



    # If we actually performed renames, write an undo/report file
    report_path = None
    if not dry_run and results:
        try:
            report_path = _write_rename_report(results)
        except Exception:
            report_path = None

    # Always return a (results, report_path) tuple so callers get a consistent
    # type. A bare list was previously returned when no report was written,
    # which caused PySide6 Signal(object) to mishandle the return value and
    # silently drop it — making successful renames appear to have done nothing.
    return (results, report_path)


def _write_rename_report(results: List[Tuple[str, str]]) -> str:
    """Write a timestamped JSON report of performed renames and return path."""
    try:
        out_dir = _Path.home() / ".dj_library_manager" / "logs"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"rename_report_{ts}.json"
        path = out_dir / fname
        payload = {
            "timestamp": ts,
            "renames": [{"orig": o, "dest": d} for o, d in results],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return str(path)
    except Exception:
        return None


def revert_from_report(report_path: str, dry_run: bool = False):
    """Revert renames recorded in a JSON report file.

    Always returns a (results_list, report_path_or_none) tuple, matching
    the consistent return type of apply_renames.
    """
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return ([], None)

    entries = data.get("renames", [])
    pairs = []
    for e in entries:
        orig = e.get("orig")
        dest = e.get("dest")
        if not orig or not dest:
            continue
        # We want to move current dest back to orig, but only if dest exists
        if Path(dest).exists():
            pairs.append((str(dest), str(orig)))

    if not pairs:
        return ([], None)

    return apply_renames(pairs, dry_run=dry_run)
