"""
DJ Library Manager — Low Confidence Manager Engine
Handles post-run review of AcoustID matches that fell below the strong
threshold but still have a plausible best-match from the database.

── What this engine does ────────────────────────────────────────────────────
After an AcoustID run, any track that matched at a confidence score between
`medium_thresh` and `strong_thresh` was tagged but logged as low-confidence.
Tracks below `medium_thresh` were skipped entirely (not tagged).

Both groups end up in the low_confidence_<ts>.json batch file.

This engine provides three operations on a loaded batch:

  1. load_batch(json_path)
     Reads the JSON, validates schema, returns the entry list.
     Caller (UI page) stores this as the working dataset.

  2. apply_tags_for_entries(entries)
     Takes a list of entries where action="apply" and writes their
     AcoustID-matched tags to the actual files on disk using
     AcoustIDEngine.write_tags(). Returns (succeeded, failed) lists.

  3. quarantine_entries(entries, quarantine_root)
     Takes a list of entries where action="quarantine" and moves each
     file into:
         <quarantine_root>/_LOW_CONFIDENCE_TAGS/<relative_subdirectory>/
     The relative path is preserved so the file can be traced back.
     Returns (moved, failed) lists.

  4. save_batch(entries, json_path)
     Writes the current action state back to the JSON file so the
     user can close the app and resume the review session later.

── Relationship to other engines ────────────────────────────────────────────
This engine calls AcoustIDEngine.write_tags() for tag application.
It does NOT call LibraryCleaner.move_to_quarantine() — the low confidence
quarantine subdirectory (_LOW_CONFIDENCE_TAGS) is intentionally separate
from the main _QUARANTINE used for duplicates and health check failures,
so the two populations stay distinct and independently reversible.

── Future: audio preview + AcoustID submission ──────────────────────────────
Planned in v0.6.x Low Confidence Manager UI:
  - Preview playback of the track before approving its tags
  - Optional submission of confirmed fingerprint+tag pairs back to AcoustID
    to improve the shared database (requires Submit Key, separate from
    the Lookup Key stored in settings)
────────────────────────────────────────────────────────────────────────────
"""

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/acoustid_engine.py  →  AcoustIDEngine.write_tags()
# Why: write_tags() is the single authoritative multi-format tag writer.
#      Duplicating it here would create a second diverging implementation.
# If you modify write_tags() signature or return type (bool, reason),
# update the _apply_one() method in this file accordingly.
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    from .acoustid_engine import AcoustIDEngine
except ImportError:
    try:
        from acoustid_engine import AcoustIDEngine
    except ImportError:
        AcoustIDEngine = None  # write_tags will be unavailable — caller should check


# ── VALID ACTION VALUES ───────────────────────────────────────────────────────
ACTION_APPLY      = "apply"        # Write the matched tags to the file
ACTION_QUARANTINE = "quarantine"   # Move to _LOW_CONFIDENCE_TAGS for later
ACTION_SKIP       = "skip"         # Do nothing, leave file as-is
ACTION_NONE       = None           # Not yet reviewed

VALID_ACTIONS = {ACTION_APPLY, ACTION_QUARANTINE, ACTION_SKIP, ACTION_NONE}

# Subfolder name inside the quarantine root
LOW_CONFIDENCE_SUBDIR = "_LOW_CONFIDENCE_TAGS"


class LowConfidenceManager:
    """
    Stateless manager — all methods are static.
    State (the loaded entry list) lives in the UI page.
    """

    # ── Batch loading ────────────────────────────────────────────────────────

    @staticmethod
    def load_batch(json_path: str) -> tuple[list, dict]:
        """
        Load a low_confidence_<ts>.json batch file written by AcoustIDEngine.

        Returns:
            (entries, metadata)
            entries  — list of entry dicts, each with keys:
                        file, score, artist, title, album, year, mbid, action
            metadata — dict with keys: schema_version, generated, cutoff, count

        Raises ValueError if the file is not a valid low confidence batch.
        Raises FileNotFoundError if the path does not exist.
        """
        path = Path(json_path)
        if not path.exists():
            raise FileNotFoundError(f"Batch file not found: {json_path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data.get("schema_version") != 1:
            raise ValueError(
                f"Unsupported schema_version: {data.get('schema_version')}. "
                "This file may have been produced by a newer version of the app."
            )

        entries  = data.get("entries", [])
        metadata = {
            "schema_version": data.get("schema_version"),
            "generated":      data.get("generated", ""),
            "cutoff":         data.get("cutoff"),
            "count":          data.get("count", len(entries)),
        }

        # Validate and normalise action field on each entry
        for entry in entries:
            if entry.get("action") not in VALID_ACTIONS:
                entry["action"] = None

        return entries, metadata

    # ── Batch persistence ────────────────────────────────────────────────────

    @staticmethod
    def save_batch(entries: list, json_path: str,
                   metadata: dict = None) -> bool:
        """
        Write the current action state back to the batch JSON file.
        Call this whenever the user changes an action so progress is not
        lost if the app is closed before the session is finalised.

        Returns True on success, False on failure.
        """
        try:
            existing = {}
            path = Path(json_path)
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)

            batch = {
                "schema_version": 1,
                "generated":      existing.get("generated", ""),
                "cutoff":         existing.get("cutoff"),
                "count":          len(entries),
                "entries":        entries,
            }
            if metadata:
                batch.update({k: v for k, v in metadata.items()
                               if k not in ("count", "entries")})

            with open(path, "w", encoding="utf-8") as f:
                json.dump(batch, f, indent=2, ensure_ascii=False)
            return True
        except Exception:
            return False

    # ── Batch filtering helpers ───────────────────────────────────────────────

    @staticmethod
    def entries_by_action(entries: list, action) -> list:
        """Return entries whose action field matches the given value."""
        return [e for e in entries if e.get("action") == action]

    @staticmethod
    def unreviewed_count(entries: list) -> int:
        """Return the number of entries that have not yet been given an action."""
        return sum(1 for e in entries if e.get("action") is None)

    # ── Tag application ───────────────────────────────────────────────────────

    @staticmethod
    def apply_tags_for_entries(
        entries: list,
        progress_callback=None,
    ) -> tuple[list, list]:
        """
        Write AcoustID-matched tags for every entry where action="apply".

        progress_callback(current, total, filename) is called after each file.

        Returns:
            succeeded — list of entry dicts that were written successfully
            failed    — list of {entry, reason} dicts for files that failed

        Only entries with action=ACTION_APPLY are processed. Others are silently
        skipped so the caller can pass the full entry list without pre-filtering.
        """
        if AcoustIDEngine is None:
            return [], [{"entry": e, "reason": "AcoustIDEngine not available"}
                        for e in entries if e.get("action") == ACTION_APPLY]

        to_apply  = [e for e in entries if e.get("action") == ACTION_APPLY]
        succeeded = []
        failed    = []
        total     = len(to_apply)

        for idx, entry in enumerate(to_apply, 1):
            filename = os.path.basename(entry["file"])
            if not Path(entry["file"]).exists():
                failed.append({"entry": entry, "reason": "File not found on disk"})
            else:
                ok, reason = AcoustIDEngine.write_tags(entry)
                if ok:
                    if reason:
                        # Success via fallback (e.g. .wma that was not actually ASF).
                        # Store the warning note on the entry so the session report
                        # can surface the format mismatch to the user.
                        entry["write_warning"] = reason
                    succeeded.append(entry)
                else:
                    failed.append({"entry": entry, "reason": reason or "unknown"})

            if progress_callback:
                progress_callback(idx, total, filename)

        return succeeded, failed

    # ── Quarantine ────────────────────────────────────────────────────────────

    @staticmethod
    def quarantine_entries(
        entries: list,
        quarantine_root: str,
        source_root: str = None,
        progress_callback=None,
    ) -> tuple[list, list]:
        """
        Move files where action="quarantine" to:
            <quarantine_root>/_LOW_CONFIDENCE_TAGS/<relative_path>/

        Relative path is computed against `source_root` if provided; falls back
        to preserving just the filename if the file is not under source_root.

        The source file is moved (not copied) so the original location is
        vacated. An empty parent directory is NOT removed — that is the
        responsibility of a future cleanup pass, not this operation.

        progress_callback(current, total, filename) called after each move.

        Returns:
            moved  — list of {entry, dest} dicts for successful moves
            failed — list of {entry, reason} dicts for failures
        """
        to_quarantine = [e for e in entries if e.get("action") == ACTION_QUARANTINE]
        moved         = []
        failed        = []
        total         = len(to_quarantine)
        q_base        = Path(quarantine_root) / LOW_CONFIDENCE_SUBDIR

        for idx, entry in enumerate(to_quarantine, 1):
            src      = Path(entry["file"])
            filename = src.name

            if not src.exists():
                failed.append({"entry": entry, "reason": "File not found on disk"})
                if progress_callback:
                    progress_callback(idx, total, filename)
                continue

            # Compute destination preserving relative subdirectory
            try:
                if source_root:
                    rel = src.relative_to(source_root)
                else:
                    rel = Path(src.name)
            except ValueError:
                # File is not under source_root — use filename only
                rel = Path(src.name)

            dest = q_base / rel

            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                # If a file already exists at dest, suffix with a counter
                if dest.exists():
                    stem, suffix = dest.stem, dest.suffix
                    counter      = 1
                    while dest.exists():
                        dest = dest.parent / f"{stem}_{counter}{suffix}"
                        counter += 1
                shutil.move(str(src), str(dest))
                moved.append({"entry": entry, "dest": str(dest)})
            except Exception as exc:
                failed.append({"entry": entry, "reason": f"{type(exc).__name__}: {exc}"})

            if progress_callback:
                progress_callback(idx, total, filename)

        return moved, failed

    # ── Reporting ─────────────────────────────────────────────────────────────

    @staticmethod
    def write_session_report(
        succeeded_apply: list,
        failed_apply: list,
        moved: list,
        failed_quarantine: list,
        skipped: list,
        out_dir: str = None,
    ) -> str | None:
        """
        Write a human-readable summary of the Low Confidence Manager session.
        Called after the user has finalised all actions and the operations
        have been executed.

        Returns path to the report file, or None on failure.
        """
        if out_dir is None:
            out_dir = str(Path.home() / ".dj_library_manager" / "logs" / "tagging")

        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            ts    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fpath = Path(out_dir) / f"low_confidence_session_{ts}.txt"

            with open(fpath, "w", encoding="utf-8") as f:
                f.write("# Low Confidence Manager Session Report\n")
                f.write(f"# Generated: {ts} UTC\n\n")
                f.write(f"  Tags applied:         {len(succeeded_apply):,}\n")
                f.write(f"  Apply failed:         {len(failed_apply):,}\n")
                f.write(f"  Moved to quarantine:  {len(moved):,}\n")
                f.write(f"  Quarantine failed:    {len(failed_quarantine):,}\n")
                f.write(f"  Skipped (no action):  {len(skipped):,}\n\n")

                if succeeded_apply:
                    f.write("─── TAGS APPLIED ───────────────────────────────\n")
                    for e in succeeded_apply:
                        f.write(f"  ✔  {os.path.basename(e['file'])}\n")
                        f.write(f"       {e.get('artist', '—')} — {e.get('title', '—')}\n")
                        if e.get("write_warning"):
                            f.write(f"       ⚠ {e['write_warning']}\n")
                    f.write("\n")

                if failed_apply:
                    f.write("─── APPLY FAILURES ─────────────────────────────\n")
                    for item in failed_apply:
                        f.write(f"  ✘  {os.path.basename(item['entry']['file'])}\n")
                        f.write(f"       Reason: {item['reason']}\n")
                    f.write("\n")

                if moved:
                    f.write("─── QUARANTINED ────────────────────────────────\n")
                    for item in moved:
                        f.write(f"  →  {os.path.basename(item['entry']['file'])}\n")
                        f.write(f"       Dest: {item['dest']}\n")
                    f.write("\n")

                if failed_quarantine:
                    f.write("─── QUARANTINE FAILURES ────────────────────────\n")
                    for item in failed_quarantine:
                        f.write(f"  ✘  {os.path.basename(item['entry']['file'])}\n")
                        f.write(f"       Reason: {item['reason']}\n")
                    f.write("\n")

            return str(fpath)
        except Exception:
            return None
