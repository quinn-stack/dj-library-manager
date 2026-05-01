"""
DJ Library Manager — Transfer Engine  (v0.5.0)

Safe, hash-verified file transfer for first-transfer and (v0.5.2) incremental
backup use cases. Replaces ad-hoc rsync invocations.

Design principles:
  - Dry run is always available and should be enforced by the UI before any
    live run. The engine itself does not enforce this — the UI must disable
    the live-run button until a dry run has completed successfully.
  - Every file copy is SHA256-verified against the source before the result
    is logged as successful. A copy that fails verification is logged as
    FAILED and the destination file is removed.
  - No file is silently skipped. Every outcome (COPIED, SKIPPED, COLLISION,
    FAILED, PATH_REFUSED) appears in the per-file result list and the report.
  - OS path guards use PlatformAdapter — never put OS conditionals here directly.
  - All operations are synchronous and designed to run inside a TaskRunner
    (QThread). Progress is reported via an optional callback, not Qt signals,
    so this module has no Qt dependency.

Collision handling modes (user-configurable per run):
  SKIP      — leave destination untouched, log as SKIPPED
  RENAME    — append _1, _2 … until a free name is found, copy to that name
  OVERWRITE — overwrite the destination (destructive — confirm in UI before use)

Outcome codes (TransferResult.outcome):
  COPIED        — file copied and hash verified
  SKIPPED       — destination already exists and mode is SKIP, or dry run
  COLLISION     — destination exists, mode is RENAME (new name recorded)
  OVERWRITTEN   — destination overwritten (OVERWRITE mode)
  FAILED        — copy or verification failure (reason recorded)
  PATH_REFUSED  — source or destination path exceeds OS limit (Windows ≤260)
  PATH_WARNED   — path exceeds advisory limit (macOS ≤1024) but copy proceeded
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, List, Optional

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/hash_utils.py  →  sha256_file(), sha256_matches()
# Why: sha256_file() is the single implementation for chunk-based file hashing.
#      Duplicating it here would risk the two implementations diverging.
# If you change sha256_file() behaviour or return type in hash_utils.py,
# verify that _verify_copy() below still works correctly.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .hash_utils import sha256_file, sha256_matches
except ImportError:
    from hash_utils import sha256_file, sha256_matches

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/platform_adapter.py  →  PlatformAdapter.get_path_limit(),
#             PlatformAdapter.get_os()
# Why: OS path limits are the single source of truth in PlatformAdapter.
#      Hardcoding 260 / 1024 here would duplicate logic and risk divergence.
# If you change _PATH_LIMITS or get_path_limit() in platform_adapter.py,
# verify that _check_path() below still refuses / warns correctly.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .platform_adapter import PlatformAdapter
except ImportError:
    from platform_adapter import PlatformAdapter

# ── USED BY OTHER ENGINES ────────────────────────────────────────────────────
# TransferEngine and TransferResult are imported by:
#   - engine/restructure_engine.py  →  all file move operations (v0.5.1)
# If you change TransferResult fields, CollisionMode values, or
# TransferEngine.run_transfer() / dry_run() signatures, update
# restructure_engine.py accordingly.
# ─────────────────────────────────────────────────────────────────────────────


# ---------------------------------------------------------------------------
# Public enums and data classes
# ---------------------------------------------------------------------------

class CollisionMode(str, Enum):
    """What to do when the destination file already exists."""
    SKIP      = "skip"
    RENAME    = "rename"
    OVERWRITE = "overwrite"


# ── System file / directory exclusions ───────────────────────────────────────
# Platform-agnostic set of directory names that must never be transferred.
# Matching is case-insensitive and exact (full directory name, not substring).
#
# Covers: Linux trash (.Trash-*), macOS metadata, Windows recycle bin /
# internals, source-control dirs, and misc ext-formatted drive artifacts.
_SYSTEM_DIR_NAMES: frozenset = frozenset({
    # Recycle bins / trash — Linux, macOS, Windows, Android
    ".trash", ".trashes",
    ".trash-1000", ".trash-100", ".trash-0", ".trash-999",
    "$recycle.bin", "recycler", "recycled",
    # macOS filesystem metadata
    ".spotlight-v100", ".fseventsd", ".temporaryitems",
    ".documentrevisions-v100", ".pkinstallsandboxmanager",
    # Windows internals
    "system volume information",
    # Source control (never belongs on a music drive)
    ".git", ".svn", ".hg",
    # Misc seen on ext/exfat-formatted portable drives
    ".android_secure", "lost+found",
})

# Individual filenames (case-insensitive) always excluded regardless of location.
_SYSTEM_FILE_NAMES: frozenset = frozenset({
    ".ds_store", ".localized",                  # macOS
    "desktop.ini", "thumbs.db",                 # Windows
    "thumbs.db:encryptable", "zone.identifier", # Windows NTFS streams
})


class Outcome(str, Enum):
    COPIED       = "COPIED"
    SKIPPED      = "SKIPPED"
    COLLISION    = "COLLISION"    # renamed destination recorded in dest_path
    OVERWRITTEN  = "OVERWRITTEN"
    FAILED       = "FAILED"
    PATH_REFUSED = "PATH_REFUSED"
    PATH_WARNED  = "PATH_WARNED"  # warning attached, copy still proceeded


@dataclass
class TransferResult:
    """Outcome record for a single file transfer."""
    src_path:    str
    dest_path:   str                   # final destination (may differ from planned on RENAME)
    outcome:     Outcome
    reason:      Optional[str] = None  # failure / skip / path-guard message
    src_hash:    Optional[str] = None  # SHA256 of source (populated on COPIED/OVERWRITTEN)
    dest_hash:   Optional[str] = None  # SHA256 of destination after copy (same as src on success)
    size_bytes:  Optional[int] = None  # source file size


@dataclass
class TransferReport:
    """Aggregate report for a complete transfer run."""
    timestamp:    str
    source_root:  str
    dest_root:    str
    dry_run:      bool
    collision_mode: str
    results:      List[TransferResult] = field(default_factory=list)

    # Computed counts — call finalise() after all results are appended.
    total:        int = 0
    copied:       int = 0
    skipped:      int = 0
    collisions:   int = 0
    overwritten:  int = 0
    failed:       int = 0
    refused:      int = 0
    warned:       int = 0
    bytes_copied: int = 0

    def finalise(self) -> None:
        """Recompute aggregate counts from results list."""
        self.total       = len(self.results)
        self.copied      = sum(1 for r in self.results if r.outcome == Outcome.COPIED)
        self.skipped     = sum(1 for r in self.results if r.outcome == Outcome.SKIPPED)
        self.collisions  = sum(1 for r in self.results if r.outcome == Outcome.COLLISION)
        self.overwritten = sum(1 for r in self.results if r.outcome == Outcome.OVERWRITTEN)
        self.failed      = sum(1 for r in self.results if r.outcome == Outcome.FAILED)
        self.refused     = sum(1 for r in self.results if r.outcome == Outcome.PATH_REFUSED)
        self.warned      = sum(1 for r in self.results if r.outcome == Outcome.PATH_WARNED)
        self.bytes_copied = sum(
            r.size_bytes or 0 for r in self.results
            if r.outcome in (Outcome.COPIED, Outcome.OVERWRITTEN, Outcome.PATH_WARNED)
        )


# ---------------------------------------------------------------------------
# Internal path guard helpers
# ---------------------------------------------------------------------------

_WINDOWS_HARD_LIMIT  = 260
_MACOS_WARN_LIMIT    = 1024


def _check_path(path: str) -> tuple[bool, Optional[str]]:
    """Check `path` against OS path limits.

    Returns (ok_to_proceed, warning_or_error_message).
    If ok_to_proceed is False the transfer must not continue for this file.
    If ok_to_proceed is True but message is not None, a PATH_WARNED outcome
    should be attached (copy proceeds with a warning note).
    """
    os_name = PlatformAdapter.get_os()
    plen    = len(path)

    if os_name == "Windows" and plen > _WINDOWS_HARD_LIMIT:
        return False, (
            f"Path length {plen} exceeds Windows hard limit of {_WINDOWS_HARD_LIMIT}. "
            f"Shorten the path or enable long-path support in the Windows registry."
        )

    if os_name == "Darwin" and plen > _MACOS_WARN_LIMIT:
        return True, (
            f"Path length {plen} exceeds macOS advisory limit of {_MACOS_WARN_LIMIT}. "
            f"Copy proceeded but this path may cause issues on some filesystems."
        )

    return True, None


# ---------------------------------------------------------------------------
# Internal copy helpers
# ---------------------------------------------------------------------------

def _resolve_collision(dest: Path, mode: CollisionMode) -> tuple[Optional[Path], Optional[str]]:
    """Return (final_dest_path, skip_reason).

    If skip_reason is not None, the file should be logged as SKIPPED and
    the copy should not proceed.
    """
    if not dest.exists():
        return dest, None

    if mode == CollisionMode.SKIP:
        return None, f"Destination already exists: {dest}"

    if mode == CollisionMode.OVERWRITE:
        return dest, None  # caller will overwrite

    # RENAME — find a free name
    # ⚠ INTER-ENGINE NAMING CONVENTION — DO NOT CHANGE THIS FORMAT WITHOUT
    # UPDATING duplicate_finder.py:
    # The suffix format here is `_N` (underscore + integer, e.g. Track_1.mp3).
    # duplicate_finder._normalise() strips this exact pattern via _TRANSFER_SUFFIX_RE
    # so that collision-renamed copies are correctly identified as duplicates.
    # If you change the suffix format (e.g. to " (1)" or "-1"), update
    # _TRANSFER_SUFFIX_RE in duplicate_finder.py and the naming convention table
    # in the roadmap. See "Inter-Engine Naming Convention Contract".
    stem    = dest.stem
    suffix  = dest.suffix
    parent  = dest.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate, None
        counter += 1
        if counter > 9999:
            return None, f"Could not find a free rename slot for: {dest}"


def _do_copy(src: Path, dest: Path) -> None:
    """Copy `src` to `dest`, creating parent directories as needed.

    Uses shutil.copy2 to preserve metadata (mtime etc.).
    Raises on any error — caller logs and records FAILED.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src), str(dest))


def _remove_failed_dest(dest: Path) -> None:
    """Best-effort removal of a partially written destination file."""
    try:
        if dest.exists():
            dest.unlink()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class TransferEngine:
    """Hash-verified file transfer engine.

    Usage (from a TaskRunner or background thread):

        engine = TransferEngine(
            source_root    = "/mnt/Music",
            dest_root      = "/mnt/Backup/Music",
            collision_mode = CollisionMode.RENAME,
            verify_hash    = True,
            progress_cb    = lambda current, total, path: ...,
        )

        # Always dry-run first — UI should enforce this.
        report = engine.dry_run()
        # ... show report to user ...

        # User confirms → live run
        report = engine.run_transfer()
    """

    def __init__(
        self,
        source_root:    str,
        dest_root:      str,
        collision_mode: CollisionMode = CollisionMode.SKIP,
        verify_hash:    bool          = True,
        progress_cb:    Optional[Callable[[int, int, str], None]] = None,
        hash_cb:        Optional[Callable[[bool, str], None]] = None,
        stage_cb:       Optional[Callable[[str], None]] = None,
        stop_event=None,
    ):
        self.source_root    = Path(source_root)
        self.dest_root      = Path(dest_root)
        self.collision_mode = collision_mode
        self.verify_hash    = verify_hash
        self.progress_cb    = progress_cb  # (current_index, total_count, current_path)
        self.hash_cb        = hash_cb      # (verified_ok: bool, filename: str)
        self.stage_cb       = stage_cb     # (message: str) — prepare/scan phase updates
        # Optional threading.Event — set it to signal cooperative cancellation.
        # Checked between files in _execute(). Matches the pattern used by
        # duplicate_finder.find_duplicates(). None means no cancellation support.
        self.stop_event     = stop_event

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def dry_run(self) -> TransferReport:
        """Simulate the transfer without touching the filesystem.

        Returns a TransferReport with every planned outcome pre-calculated.
        No files are copied, created, or deleted. Destination directory is
        not created.
        """
        return self._execute(dry_run=True)

    def run_transfer(self) -> TransferReport:
        """Execute the transfer live.

        The UI must enforce that dry_run() has already been completed and
        confirmed by the user before calling this method.
        """
        return self._execute(dry_run=False)

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    def _execute(self, dry_run: bool) -> TransferReport:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report = TransferReport(
            timestamp      = ts,
            source_root    = str(self.source_root),
            dest_root      = str(self.dest_root),
            dry_run        = dry_run,
            collision_mode = self.collision_mode.value,
        )

        # Crash-safe log — written incrementally so partial results survive
        # a UI crash or forced kill. Stored alongside normal transfer reports.
        crash_log_path = None
        crash_log      = None
        try:
            log_base = Path.home() / ".dj_library_manager" / "logs" / "transfer"
            log_base.mkdir(parents=True, exist_ok=True)
            crash_log_path = log_base / f"transfer_{ts}_engine.log"
            crash_log = open(crash_log_path, "w", encoding="utf-8")
            label = "DRY RUN" if dry_run else "LIVE TRANSFER"
            crash_log.write(
                f"Transfer Engine Log — {label}\n"
                f"Started  : {ts} UTC\n"
                f"Source   : {self.source_root}\n"
                f"Dest     : {self.dest_root}\n"
                f"Collision: {self.collision_mode.value}\n"
                f"{'─' * 60}\n"
            )
            crash_log.flush()
        except Exception:
            crash_log = None

        if self.stage_cb:
            try:
                self.stage_cb("Scanning library…")
            except Exception:
                pass
        files = self._collect_files(stage_cb=self.stage_cb)
        total = len(files)
        if self.stage_cb:
            try:
                self.stage_cb(f"Preparing…  {total:,} files found")
            except Exception:
                pass

        if crash_log:
            try:
                crash_log.write(f"Files to process: {total}\n{'─' * 60}\n")
                crash_log.flush()
            except Exception:
                pass

        for idx, src in enumerate(files):
            # Cooperative cancellation — checked between files so an in-progress
            # copy is never interrupted mid-write. Matches duplicate_finder pattern.
            if self.stop_event and self.stop_event.is_set():
                if crash_log:
                    try:
                        crash_log.write(f"CANCELLED by stop_event after {idx} files\n")
                        crash_log.flush()
                    except Exception:
                        pass
                break

            if self.progress_cb:
                try:
                    self.progress_cb(idx + 1, total, str(src))
                except Exception:
                    pass

            result = self._transfer_one(src, dry_run=dry_run)
            report.results.append(result)

            if crash_log:
                try:
                    crash_log.write(
                        f"[{result.outcome.value:<13}] {result.src_path}"
                        + (f"  —  {result.reason}" if result.reason else "")
                        + "\n"
                    )
                    crash_log.flush()
                except Exception:
                    pass

        report.finalise()

        if crash_log:
            try:
                crash_log.write(
                    f"{'─' * 60}\n"
                    f"Complete : copied={report.copied} skipped={report.skipped} "
                    f"failed={report.failed} refused={report.refused}\n"
                )
                crash_log.close()
            except Exception:
                pass

        return report

    def _collect_files(self, stage_cb=None) -> List[Path]:
        """Walk source_root and return all files sorted for deterministic ordering.

        Skips:
          - Directories starting with '_QUARANTINE' (DJ Library Manager quarantine)
          - Directories in _SYSTEM_DIR_NAMES (OS trash, metadata, internals)
          - Files in _SYSTEM_FILE_NAMES (DS_Store, Thumbs.db, desktop.ini, etc.)

        Args:
            stage_cb: Optional callable(message: str) — called periodically
                      during the walk so the UI can show activity. Called with
                      a human-readable status string, e.g. "Scanning… 1,240 files".
        """
        files = []
        if not self.source_root.exists():
            return files
        dir_count = 0
        for dirpath, dirnames, filenames in os.walk(self.source_root):
            dir_count += 1
            # Prune unwanted directories in-place so os.walk never descends
            dirnames[:] = [
                d for d in dirnames
                if not d.upper().startswith("_QUARANTINE")
                and d.lower() not in _SYSTEM_DIR_NAMES
            ]
            for fn in sorted(filenames):
                if fn.lower() not in _SYSTEM_FILE_NAMES:
                    files.append(Path(dirpath) / fn)
            # Report progress every 100 directories so the UI doesn't appear frozen
            if stage_cb and dir_count % 100 == 0:
                try:
                    stage_cb(f"Scanning…  {len(files):,} files found")
                except Exception:
                    pass
        return files

    def _transfer_one(self, src: Path, dry_run: bool) -> TransferResult:
        """Process a single file. Returns a TransferResult."""
        # Compute destination path
        try:
            rel  = src.relative_to(self.source_root)
        except ValueError:
            return TransferResult(
                src_path  = str(src),
                dest_path = "",
                outcome   = Outcome.FAILED,
                reason    = f"Source file is not under source_root: {src}",
            )

        planned_dest = self.dest_root / rel

        # --- Path guard (source) ---
        ok, msg = _check_path(str(src))
        if not ok:
            return TransferResult(
                src_path  = str(src),
                dest_path = str(planned_dest),
                outcome   = Outcome.PATH_REFUSED,
                reason    = msg,
            )

        # --- Path guard (destination) ---
        ok, dest_msg = _check_path(str(planned_dest))
        if not ok:
            return TransferResult(
                src_path  = str(src),
                dest_path = str(planned_dest),
                outcome   = Outcome.PATH_REFUSED,
                reason    = dest_msg,
            )

        # --- Collision resolution ---
        final_dest, skip_reason = _resolve_collision(planned_dest, self.collision_mode)
        if final_dest is None:
            # SKIP mode: destination exists
            return TransferResult(
                src_path  = str(src),
                dest_path = str(planned_dest),
                outcome   = Outcome.SKIPPED,
                reason    = skip_reason,
                size_bytes = _safe_size(src),
            )

        # Determine outcome code before copy
        if not planned_dest.exists():
            outcome_code = Outcome.COPIED
        elif self.collision_mode == CollisionMode.RENAME:
            outcome_code = Outcome.COLLISION
        else:
            outcome_code = Outcome.OVERWRITTEN

        # --- Dry run: return planned outcome without touching filesystem ---
        if dry_run:
            result = TransferResult(
                src_path   = str(src),
                dest_path  = str(final_dest),
                outcome    = outcome_code,
                size_bytes = _safe_size(src),
            )
            # Attach path warning if dest has one
            if dest_msg:
                result.outcome = Outcome.PATH_WARNED
                result.reason  = dest_msg
            return result

        # --- Live copy ---
        src_size = _safe_size(src)

        # Hash the source BEFORE copying. This guarantees the hash was taken
        # from the same read-pass that preceded the copy, not a second read
        # after the fact (when the source could theoretically have changed).
        # It also halves source I/O: one read for hash, one for the copy.
        src_hash: Optional[str] = None
        if self.verify_hash:
            try:
                src_hash = sha256_file(str(src))
            except Exception as e:
                return TransferResult(
                    src_path   = str(src),
                    dest_path  = str(final_dest),
                    outcome    = Outcome.FAILED,
                    reason     = f"Pre-copy hash failed: {e}",
                    size_bytes = src_size,
                )

        try:
            _do_copy(src, final_dest)
        except Exception as e:
            return TransferResult(
                src_path   = str(src),
                dest_path  = str(final_dest),
                outcome    = Outcome.FAILED,
                reason     = f"Copy failed: {e}",
                size_bytes = src_size,
            )

        # --- Hash verification ---
        if self.verify_hash:
            try:
                dest_hash = sha256_file(str(final_dest))
            except Exception as e:
                _remove_failed_dest(final_dest)
                if self.hash_cb:
                    try:
                        self.hash_cb(False, src.name)
                    except Exception:
                        pass
                return TransferResult(
                    src_path   = str(src),
                    dest_path  = str(final_dest),
                    outcome    = Outcome.FAILED,
                    reason     = f"Post-copy hash failed: {e}",
                    size_bytes = src_size,
                )

            if src_hash != dest_hash:
                _remove_failed_dest(final_dest)
                if self.hash_cb:
                    try:
                        self.hash_cb(False, src.name)
                    except Exception:
                        pass
                return TransferResult(
                    src_path   = str(src),
                    dest_path  = str(final_dest),
                    outcome    = Outcome.FAILED,
                    reason     = (
                        f"Hash mismatch after copy — destination removed. "
                        f"src={src_hash[:12]}… dest={dest_hash[:12]}…"
                    ),
                    src_hash   = src_hash,
                    dest_hash  = dest_hash,
                    size_bytes = src_size,
                )

            # Hash matched — notify UI
            if self.hash_cb:
                try:
                    self.hash_cb(True, src.name)
                except Exception:
                    pass
        else:
            dest_hash = None

        result = TransferResult(
            src_path   = str(src),
            dest_path  = str(final_dest),
            outcome    = outcome_code,
            src_hash   = src_hash,
            dest_hash  = dest_hash,
            size_bytes = src_size,
        )

        # Attach path warning note if applicable (copy still succeeded).
        # Previously this overwrote outcome_code with PATH_WARNED, causing
        # bytes_copied to be undercounted in finalise() because COPIED and
        # OVERWRITTEN were the only outcomes counted. Now PATH_WARNED is
        # kept as outcome so the warning is visible in the report, but
        # bytes_copied in finalise() includes PATH_WARNED rows.
        if dest_msg:
            result.outcome = Outcome.PATH_WARNED
            result.reason  = dest_msg

        return result


# ---------------------------------------------------------------------------
# Report writing
# ---------------------------------------------------------------------------

def write_transfer_report(report: TransferReport, log_dir: Optional[str] = None) -> Optional[str]:
    """Write a human-readable transfer report and return the file path.

    Report is written to `log_dir` (defaults to ~/.dj_library_manager/logs/transfer/).
    Returns the path on success, None on failure.
    """
    import json

    if log_dir is None:
        base = Path.home() / ".dj_library_manager" / "logs" / "transfer"
    else:
        base = Path(log_dir)

    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None

    ts        = report.timestamp
    txt_path  = base / f"transfer_{ts}.txt"
    json_path = base / f"transfer_{ts}.json"

    # --- Human-readable .txt ---
    try:
        label = "DRY RUN" if report.dry_run else "LIVE TRANSFER"
        lines = [
            f"Transfer Report — {label}",
            f"Generated : {ts} UTC",
            f"Source    : {report.source_root}",
            f"Dest      : {report.dest_root}",
            f"Collision : {report.collision_mode}",
            f"",
            f"{'─' * 60}",
            f"  Total files   : {report.total:>8,}",
            f"  Copied        : {report.copied:>8,}",
            f"  Skipped       : {report.skipped:>8,}",
            f"  Collisions    : {report.collisions:>8,}  (renamed)",
            f"  Overwritten   : {report.overwritten:>8,}",
            f"  Failed        : {report.failed:>8,}",
            f"  Path refused  : {report.refused:>8,}",
            f"  Path warned   : {report.warned:>8,}",
            f"  Bytes copied  : {report.bytes_copied:>8,}  ({_fmt_bytes(report.bytes_copied)})",
            f"{'─' * 60}",
            f"",
        ]

        # Failures first — most important for the user to see
        failures = [r for r in report.results if r.outcome in (Outcome.FAILED, Outcome.PATH_REFUSED)]
        if failures:
            lines.append(f"FAILURES / REFUSED ({len(failures)}):")
            for r in failures:
                lines.append(f"  [{r.outcome.value}] {r.src_path}")
                if r.reason:
                    lines.append(f"           {r.reason}")
            lines.append("")

        warned = [r for r in report.results if r.outcome == Outcome.PATH_WARNED]
        if warned:
            lines.append(f"PATH WARNINGS ({len(warned)}):")
            for r in warned:
                lines.append(f"  {r.dest_path}")
                if r.reason:
                    lines.append(f"    {r.reason}")
            lines.append("")

        collisions = [r for r in report.results if r.outcome == Outcome.COLLISION]
        if collisions:
            lines.append(f"RENAMED (collision avoidance) ({len(collisions)}):")
            for r in collisions:
                lines.append(f"  {Path(r.src_path).name}  →  {Path(r.dest_path).name}")
            lines.append("")

        lines.append("FULL FILE LIST:")
        for r in report.results:
            lines.append(f"  [{r.outcome.value:<13}] {r.src_path}")

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
    except Exception:
        txt_path = None

    # --- Machine-readable .json ---
    try:
        payload = {
            "schema_version": 1,
            "timestamp":      ts,
            "source_root":    report.source_root,
            "dest_root":      report.dest_root,
            "dry_run":        report.dry_run,
            "collision_mode": report.collision_mode,
            "summary": {
                "total":        report.total,
                "copied":       report.copied,
                "skipped":      report.skipped,
                "collisions":   report.collisions,
                "overwritten":  report.overwritten,
                "failed":       report.failed,
                "refused":      report.refused,
                "warned":       report.warned,
                "bytes_copied": report.bytes_copied,
            },
            "results": [
                {
                    "src":      r.src_path,
                    "dest":     r.dest_path,
                    "outcome":  r.outcome.value,
                    "reason":   r.reason,
                    "src_hash": r.src_hash,
                    "size":     r.size_bytes,
                }
                for r in report.results
            ],
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except Exception:
        json_path = None

    return str(txt_path) if txt_path else None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_size(path: Path) -> Optional[int]:
    try:
        return path.stat().st_size
    except Exception:
        return None


def _fmt_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024.0
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
    return f"{n:.1f} TB"
