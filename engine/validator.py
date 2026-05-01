"""
DJ Library Manager — Validation Engine

Lightweight validation utilities used before transfer:
- Path length scanner (OS-aware: 260 Windows / 1024 macOS / 4096 Linux)
- [Planned] Corrupt / degraded MP3 detection via mp3val

Duplicate detection has moved to engine/duplicate_finder.py.
"""

import os
from pathlib import Path
from datetime import datetime
from PySide6.QtCore import QThread, Signal

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/platform_adapter.py  →  PlatformAdapter.get_path_limit()
# Why: get_path_limit() is the single OS-aware source of truth for path length
#      defaults (260 Windows / 1024 macOS / 4096 Linux). Hardcoding a value here
#      would give wrong defaults on Linux and macOS.
# If you modify platform_adapter.PlatformAdapter.get_path_limit(), verify that
# _os_path_limit() below (and ValidatorRunner) still produce correct results.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .platform_adapter import PlatformAdapter as _PA
except ImportError:
    try:
        from platform_adapter import PlatformAdapter as _PA
    except ImportError:
        _PA = None


def _os_path_limit():
    if _PA is not None:
        return _PA.get_path_limit()
    return 240


class Validator:

    @staticmethod
    def scan_path_lengths(root_path, limit=None):
        if limit is None:
            limit = _os_path_limit()
        root = Path(root_path)
        if not root.exists():
            return []
        results = []
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                p = Path(dirpath) / name
                plen = len(str(p))
                if plen > limit:
                    results.append((str(p), plen))
        return results

    @staticmethod
    def write_path_length_report(results, out_dir=None, limit=240, retention=20):
        if out_dir is None:
            out_dir = Path.home() / ".dj_library_manager" / "logs" / "validation"
        else:
            out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fpath = out_dir / f"path_length_report_{ts}.txt"
        try:
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(f"Path length report — cutoff={limit}\n")
                f.write(f"Generated: {ts} UTC\n\n")
                if not results:
                    f.write("No paths exceeding limit found.\n")
                else:
                    for p, l in results:
                        f.write(f"{l:4d}  {p}\n")
            Validator._prune_logs(out_dir, keep=int(retention))
            return str(fpath)
        except Exception:
            return None

    @staticmethod
    def scan_corrupt_mp3s(root_path):
        """[PLANNED v0.5.0] Scan for corrupt/degraded MP3s via mp3val."""
        raise NotImplementedError(
            "Corrupt MP3 scanning is planned for v0.5.0. Requires mp3val."
        )

    @staticmethod
    def _prune_logs(out_dir, keep=20):
        try:
            files = [p for p in Path(out_dir).iterdir() if p.is_file()]
            if len(files) <= keep:
                return
            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for p in files[keep:]:
                try:
                    p.unlink()
                except Exception:
                    pass
        except Exception:
            pass


class ValidatorRunner(QThread):
    """Runs validation tasks in a background thread.
    Supported: 'path_length'   Planned: 'corrupt_mp3'
    """
    output   = Signal(str)
    finished = Signal(str)

    def __init__(self, task, root_path, **kwargs):
        super().__init__()
        self.task      = task
        self.root_path = root_path
        self.kwargs    = kwargs or {}

    def run(self):
        try:
            if self.task == "path_length":
                limit = self.kwargs.get("limit") or _os_path_limit()
                self.output.emit(f"Scanning for paths longer than {limit} chars…")
                results = Validator.scan_path_lengths(self.root_path, limit=limit)
                self.output.emit(
                    f"Found {len(results):,} path(s) exceeding {limit} characters."
                    if results else "All paths within limit."
                )
                out = Validator.write_path_length_report(
                    results,
                    out_dir=self.kwargs.get("out_dir"),
                    limit=limit,
                    retention=self.kwargs.get("retention", 20),
                )
                self.finished.emit(out or "")
            else:
                self.output.emit(f"Unknown validation task: {self.task!r}")
                self.finished.emit("")
        except Exception as e:
            self.output.emit(f"ERROR: {e}")
            self.finished.emit("")
