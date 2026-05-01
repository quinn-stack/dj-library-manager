"""
DJ Library Manager — Tag Finder Page
Replaces the old Beets page entirely.

Uses AcoustIDEngine directly — no beets dependency.
Wires into the existing settings_manager and profile_manager.
Drop this into ui/ and wire it in main_window._build_ui() like any other page.
"""

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/acoustid_engine.py  →  AcoustIDEngine, AcoustIDRunner
# Why: AcoustIDRunner is the sole pipeline runner for fingerprint-based tagging.
#      AcoustIDEngine provides dependency checks displayed in the info card.
#      This page is the only UI surface that drives the AcoustID pipeline.
# If you modify acoustid_engine.py (signals, stats dict keys, constructor
# parameters), update this page accordingly and test both normal and retry runs.
# Key stats keys consumed here: tagged, skipped_low_confidence, no_match,
# tag_write_failed, tag_write_failed_files, api_errors, error_files,
# low_confidence_json, write_failure_report, error_report, cancelled.
# ─────────────────────────────────────────────────────────────────────────────

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/low_confidence_manager.py  →  LowConfidenceManager
# Why: LowConfidenceManager.load_batch() is used to open a saved low confidence
#      JSON batch for in-page review. The quarantine and tag-apply operations
#      are also delegated to this engine.
# If you modify LowConfidenceManager.load_batch() or the batch JSON schema,
# update _load_lc_batch() and the review card in this page accordingly.
# NOTE: low_confidence_manager.py does not yet carry the reciprocal "USED BY"
#       annotation — add it the next time that file is edited.
# ─────────────────────────────────────────────────────────────────────────────

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/tagging.py  →  rename_files_to_tags, apply_renames,
#             revert_from_report
# Why: These are the sole authoritative implementations for tag-based rename,
#      bulk rename application, and rename undo.
# If you modify tagging.py, verify this page's rename workflow still works.
# NOTE: tagging.py does not yet carry the reciprocal "USED BY" annotation —
#       add it the next time tagging.py is edited.
# ─────────────────────────────────────────────────────────────────────────────

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QTextEdit, QFrame, QProgressBar, QLineEdit,
    QSizePolicy, QTableWidget, QTableWidgetItem, QHeaderView,
    QAbstractItemView, QMessageBox, QFileDialog, QScrollArea,
    QDialog, QVBoxLayout as QVBox, QListWidget, QDialogButtonBox,
    QCheckBox
)
from threading import Event
from PySide6.QtCore import Qt, QTimer, QThread, Signal
from PySide6.QtGui import QTextCursor, QColor
import re
from pathlib import Path
from datetime import datetime

from engine.acoustid_engine import AcoustIDEngine, AcoustIDRunner
from engine.command_runner import TaskRunner


class RenameRunner(QThread):
    """QThread wrapper for rename_files_to_tags / apply_renames.

    Emits per-file progress so the UI can show a live progress bar and ETA.
    Two modes controlled by ``mode``:
      "scan"  — calls rename_files_to_tags(root, dry_run=True, ...)
      "apply" — calls apply_renames(pairs, dry_run=False, ...)

    Cancellation:
      Call stop() from the UI thread. This sets the internal threading.Event
      which is passed to the underlying function as stop_event. The function
      returns its partial results at the next file boundary — finished() is
      always emitted so the UI transitions cleanly.

    Signals:
      progress(current, total, filename) — emitted per file; total is 0
          during the initial walk phase of "scan" mode.
      finished(result)                   — emitted on completion or
          cancellation with the (partial) return value.
    """
    progress = Signal(int, int, str)
    finished = Signal(object)

    def __init__(self, mode: str, *, root: str = "", pairs: list = None):
        super().__init__()
        self.mode        = mode    # "scan" | "apply"
        self.root        = root
        self.pairs       = pairs or []
        self._stop_event = Event()

    def stop(self):
        """Request cancellation at the next file boundary."""
        self._stop_event.set()

    def run(self):
        try:
            from engine.tagging import rename_files_to_tags, apply_renames
            if self.mode == "scan":
                result = rename_files_to_tags(
                    self.root, dry_run=True,
                    progress_cb=lambda c, t, f: self.progress.emit(c, t, f),
                    stop_event=self._stop_event,
                )
            else:  # "apply"
                result = apply_renames(
                    self.pairs, dry_run=False,
                    progress_cb=lambda c, t, f: self.progress.emit(c, t, f),
                    stop_event=self._stop_event,
                )
            self.finished.emit(result)
        except Exception as exc:
            import traceback
            self.finished.emit({"__task_error__": str(exc),
                                "__traceback__": traceback.format_exc()})

try:
    from engine.low_confidence_manager import (
        LowConfidenceManager, ACTION_APPLY, ACTION_QUARANTINE, ACTION_SKIP
    )
    HAS_LC_MANAGER = True
except ImportError:
    HAS_LC_MANAGER = False


class TagFinderPage(QWidget):
    """
    Self-contained Tag Finder page.

    Usage in main_window._build_ui():
        from ui.tag_finder_page import TagFinderPage
        self.page_tag_finder = self._wrap_page_with_scroll(
            TagFinderPage(self.settings_manager, self.profile_manager)
        )
    """

    def __init__(self, settings_manager, profile_manager):
        super().__init__()
        self.settings_manager  = settings_manager
        self.profile_manager   = profile_manager
        self._runner           = None
        self._rename_runner    = None
        self._current_profile  = None
        self._last_source      = None    # remembered after a run; also populates rename path field
        self._rename_path_field  = None   # QLineEdit ref set in _build_rename_controls()
        self._rename_start_time  = None   # datetime when current rename phase started
        self._rename_phase       = ""     # "scan" | "apply" — drives ETA label copy
        self._run_start_time   = None    # datetime of run start
        self._last_error_files = []      # api_error paths — for retry
        self._lc_entries       = []      # loaded low confidence batch entries
        self._lc_metadata      = {}      # batch metadata (generated, cutoff, count)
        self._lc_batch_path    = None    # path of currently loaded batch JSON

        # Running totals — reset at run start, incremented by _on_result
        self._run_totals = {"tagged": 0, "cached": 0, "partial": 0,
                            "skipped": 0, "no_match": 0, "error": 0}

        self._elapsed_timer = QTimer(self)
        self._elapsed_timer.setInterval(1000)
        self._elapsed_timer.timeout.connect(self._tick_elapsed)

        # ETA tracking — populated during a run
        self._fp_start_time        = None   # set when Stage 1 begins
        self._fp_last_current      = 0      # last progress value seen from Stage 1
        self._fp_last_total        = 0
        self._lookup_start_time    = None   # set when Stage 2 begins
        self._lookup_last_idx      = 0
        self._lookup_last_total    = 0
        self._tag_check_start_time = None   # set when skip-tagged check begins
        self._tag_check_current    = 0
        self._tag_check_total      = 0
        # Rolling window of (timestamp, count) tuples for Stage 1 rate smoothing.
        self._fp_window            = []

        self._build_ui()
        self._refresh_info()
        self._check_undo_available()

    # ═══════════════════════════════════════════════════════════════════
    # Build UI
    # ═══════════════════════════════════════════════════════════════════

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(16)

        heading = QLabel("TAG FINDER — ACOUSTID FINGERPRINT TAGGER")
        heading.setObjectName("heading")
        layout.addWidget(heading)
        layout.addWidget(self._divider())

        layout.addWidget(self._build_info_card())
        layout.addWidget(self._divider())
        layout.addLayout(self._build_controls())
        layout.addLayout(self._build_run_options())
        layout.addWidget(self._divider())
        layout.addLayout(self._build_rename_controls())
        layout.addWidget(self._divider())
        layout.addWidget(self._build_benchmark_card())

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(6)
        layout.addWidget(self.progress_bar)

        # Stage label
        self.stage_label = QLabel("")
        self.stage_label.setObjectName("subheading")
        self.stage_label.setVisible(False)
        layout.addWidget(self.stage_label)

        # Results table + log
        layout.addLayout(self._build_results_area())

        # Write failure banner — shown when tag_write_failed > 0
        self.write_fail_banner = QLabel("")
        self.write_fail_banner.setWordWrap(True)
        self.write_fail_banner.setContentsMargins(16, 12, 16, 12)
        self.write_fail_banner.setVisible(False)
        layout.addWidget(self.write_fail_banner)

        # Low confidence action card — hidden until a batch is available
        layout.addWidget(self._divider())
        layout.addWidget(self._build_lc_card())

    def _build_info_card(self):
        card        = QFrame()
        card.setObjectName("card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(16, 12, 16, 12)
        card_layout.setSpacing(4)

        self.info_platform  = QLabel()
        self.info_platform.setObjectName("subheading")
        card_layout.addWidget(self.info_platform)

        self.info_threshold = QLabel()
        self.info_threshold.setObjectName("subheading")
        card_layout.addWidget(self.info_threshold)

        self.info_key = QLabel()
        self.info_key.setObjectName("subheading")
        card_layout.addWidget(self.info_key)

        self.info_deps = QLabel()
        self.info_deps.setObjectName("subheading")
        card_layout.addWidget(self.info_deps)

        self.info_rps = QLabel()
        self.info_rps.setObjectName("subheading")
        card_layout.addWidget(self.info_rps)

        return card

    def _build_controls(self):
        row = QHBoxLayout()

        self.run_btn = QPushButton("▶  RUN TAG FINDER")
        self.run_btn.setObjectName("primary")
        self.run_btn.setFixedHeight(38)
        self.run_btn.clicked.connect(self.start_run)
        row.addWidget(self.run_btn)

        self.stop_btn = QPushButton("■  STOP")
        self.stop_btn.setObjectName("danger")
        self.stop_btn.setFixedHeight(38)
        self.stop_btn.setFixedWidth(100)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setToolTip(
            "Stop the current operation at the next safe checkpoint.\n"
            "Works during all phases: file scan, tag check,\n"
            "fingerprinting, and AcoustID lookup.\n"
            "Any files processed before you stop are unaffected."
        )
        self.stop_btn.clicked.connect(self.stop_run)
        row.addWidget(self.stop_btn)

        self.retry_btn = QPushButton("↩  RETRY ERRORS (0)")
        self.retry_btn.setObjectName("warning")
        self.retry_btn.setFixedHeight(38)
        self.retry_btn.setVisible(False)
        self.retry_btn.setToolTip(
            "Re-run the AcoustID lookup for files that returned API errors\n"
            "on the previous run (network errors, timeouts, HTTP failures).\n\n"
            "If you are seeing many errors, reduce the rate limit in Settings\n"
            "before retrying."
        )
        self.retry_btn.clicked.connect(self.start_retry)
        row.addWidget(self.retry_btn)

        self.perm_retry_btn = QPushButton("🔒  FIX PERMISSIONS (0)")
        self.perm_retry_btn.setObjectName("warning")
        self.perm_retry_btn.setFixedHeight(38)
        self.perm_retry_btn.setVisible(False)
        self.perm_retry_btn.setToolTip(
            "Files that failed due to \'Permission denied\' errors.\n"
            "Click to get instructions for fixing permissions, then\n"
            "re-run tag writing on just those files."
        )
        self.perm_retry_btn.clicked.connect(self._handle_perm_errors)
        row.addWidget(self.perm_retry_btn)

        self.status_label = QLabel("IDLE")
        self.status_label.setObjectName("status_idle")
        row.addWidget(self.status_label)

        row.addStretch()
        return row

    def _build_run_options(self):
        """Checkboxes that modify run behaviour — shown directly under the buttons."""
        row = QHBoxLayout()
        row.setContentsMargins(2, 2, 0, 2)
        row.setSpacing(24)

        self.opt_use_cache = QCheckBox("Skip files tagged in a previous run  (fingerprint cache)")
        self.opt_use_cache.setChecked(True)
        self.opt_use_cache.setToolTip(
            "When enabled, files that were successfully fingerprinted and tagged\n"
            "on a previous run are skipped entirely — no fpcalc, no API call.\n\n"
            "The cache is stored at:\n"
            "  ~/.dj_library_manager/fingerprint_cache.json\n\n"
            "If a file has been modified since it was cached (e.g. re-encoded\n"
            "or replaced), it is automatically re-processed."
        )
        row.addWidget(self.opt_use_cache)

        self.opt_skip_tagged = QCheckBox("Skip files that already have Artist + Title tags")
        self.opt_skip_tagged.setChecked(False)
        self.opt_skip_tagged.setToolTip(
            "When enabled, any file that already has non-empty Artist AND Title\n"
            "tags is skipped before fingerprinting begins.\n\n"
            "Use this to avoid overwriting tags that were set manually or by\n"
            "another tool. Disable it if you want AcoustID to re-tag everything."
        )
        row.addWidget(self.opt_skip_tagged)

        row.addStretch()
        return row

    def _build_rename_controls(self):
        """Two-row rename section.

        Row 1 — path selection: label | path field | Browse button
        Row 2 — actions:        RENAME button | UNDO button | status label

        The rename button is enabled whenever a folder path is set — it is NOT
        gated on a Tag Finder run having completed. This means the user can:
          - Rename immediately after a normal tagging run (path auto-filled)
          - Rename after a cache-hit-only run (no new tags written but files tagged)
          - Rename after applying LC batch actions (tags written outside the run)
          - Rename any folder with existing tags without running Tag Finder at all
        """
        outer = QVBoxLayout()
        outer.setSpacing(6)

        # ── Row 1: path selection ──────────────────────────────────────────────
        path_row = QHBoxLayout()

        rename_heading = QLabel("RENAME FILES:")
        rename_heading.setObjectName("subheading")
        path_row.addWidget(rename_heading)

        self._rename_path_field = QLineEdit()
        self._rename_path_field.setPlaceholderText("Folder to rename — auto-filled from profile/run, or browse…")
        self._rename_path_field.setToolTip(
            "Folder to walk when renaming files to 'Artist - Title'.\n"
            "Auto-filled from the active profile source path and after each run.\n"
            "You can also type or browse to any folder."
        )
        self._rename_path_field.textChanged.connect(self._update_rename_btn)
        path_row.addWidget(self._rename_path_field, stretch=1)

        rename_browse_btn = QPushButton("Browse…")
        rename_browse_btn.setFixedHeight(30)
        rename_browse_btn.setToolTip("Choose a folder to rename")
        rename_browse_btn.clicked.connect(self._browse_rename_path)
        path_row.addWidget(rename_browse_btn)

        outer.addLayout(path_row)

        # ── Row 2: action buttons + status ────────────────────────────────────
        action_row = QHBoxLayout()

        self.rename_btn = QPushButton("✎  RENAME TO ARTIST — TITLE")
        self.rename_btn.setObjectName("primary")
        self.rename_btn.setFixedHeight(34)
        self.rename_btn.setEnabled(False)
        self.rename_btn.setToolTip(
            "Rename every audio file in the chosen folder to 'Artist - Title.ext'\n"
            "by reading its current tags with mutagen.\n"
            "Shows a full preview before applying — no files are touched until you confirm.\n"
            "Works any time a folder is set — does not require a Tag Finder run."
        )
        self.rename_btn.clicked.connect(self.run_rename_preview)
        action_row.addWidget(self.rename_btn)

        self.undo_rename_btn = QPushButton("↩  UNDO RENAMES")
        self.undo_rename_btn.setFixedHeight(34)
        self.undo_rename_btn.setEnabled(False)
        self.undo_rename_btn.setToolTip("Revert the most recent batch of renames using its undo report.")
        self.undo_rename_btn.clicked.connect(self.run_undo_renames)
        action_row.addWidget(self.undo_rename_btn)

        self.rename_status = QLabel("")
        self.rename_status.setObjectName("subheading")
        action_row.addWidget(self.rename_status)

        action_row.addStretch()
        outer.addLayout(action_row)

        return outer

    def _build_benchmark_card(self):
        card        = QFrame()
        card.setObjectName("card")
        card_layout = QHBoxLayout(card)
        card_layout.setContentsMargins(16, 10, 16, 10)
        card_layout.setSpacing(32)

        self.bench_started   = QLabel("Started:  —")
        self.bench_started.setObjectName("subheading")
        card_layout.addWidget(self.bench_started)

        self.bench_elapsed   = QLabel("Elapsed:  —")
        self.bench_elapsed.setObjectName("subheading")
        card_layout.addWidget(self.bench_elapsed)

        self.bench_eta = QLabel("Overall ETA:  —")
        self.bench_eta.setObjectName("subheading")
        self.bench_eta.setVisible(False)   # hidden until we have enough data
        card_layout.addWidget(self.bench_eta)

        self.bench_completed = QLabel("Completed:  —")
        self.bench_completed.setObjectName("subheading")
        card_layout.addWidget(self.bench_completed)

        # ── Running totals — coloured inline counters ─────────────────
        # Separator
        sep = QLabel("│")
        sep.setObjectName("subheading")
        sep.setStyleSheet("color: #444;")
        card_layout.addWidget(sep)

        self._totals_labels = {}
        totals_spec = [
            ("tagged",   "TAGGED",   "#4caf82"),
            ("cached",   "CACHED",   "#4a90d9"),
            ("partial",  "PARTIAL",  "#7a8fc4"),
            ("skipped",  "SKIPPED",  "#f0c040"),
            ("no_match", "NO MATCH", "#888888"),
            ("error",    "ERROR",    "#e05050"),
        ]
        for key, label, colour in totals_spec:
            lbl = QLabel(f"{label}: 0")
            lbl.setObjectName("subheading")
            lbl.setStyleSheet(f"color: {colour}; font-weight: bold;")
            lbl.setToolTip(f"Running total for '{label.lower()}' outcomes this run.")
            card_layout.addWidget(lbl)
            self._totals_labels[key] = lbl

        card_layout.addStretch()
        return card

    def _build_results_area(self):
        mid = QHBoxLayout()

        # Left: log
        left      = QVBoxLayout()
        log_label = QLabel("▌ OUTPUT LOG")
        log_label.setObjectName("subheading")
        left.addWidget(log_label)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMinimumHeight(340)
        left.addWidget(self.log)
        mid.addLayout(left, 2)

        # Right: per-track results table
        right       = QVBoxLayout()
        table_label = QLabel("▌ TRACK RESULTS")
        table_label.setObjectName("subheading")
        right.addWidget(table_label)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["File", "Status", "Score"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setMinimumWidth(380)
        self.table.setMinimumHeight(340)
        right.addWidget(self.table)

        self.summary_label = QLabel("")
        self.summary_label.setObjectName("subheading")
        self.summary_label.setWordWrap(True)
        right.addWidget(self.summary_label)

        mid.addLayout(right, 1)
        return mid

    def _build_lc_card(self):
        """
        Low Confidence Action Card.
        Shows a list of recent LC_BATCH_*.json files sorted newest-first.
        Single-click to load a batch; double-click to load and open review dialog.
        Right-click for: Open, Quarantine All, Delete Batch.
        """
        from PySide6.QtWidgets import QListWidget, QListWidgetItem, QMenu
        from PySide6.QtCore import Qt as _Qt

        self.lc_card = QFrame()
        self.lc_card.setObjectName("card")
        card_layout = QVBoxLayout(self.lc_card)
        card_layout.setContentsMargins(16, 14, 16, 16)
        card_layout.setSpacing(10)

        # ── Title row ─────────────────────────────────────────────────────────
        title_row = QHBoxLayout()
        lc_title  = QLabel("LOW CONFIDENCE BATCHES")
        lc_title.setObjectName("heading")
        title_row.addWidget(lc_title)
        title_row.addStretch()

        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setFixedHeight(28)
        refresh_btn.setToolTip("Rescan for new LC_BATCH files.")
        refresh_btn.clicked.connect(self._refresh_lc_batch_list)
        title_row.addWidget(refresh_btn)
        card_layout.addLayout(title_row)

        # ── Batch list ────────────────────────────────────────────────────────
        self.lc_batch_list = QListWidget()
        self.lc_batch_list.setFixedHeight(130)
        self.lc_batch_list.setToolTip(
            "Single-click to load a batch.\n"
            "Double-click to load and open the review dialog.\n"
            "Right-click for more options."
        )
        self.lc_batch_list.setContextMenuPolicy(_Qt.CustomContextMenu)
        self.lc_batch_list.customContextMenuRequested.connect(self._lc_batch_list_context_menu)
        self.lc_batch_list.itemClicked.connect(self._on_lc_batch_list_click)
        self.lc_batch_list.itemDoubleClicked.connect(self._on_lc_batch_list_double_click)
        card_layout.addWidget(self.lc_batch_list)

        # ── Batch info line ───────────────────────────────────────────────────
        self.lc_info = QLabel("Select a batch above to load it.")
        self.lc_info.setObjectName("subheading")
        self.lc_info.setWordWrap(True)
        card_layout.addWidget(self.lc_info)

        # ── Action buttons ────────────────────────────────────────────────────
        btn_row = QHBoxLayout()

        self.lc_review_btn = QPushButton("🔍  REVIEW TRACKS")
        self.lc_review_btn.setObjectName("primary")
        self.lc_review_btn.setFixedHeight(34)
        self.lc_review_btn.setEnabled(False)
        self.lc_review_btn.setToolTip(
            "Open the per-track review table.\n"
            "Set each track to: Apply Tags / Quarantine / Skip."
        )
        self.lc_review_btn.clicked.connect(self._open_lc_review_dialog)
        btn_row.addWidget(self.lc_review_btn)

        self.lc_quarantine_all_btn = QPushButton("📦  QUARANTINE ALL")
        self.lc_quarantine_all_btn.setFixedHeight(34)
        self.lc_quarantine_all_btn.setEnabled(False)
        self.lc_quarantine_all_btn.setToolTip(
            "Move all low-confidence files to:\n"
            "  <source>/_QUARANTINE/_LOW_CONFIDENCE_TAGS/\n\n"
            "Files can be listened to and manually tagged later.\n"
            "Their relative folder structure is preserved."
        )
        self.lc_quarantine_all_btn.clicked.connect(self._lc_quarantine_all)
        btn_row.addWidget(self.lc_quarantine_all_btn)

        btn_row.addStretch()
        card_layout.addLayout(btn_row)

        # ── Status line ───────────────────────────────────────────────────────
        self.lc_status = QLabel("")
        self.lc_status.setObjectName("subheading")
        self.lc_status.setWordWrap(True)
        card_layout.addWidget(self.lc_status)

        # Populate list on first build
        self._refresh_lc_batch_list()

        return self.lc_card

    # ═══════════════════════════════════════════════════════════════════
    # Info Card Refresh
    # ═══════════════════════════════════════════════════════════════════

    def _refresh_info(self):
        """Call this whenever settings may have changed (e.g. on page show)."""
        try:
            from engine.platform_adapter import PlatformAdapter
            os_name = PlatformAdapter.get_os()
        except Exception:
            import platform
            os_name = platform.system()

        thresh  = self.settings_manager.get_active_thresholds()
        preset  = self.settings_manager.get_setting("threshold_preset")
        api_key = self.settings_manager.get_setting("acoustid_api_key") or ""
        key_display = (
            f"{api_key[:4]}{'*' * (len(api_key) - 4)}"
            if len(api_key) > 4
            else ("SET" if api_key else "⚠  NOT SET — go to Settings")
        )

        deps     = AcoustIDEngine.check_dependencies()
        dep_str  = "   |   ".join(
            f"{name}: {'✔' if ok else '✘ MISSING'}" for name, ok in deps.items()
        )

        rps      = self.settings_manager.get_acoustid_rps()
        rps_note = (
            f"{int(rps)} RPS (max)"
            if rps >= 3
            else f"{rps:.0f} RPS  ⚠ reduced — server stress mode"
        )

        self.info_platform.setText(f"Platform: {os_name}   |   Workers: 4 fingerprint threads")
        self.info_threshold.setText(
            f"Preset: {preset}   |   "
            f"strong≥{thresh.get('strong', '?')}   medium≥{thresh.get('medium', '?')}"
        )
        self.info_key.setText(f"AcoustID Key: {key_display}")
        self.info_deps.setText(f"Dependencies: {dep_str}")
        self.info_rps.setText(f"Lookup rate: {rps_note}")

    def set_profile(self, profile_name: str):
        """Called by main_window when the active profile changes."""
        self._current_profile = profile_name
        # Pre-populate the rename path field with the profile source path so the
        # rename button is usable immediately — before any run has been started.
        # Only set it if _last_source isn't already set (i.e. no run has been done
        # this session) to avoid overwriting a path from a recent run.
        if not self._last_source and self._rename_path_field is not None:
            try:
                profile = self.profile_manager.load_profile(profile_name)
                src = (profile or {}).get("source_path", "").strip()
                if src:
                    self._rename_path_field.setText(src)
            except Exception:
                pass

    # ═══════════════════════════════════════════════════════════════════
    # Run / Stop / Retry
    # ═══════════════════════════════════════════════════════════════════

    def start_run(self):
        """Full run — discover all files under the profile source path."""
        self._refresh_info()

        if not self._current_profile:
            self._log("⚠  No profile selected. Choose a profile from the header dropdown.")
            return

        profile = self.profile_manager.load_profile(self._current_profile)
        if not profile:
            self._log("⚠  Profile could not be loaded.")
            return

        source = profile.get("source_path", "").strip()
        if not source:
            self._log("⚠  Source path not set in the current profile.")
            return

        api_key = self.settings_manager.get_setting("acoustid_api_key") or ""
        if not api_key:
            self._log(
                "⚠  No AcoustID API key set.\n"
                "    Go to Settings → AcoustID API Key and add your key."
            )
            return

        deps = AcoustIDEngine.check_dependencies()
        if not deps["fpcalc"]:
            self._log(
                "✘  fpcalc not found.\n"
                "    Linux:  sudo apt install libchromaprint-tools\n"
                "    macOS:  brew install chromaprint\n"
                "    Then restart the app."
            )
            return
        if not deps["mutagen"]:
            self._log("✘  mutagen not installed. Run: pip install mutagen")
            return

        thresh = self.settings_manager.get_active_thresholds()
        rps    = self.settings_manager.get_acoustid_rps()
        self._last_source = source
        # Push to path field so rename is immediately available after run starts
        if self._rename_path_field is not None:
            self._rename_path_field.setText(source)

        self._start_runner(
            source_path=source, api_key=api_key, thresh=thresh,
            rps=rps, retry_files=None,
            skip_tagged=self.opt_skip_tagged.isChecked(),
            use_cache=self.opt_use_cache.isChecked(),
        )

    def start_retry(self):
        """Retry run — only processes files that returned API errors last time."""
        if not self._last_error_files:
            return

        api_key = self.settings_manager.get_setting("acoustid_api_key") or ""
        if not api_key:
            self._log("⚠  No AcoustID API key set.")
            return

        thresh = self.settings_manager.get_active_thresholds()
        rps    = self.settings_manager.get_acoustid_rps()

        self._log(
            f"\n{'─' * 48}\n"
            f"↩  RETRY — {len(self._last_error_files):,} file(s) that errored on previous run\n"
            f"   Rate: {rps:.0f} RPS"
            + ("  ⚠ reduced" if rps < 3 else "") + "\n"
            f"{'─' * 48}"
        )

        self._start_runner(
            source_path=self._last_source or "", api_key=api_key, thresh=thresh,
            rps=rps, retry_files=self._last_error_files,
        )

    def _start_runner(self, source_path: str, api_key: str, thresh: dict,
                      rps: float, retry_files: list | None,
                      skip_tagged: bool = False, use_cache: bool = True):
        """Common setup for both normal and retry runs."""
        self._run_start_time       = datetime.now()
        self._fp_start_time        = datetime.now()
        self._fp_last_current      = 0
        self._fp_last_total        = 0
        self._fp_window            = []
        self._lookup_start_time    = None
        self._lookup_last_idx      = 0
        self._lookup_last_total    = 0
        self._tag_check_start_time = None
        self._tag_check_current    = 0
        self._tag_check_total      = 0
        self.bench_started.setText(f"Started:  {self._run_start_time.strftime('%H:%M:%S')}")
        self.bench_elapsed.setText("Elapsed:  00:00:00")
        self.bench_eta.setText("Overall ETA:  —")
        self.bench_eta.setVisible(False)
        # Reset running totals
        self._run_totals = {k: 0 for k in self._run_totals}
        for key, lbl in self._totals_labels.items():
            lbl.setText(f"{lbl.text().split(':')[0]}: 0")
        self.bench_completed.setText("Completed:  —")
        self._elapsed_timer.start()

        self.log.clear()
        self.table.setRowCount(0)
        self.summary_label.setText("")
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(0)   # indeterminate until file count known
        self.progress_bar.setVisible(True)
        self.stage_label.setVisible(True)
        self.stage_label.setText("Scanning for audio files…")
        self._set_status("RUNNING", "status_running")
        self.run_btn.setEnabled(False)
        self.retry_btn.setVisible(False)
        self.perm_retry_btn.setVisible(False)
        self.stop_btn.setEnabled(True)
        # Rename button stays enabled during a run if a path is already set
        # (user could start renaming a different folder while the run proceeds).
        # We just clear the status text so it doesn't show stale info.
        self.rename_status.setText("")
        self.write_fail_banner.setVisible(False)
        # Don't hide the lc_card — a previously loaded batch persists

        if retry_files is None:
            options_line = "  ".join(filter(None, [
                "cache=ON" if use_cache else "cache=OFF",
                "skip-tagged=ON" if skip_tagged else None,
            ]))
            self._log(
                f"Starting Tag Finder run\n"
                f"Source:  {source_path}\n"
                f"Preset:  {self.settings_manager.get_setting('threshold_preset')}   "
                f"strong≥{thresh.get('strong')}   medium≥{thresh.get('medium')}\n"
                f"Rate:    {rps:.0f} RPS   |   {options_line}\n"
                f"{'─' * 48}"
            )

        self._runner = AcoustIDRunner(
            source_path=source_path,
            api_key=api_key,
            strong_thresh=thresh.get("strong", 0.95),
            medium_thresh=thresh.get("medium", 0.90),
            rps=rps,
            retry_files=retry_files or [],
            skip_tagged=skip_tagged,
            use_cache=use_cache,
        )
        self._runner.progress.connect(self._on_progress)
        self._runner.lookup_progress.connect(self._on_lookup_progress)
        self._runner.scanning.connect(self._on_scanning)
        self._runner.tag_check_progress.connect(self._on_tag_check_progress)
        self._runner.result.connect(self._on_result)
        self._runner.log.connect(self._log)
        self._runner.finished.connect(self._on_finished)
        self._runner.start()

    def stop_run(self):
        """Signal the runner to stop at the next safe checkpoint.

        Works at all pipeline phases:
          - File scan     : interrupts between directory batches (≤200 dirs)
          - Tag check     : interrupts every 500 files
          - Fingerprinting: interrupts between fpcalc workers
          - AcoustID lookup: interrupts between API calls

        The runner always emits finished() after stopping so the UI
        transitions cleanly to its post-run state.
        """
        stopped_something = False
        if self._runner and self._runner.isRunning():
            self._runner.stop()
            stopped_something = True
        if self._rename_runner and self._rename_runner.isRunning():
            self._rename_runner.stop()
            stopped_something = True
        if stopped_something:
            self._log("⚠  Stop requested — will halt at next checkpoint…")
        self.stop_btn.setEnabled(False)

    # ═══════════════════════════════════════════════════════════════════
    # Signal Handlers
    # ═══════════════════════════════════════════════════════════════════

    # Rolling window size for Stage 1 rate smoothing (number of completions)
    _FP_WINDOW_SIZE = 40

    def _on_scanning(self, message: str):
        """Pre-fingerprint phase — file walk, tag check, cache check.

        These run on the worker thread but can take several seconds on large
        libraries or network drives. Show the message in the stage label with
        an indeterminate progress bar so the UI doesn't appear frozen.
        """
        self.stage_label.setText(message)
        self.stage_label.setVisible(True)
        self.progress_bar.setMaximum(0)   # indeterminate spinner
        self.progress_bar.setVisible(True)

    def _on_tag_check_progress(self, current: int, total: int):
        """Skip-tagged check progress — stores state for _tick_elapsed, updates bar.

        Signals arrive every 500 files (throttled in engine) so overhead is
        minimal even on 65k-file libraries.  setMaximum is only called on the
        first signal to avoid unnecessary repaints on every update.
        """
        first = self._tag_check_start_time is None
        if first:
            self._tag_check_start_time = datetime.now()
        self._tag_check_current = current
        self._tag_check_total   = total
        if total > 0:
            if first:
                self.progress_bar.setMaximum(total)   # only needed once
            self.progress_bar.setValue(current)

    def _on_progress(self, current: int, total: int, filename: str):
        """Stage 1 — fingerprinting progress.

        Stores state for the timer-driven label update. Only updates the
        progress bar here — the stage label is updated by _tick_elapsed so
        that rapid-fire signals from 4 parallel workers don't cause flickering.
        """
        self._fp_last_current = current
        self._fp_last_total   = total

        # Append to rolling window: (timestamp, cumulative_count)
        self._fp_window.append((datetime.now(), current))
        if len(self._fp_window) > self._FP_WINDOW_SIZE:
            self._fp_window.pop(0)

        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)

        if current == total and total > 0:
            # Stage 1 complete — record Stage 2 start, reset bar
            self._lookup_start_time = datetime.now()
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(0)
            # Immediate label update so there's no stale "Fingerprinting" text
            # while waiting for the first lookup_progress signal
            rps      = self.settings_manager.get_acoustid_rps()
            rps_note = f"{int(rps)} RPS" if rps >= 3 else f"{rps:.1f} RPS  ⚠ reduced"
            self.stage_label.setText(
                f"Stage 2/2: AcoustID lookup  ({rps_note}) — starting..."
            )

    def _on_lookup_progress(self, idx: int, total: int):
        """Stage 2 — AcoustID lookup progress.

        Stores state only. Label is updated by _tick_elapsed.
        Progress bar updated here since Stage 2 is serial (one per tick anyway).
        """
        self._lookup_last_idx   = idx
        self._lookup_last_total = total

        if self._lookup_start_time is None:
            self._lookup_start_time = datetime.now()

        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(idx)

    def _compute_eta(self, current: int, total: int, start_time,
                     stage: int, rps: float = None) -> str:
        """Return a rounded ETA string or '' if not yet calculable.

        Format:
          >= 2 min  →  '  —  ETA ~N min'
          1-2 min   →  '  —  ETA ~1 min'
          < 1 min   →  '  —  ETA < 1 min'
          done/n/a  →  ''

        stage 0 — tag check: pure elapsed-based rate, 1 s warmup.
                  Mutagen reads are uniform so this is accurate quickly.
        stage 1 — fingerprinting: rolling window rate, 3 s warmup.
                  Variable duration per file; window dampens the noise.
        stage 2 — AcoustID lookup: uses RPS directly, 3 s warmup.
                  Token bucket enforces rate precisely.
        """
        remaining = total - current
        if remaining <= 0 or current <= 0 or start_time is None:
            return ""

        elapsed  = (datetime.now() - start_time).total_seconds()
        warmup   = 1.0 if stage == 0 else 3.0
        if elapsed < warmup:
            return ""

        if stage == 2 and rps and rps > 0:
            secs = remaining / rps

        elif stage == 1:
            # Rolling window rate for fingerprinting
            if len(self._fp_window) >= 2:
                oldest_ts, oldest_count = self._fp_window[0]
                newest_ts, newest_count = self._fp_window[-1]
                window_elapsed = (newest_ts - oldest_ts).total_seconds()
                window_count   = newest_count - oldest_count
                if window_elapsed > 0.5 and window_count > 0:
                    rate = window_count / window_elapsed
                else:
                    rate = current / elapsed
            else:
                rate = current / elapsed
            if rate <= 0:
                return ""
            secs = remaining / rate

        else:
            # stage 0 (tag check) or any unknown stage — simple elapsed rate
            rate = current / elapsed
            if rate <= 0:
                return ""
            secs = remaining / rate

        mins = secs / 60.0
        return "  —  ETA " + self._fmt_eta_mins(mins)

    def _on_result(self, filename: str, status: str, score_pct: str):
        # Update running totals counter
        if status in self._run_totals:
            self._run_totals[status] += 1
            lbl = self._totals_labels.get(status)
            if lbl:
                label_text = lbl.text().split(":")[0]
                lbl.setText(f"{label_text}: {self._run_totals[status]:,}")

        row         = self.table.rowCount()
        self.table.insertRow(row)
        file_item   = QTableWidgetItem(filename)
        status_item = QTableWidgetItem(status.upper())
        score_item  = QTableWidgetItem(score_pct)

        colour_map = {
            "tagged":   QColor("#1e3a28"),
            "cached":   QColor("#1a2a3a"),
            "partial":  QColor("#1a1e2e"),   # no artist/title but supplemental tags written
            "skipped":  QColor("#2b2000"),
            "no_match": QColor("#1a1a1a"),
            "error":    QColor("#2b0a0a"),
        }
        text_colour_map = {
            "tagged":   QColor("#4caf82"),
            "cached":   QColor("#4a90d9"),
            "partial":  QColor("#7a8fc4"),   # muted blue — something written, not a full tag
            "skipped":  QColor("#f0c040"),
            "no_match": QColor("#888888"),
            "error":    QColor("#e05050"),
        }
        bg = colour_map.get(status, QColor("#1a1a1a"))
        fg = text_colour_map.get(status, QColor("#f0f0f0"))
        for item in (file_item, status_item, score_item):
            item.setBackground(bg)
            item.setForeground(fg)

        self.table.setItem(row, 0, file_item)
        self.table.setItem(row, 1, status_item)
        self.table.setItem(row, 2, score_item)
        self.table.scrollToBottom()

    def _on_finished(self, stats: dict):
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setVisible(False)
        self.stage_label.setVisible(False)

        self._elapsed_timer.stop()
        completed_time = datetime.now()
        if self._run_start_time:
            elapsed_secs = int((completed_time - self._run_start_time).total_seconds())
            self.bench_elapsed.setText(f"Time taken:  {self._fmt_elapsed(elapsed_secs)}")
        self.bench_eta.setVisible(False)
        self.bench_completed.setText(f"Completed:  {completed_time.strftime('%H:%M:%S')}")

        # ── Error file tracking — store for retry ─────────────────────────────
        self._last_error_files = stats.get("error_files", [])
        error_count = len(self._last_error_files)
        if error_count > 0:
            self.retry_btn.setText(f"↩  RETRY ERRORS ({error_count:,})")
            self.retry_btn.setVisible(True)
            self.retry_btn.setToolTip(
                f"{error_count:,} file(s) returned API errors on this run.\n"
                "Click to re-run just those files.\n\n"
                "If errors persist, try reducing the rate limit in Settings\n"
                "then retry again."
            )
        else:
            self.retry_btn.setText("↩  RETRY ERRORS (0)")
            self.retry_btn.setVisible(False)

        # ── Permission-denied file tracking ───────────────────────────────────
        fail_files = stats.get("tag_write_failed_files", [])
        perm_files = [
            f["file"] for f in fail_files
            if "PermissionError" in (f.get("reason") or "")
            or "Permission denied" in (f.get("reason") or "")
            or "Access is denied" in (f.get("reason") or "")
        ]
        self._last_perm_denied_files = perm_files
        perm_count = len(perm_files)
        if perm_count > 0:
            self.perm_retry_btn.setText(f"🔒  FIX PERMISSIONS ({perm_count:,})")
            self.perm_retry_btn.setVisible(True)
        else:
            self.perm_retry_btn.setText("🔒  FIX PERMISSIONS (0)")
            self.perm_retry_btn.setVisible(False)

        # ── Write failure banner ──────────────────────────────────────────────
        write_fail_count = stats.get("tag_write_failed", 0)
        if write_fail_count > 0:
            fail_files = stats.get("tag_write_failed_files", [])
            by_ext: dict[str, int] = {}
            for f in fail_files:
                ext = f.get("ext", "?")
                by_ext[ext] = by_ext.get(ext, 0) + 1
            ext_summary = "  ".join(
                f"{ext}: {n}" for ext, n in sorted(by_ext.items(), key=lambda kv: -kv[1])
            )
            report_note = ""
            if stats.get("write_failure_report"):
                report_note = f"\n📋 Full report: {stats['write_failure_report']}"

            perm_note = (
                f"\n🔒 {perm_count:,} due to permission errors — click FIX PERMISSIONS to resolve."
                if perm_count > 0 else ""
            )
            self.write_fail_banner.setText(
                f"⚠  {write_fail_count:,} tag write failure(s).  "
                f"Breakdown: {ext_summary}\n"
                f"Common cause: format mismatch or read-only file. "
                f"Check tag_write_failures_*.txt for per-file detail."
                f"{perm_note}"
                f"{report_note}"
            )
            self.write_fail_banner.setStyleSheet(
                "background-color: #2b0a0a; color: #e05050; "
                "border: 1px solid #e05050; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )
            self.write_fail_banner.setVisible(True)
        else:
            self.write_fail_banner.setVisible(False)

        # ── Low confidence batch card ─────────────────────────────────────────
        lc_json = stats.get("low_confidence_json")
        self._refresh_lc_batch_list()   # ensure new batch appears in list
        if lc_json and HAS_LC_MANAGER:
            self._load_lc_batch(lc_json)
        elif lc_json:
            # Manager unavailable — just surface the path
            self._show_lc_card_simple(lc_json, stats.get("skipped_low_confidence", 0))

        # ── Run status ────────────────────────────────────────────────────────
        if stats.get("cancelled"):
            self._set_status("STOPPED", "status_error")
        else:
            self._set_status("COMPLETE", "status_ok")
            # Rename button state is path-driven, not run-result-driven.
            # Show a contextual hint in the status label based on what the run did,
            # but the button itself is already enabled from the path field.
            tagged_ct = stats.get("tagged", 0)
            cached_ct = stats.get("cache_hits", 0)
            partial_ct = stats.get("no_metadata", 0)
            if tagged_ct > 0 or cached_ct > 0:
                parts = []
                if tagged_ct:  parts.append(f"{tagged_ct:,} tagged")
                if cached_ct:  parts.append(f"{cached_ct:,} from cache")
                if partial_ct: parts.append(f"{partial_ct:,} partial")
                self.rename_status.setText(
                    "Rename ready — " + ",  ".join(parts) + "."
                )
            else:
                self.rename_status.setText("Run complete — rename available.")
            self._update_rename_btn()

        self._check_undo_available()

        # ── Summary bar ───────────────────────────────────────────────────────
        api_errors_part = (
            f"   |   API errors: {stats.get('api_errors', 0):,}"
            if stats.get("api_errors", 0) > 0 else ""
        )
        write_fail_part = (
            f"   |   Write failures: {write_fail_count:,}"
            if write_fail_count > 0 else ""
        )
        self.summary_label.setText(
            f"Tagged: {stats.get('tagged', 0):,}   |   "
            f"Low confidence: {stats.get('skipped_low_confidence', 0):,}   |   "
            f"No match: {stats.get('no_match', 0):,}"
            f"{write_fail_part}"
            f"{api_errors_part}"
        )

        # ── Report links in log ───────────────────────────────────────────────
        if stats.get("error_report"):
            self._log(f"📋 API error report: {stats['error_report']}")

    # ═══════════════════════════════════════════════════════════════════
    # Low Confidence Batch
    # ═══════════════════════════════════════════════════════════════════

    # ── LC batch list helpers ────────────────────────────────────────────────

    def _lc_batch_dir(self) -> Path:
        """Return the directory where LC_BATCH_*.json files are stored."""
        return Path.home() / ".dj_library_manager" / "logs" / "tagging" / "lc_batches"

    def _refresh_lc_batch_list(self):
        """Scan for LC_BATCH_*.json files and repopulate the list widget."""
        from PySide6.QtWidgets import QListWidgetItem
        from PySide6.QtCore import Qt as _Qt

        self.lc_batch_list.clear()
        batch_dir = self._lc_batch_dir()
        try:
            files = sorted(
                batch_dir.glob("LC_BATCH_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,   # newest first
            )
        except Exception:
            files = []

        if not files:
            placeholder = QListWidgetItem("No LC batches found — run Tag Finder to generate one.")
            placeholder.setFlags(placeholder.flags() & ~_Qt.ItemIsSelectable)
            placeholder.setForeground(placeholder.foreground())   # keep default muted colour
            self.lc_batch_list.addItem(placeholder)
            self.lc_review_btn.setEnabled(False)
            self.lc_quarantine_all_btn.setEnabled(False)
            return

        for p in files:
            # Display label: human-readable name + file size
            try:
                size_kb = p.stat().st_size / 1024
                size_str = f"{size_kb:.0f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
            except Exception:
                size_str = ""
            label = f"{p.name}   ({size_str})"
            item  = QListWidgetItem(label)
            item.setData(_Qt.UserRole, str(p))   # store full path for retrieval
            item.setToolTip(str(p))
            self.lc_batch_list.addItem(item)

    def _selected_lc_batch_path(self):
        """Return the full path of the currently selected list item, or None."""
        from PySide6.QtCore import Qt as _Qt
        item = self.lc_batch_list.currentItem()
        if item is None:
            return None
        return item.data(_Qt.UserRole)   # None for placeholder row

    def _on_lc_batch_list_click(self, item):
        """Single-click: load the batch and update the card info."""
        from PySide6.QtCore import Qt as _Qt
        path = item.data(_Qt.UserRole)
        if path:
            self._load_lc_batch(path)

    def _on_lc_batch_list_double_click(self, item):
        """Double-click: load the batch then open the review dialog immediately."""
        from PySide6.QtCore import Qt as _Qt
        path = item.data(_Qt.UserRole)
        if path:
            self._load_lc_batch(path)
            self._open_lc_review_dialog()

    def _lc_batch_list_context_menu(self, pos):
        """Right-click context menu for the batch list."""
        from PySide6.QtWidgets import QMenu
        from PySide6.QtCore import Qt as _Qt

        item = self.lc_batch_list.itemAt(pos)
        if item is None:
            return
        path = item.data(_Qt.UserRole)
        if not path:
            return   # placeholder row

        menu = QMenu(self)

        act_open = menu.addAction("🔍  Open (Review)")
        act_open.triggered.connect(lambda: (
            self._load_lc_batch(path),
            self._open_lc_review_dialog(),
        ))

        act_quarantine = menu.addAction("📦  Quarantine All")
        act_quarantine.triggered.connect(lambda: (
            self._load_lc_batch(path),
            self._lc_quarantine_all(),
        ))

        menu.addSeparator()

        act_delete = menu.addAction("🗑  Delete Batch")
        act_delete.triggered.connect(lambda: self._delete_lc_batch(path))

        menu.exec(self.lc_batch_list.viewport().mapToGlobal(pos))

    def _delete_lc_batch(self, path: str):
        """Delete a batch file after confirmation, then refresh the list."""
        reply = QMessageBox.question(
            self,
            "Delete Batch",
            f"Permanently delete this batch file?\n\n{Path(path).name}\n\n"
            "This cannot be undone. The original audio files are not affected.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        try:
            Path(path).unlink()
            self._log(f"🗑  Deleted LC batch: {Path(path).name}")
        except Exception as exc:
            self._log(f"⚠  Could not delete batch: {exc}")
        # Clear loaded state if we just deleted the active batch
        if self._lc_batch_path == path:
            self._lc_entries    = []
            self._lc_batch_path = None
            self._lc_metadata   = {}
            self.lc_info.setText("Select a batch above to load it.")
            self.lc_review_btn.setEnabled(False)
            self.lc_quarantine_all_btn.setEnabled(False)
            self.lc_status.setText("")
            self._set_lc_card_active(False)
        self._refresh_lc_batch_list()

    def _load_lc_batch_dialog(self):
        """Fallback: open file dialog to load any .json batch (e.g. from another location)."""
        default_dir = str(self._lc_batch_dir())
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Low Confidence Batch",
            default_dir,
            "Low Confidence Batch (LC_BATCH_*.json low_confidence_*.json);;All JSON Files (*.json)",
        )
        if path:
            self._load_lc_batch(path)

    def _load_lc_batch(self, json_path: str):
        """Load a low confidence JSON batch and populate the LC card."""
        if not HAS_LC_MANAGER:
            self._show_lc_card_simple(json_path, 0)
            return
        try:
            entries, metadata = LowConfidenceManager.load_batch(json_path)
        except Exception as exc:
            self._log(f"⚠  Could not load low confidence batch: {exc}")
            return

        self._lc_entries   = entries
        self._lc_metadata  = metadata
        self._lc_batch_path = json_path

        count    = len(entries)
        cutoff   = metadata.get("cutoff")
        generated = metadata.get("generated", "")
        cutoff_str = f"{cutoff * 100:.0f}%" if cutoff else "—"
        unreviewed = LowConfidenceManager.unreviewed_count(entries)

        self.lc_info.setText(
            f"Batch: {Path(json_path).name}   |   "
            f"Generated: {generated}   |   "
            f"Threshold: {cutoff_str}   |   "
            f"Tracks: {count:,}   |   "
            f"Unreviewed: {unreviewed:,}"
        )

        self.lc_review_btn.setEnabled(count > 0)
        self.lc_quarantine_all_btn.setEnabled(count > 0)
        self.lc_status.setText("")
        self._set_lc_card_active(True)

        # Sync list selection to the loaded batch so active batch is always visible
        from PySide6.QtCore import Qt as _Qt
        for i in range(self.lc_batch_list.count()):
            item = self.lc_batch_list.item(i)
            if item and item.data(_Qt.UserRole) == json_path:
                self.lc_batch_list.setCurrentItem(item)
                break

    def _show_lc_card_simple(self, json_path: str, count: int):
        """Show the LC card with minimal info when LowConfidenceManager is unavailable."""
        self.lc_info.setText(
            f"Batch: {Path(json_path).name}   |   {count:,} track(s)\n"
            f"Low Confidence Manager not available — install engine/low_confidence_manager.py."
        )
        self.lc_review_btn.setEnabled(False)
        self.lc_quarantine_all_btn.setEnabled(False)
        self._set_lc_card_active(True)

    def _set_lc_card_active(self, active: bool):
        """
        Idle  — card matches the rest of the page; LOAD BATCH is prominent.
        Active — red border and tinted background signal tracks need attention.
        """
        if active:
            self.lc_card.setStyleSheet(
                "QFrame#card { background-color: #1a0505; border: 1px solid #e05050; "
                "border-radius: 3px; }"
            )
        else:
            self.lc_card.setStyleSheet("")

    # Keywords in a suggested title that indicate an almost-certain wrong match.
    # Mashups, bootlegs, and DJ edits share fingerprint energy with source tracks —
    # AcoustID often returns the source track's metadata for them, which is wrong.
    # Files whose SUGGESTED title (from AcoustID DB) contains any of these tokens
    # are auto-set to Skip in the review dialog. The user can always override.
    _AUTOSKIP_TITLE_TOKENS = frozenset({
        "mashup", "mega mix", "megamix", "megamash", "mash up",
        "bootleg", "blend", "blends",
        "vs.", "vs ",
        "feat.", "feat ",
        "edit", "re-edit",
        "re edit",
        "medley",
        "mix show",
        "mixshow",
    })

    @classmethod
    def _title_needs_autoskip(cls, title: str) -> bool:
        """Return True if the suggested title contains a mashup/edit/blend token.

        Matched case-insensitively on the full title string. Uses word-boundary-
        style checks so "medley" doesn't match "remedy". Short tokens that could
        appear as word fragments (e.g. "edit") are checked with surrounding
        whitespace or parentheses to avoid false positives on "credited" etc.
        """
        if not title:
            return False
        t = title.lower()
        # Tokens that are safe to substring-match anywhere (unlikely to false-positive)
        broad = {"mashup", "megamix", "mega mix", "megamash", "mash up",
                 "bootleg", "medley", "mixshow", "mix show", "blend", "blends"}
        for tok in broad:
            if tok in t:
                return True
        # Tokens that need word-boundary protection
        guarded = [r"\bvs\b", r"\bfeat\b", r"\bedit\b", r"\bre.?edit\b",
                   r"\bre edit\b"]
        for pat in guarded:
            if re.search(pat, t):
                return True
        return False

    # Minimum token-sort similarity ratio (0–1) between the normalised filename
    # stem and the normalised "artist title" AcoustID suggestion for a track to
    # be auto-set to Apply. 0.75 means 75% of characters must be shared once
    # both strings are sorted by token — robust to word-order differences and
    # minor punctuation variation (feat., parentheses, dashes).
    _AUTOMATCH_THRESHOLD = 0.75

    # Qualifier words that commonly appear in AcoustID DB titles but carry no
    # identity information for filename matching. Stripped from the suggestion
    # side only — the filename may not have them, which is intentional.
    _QUALIFIER_RE = re.compile(
        r'\b(?:album|radio|single|extended|original|club|instrumental|'
        r'acoustic|live|remix|edit|version|mix|cut|dub|vip|remaster(?:ed)?|'
        r'explicit|clean|feat|ft)\b',
        re.IGNORECASE,
    )

    @classmethod
    def _filename_match_score(cls, filename_stem: str, artist: str, title: str) -> float:
        """Return a 0–1 similarity score between the filename stem and the
        AcoustID-suggested artist+title.

        Handles three real-world DJ library patterns that a naive comparison fails on:

        1. Dotted acronym artists (V.I.C, M.I.A., D.J. Khaled, A$AP Rocky):
           Dots between single letters are collapsed before tokenising so
           "V.I.C" → "vic" which matches "vic" in the filename.

        2. Version qualifiers in AcoustID titles ("(Album Version)", "(Radio Edit)",
           "(Original Mix)"):
           Stripped from the suggestion side via _QUALIFIER_RE before tokenising.
           These words are absent from most filenames and would dilute the score.

        3. Converter watermarks and junk tokens in filenames
           ("Mp3Convert.Io", "320Kbps", random hash strings):
           Short tokens (≤2 chars) are dropped unless they appear on both sides.
           This keeps meaningful short tokens (artist initials that survived
           acronym collapse) while removing single-letter punctuation artifacts.

        Two-pass scoring via stdlib difflib:
          Pass 1 — token-set containment: what fraction of cleaned suggestion
                   tokens appear in the cleaned filename tokens?
          Pass 2 — character-level token-sort ratio via SequenceMatcher.
          Final score = max(containment, char_ratio).

        Returns 0.0 if either side is empty after normalisation.
        """
        import difflib

        def _tok(s: str, strip_qualifiers: bool = False) -> set:
            s = s.lower()
            # Strip audio extension if stem accidentally includes it
            s = re.sub(r'\.(?:mp3|flac|aiff?|wav|m4a|ogg|wma|aac)$', '', s)
            # Collapse dotted acronyms before stripping punctuation:
            # X.X.X. → XXX so "V.I.C" → "vic", "D.J." → "dj", "A.S.A.P." → "asap"
            s = re.sub(r'(?<!\w)([a-z])\.([a-z])\.([a-z])\.?', r'\1\2\3', s)
            s = re.sub(r'(?<!\w)([a-z])\.([a-z])\.?', r'\1\2', s)
            # Strip qualifier noise from the suggestion side only
            if strip_qualifiers:
                s = cls._QUALIFIER_RE.sub(' ', s)
            # Remove remaining punctuation
            s = re.sub(r'[\-–—_\(\)\[\]\{\}\.,:;!?/\\\'"]', ' ', s)
            s = re.sub(r'\s+', ' ', s).strip()
            return set(s.split())

        file_tokens = _tok(filename_stem, strip_qualifiers=False)
        sugg_tokens = _tok(f"{artist or ''} {title or ''}", strip_qualifiers=True)

        if not file_tokens or not sugg_tokens:
            return 0.0

        # Drop short tokens (≤2 chars) that are almost always punctuation
        # artifacts or converter watermarks — unless they appear on both sides,
        # in which case they're meaningful (e.g. shared artist initials like "dj").
        shared_raw   = file_tokens & sugg_tokens
        file_cleaned = {t for t in file_tokens if len(t) > 2 or t in shared_raw}
        sugg_cleaned = {t for t in sugg_tokens if len(t) > 2 or t in shared_raw}

        if not file_cleaned or not sugg_cleaned:
            return 0.0

        # Pass 1: what fraction of suggestion tokens appear in the filename?
        containment = len(sugg_cleaned & file_cleaned) / len(sugg_cleaned)

        # Pass 2: character-level ratio on sorted token strings
        char_ratio = difflib.SequenceMatcher(
            None,
            " ".join(sorted(file_cleaned)),
            " ".join(sorted(sugg_cleaned)),
            autojunk=False,
        ).ratio()

        return max(containment, char_ratio)
    def _open_lc_review_dialog(self):
        """Open a per-track review dialog letting the user set action per entry.

        Enhancements vs original:
          - Row number column (#) so users know progress through the list
          - "Not reviewed" is treated as Skip on save (unreviewed ≠ skipped is
            confusing — if the user saves without reviewing something they want
            it left alone, which is Skip)
          - Titles containing mashup/bootleg/blend/edit keywords are auto-set
            to Skip with an amber row tint — AcoustID almost always returns
            the source-track metadata for these files, which would be wrong
        """
        if not self._lc_entries:
            return

        from PySide6.QtWidgets import (
            QDialog, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
            QComboBox, QHeaderView, QDialogButtonBox, QAbstractItemView
        )

        n_entries   = len(self._lc_entries)
        dlg         = QDialog(self)
        dlg.setWindowTitle(f"Review Low Confidence Tracks — {n_entries:,} files")
        dlg.setMinimumSize(1300, 750)
        dlg_layout  = QVBoxLayout(dlg)
        dlg_layout.setContentsMargins(16, 16, 16, 16)
        dlg_layout.setSpacing(12)

        hint = QLabel(
            "Set an action for each track. APPLY TAGS writes the matched tags to the file. "
            "QUARANTINE moves the file to _QUARANTINE/_LOW_CONFIDENCE_TAGS/ for manual review. "
            "SKIP leaves the file untouched. "
            "✓ Tracks auto-set to Apply — filename closely matches the AcoustID suggestion. "
            "⚡ Tracks auto-set to Skip — suggested title contains a mashup/bootleg/edit keyword."
        )
        hint.setObjectName("subheading")
        hint.setWordWrap(True)
        dlg_layout.addWidget(hint)

        # ── "Hide already applied" checkbox ──────────────────────────────────
        from PySide6.QtWidgets import QCheckBox
        already_applied_ct = sum(
            1 for e in self._lc_entries if e.get("action") == ACTION_APPLY
        )
        hide_applied_chk = QCheckBox(
            f"Hide already-applied tracks ({already_applied_ct:,})"
        )
        hide_applied_chk.setChecked(already_applied_ct > 0)
        hide_applied_chk.setToolTip(
            "Tracks whose action is already set to Apply Tags are hidden by default.\n"
            "Uncheck to show them — they remain in the batch and can still be changed."
        )
        dlg_layout.addWidget(hide_applied_chk)

        # 7 columns: # | Score | File | Best Match | Album | Action | Play
        tbl = QTableWidget(n_entries, 7)
        tbl.setHorizontalHeaderLabels(["#", "Score", "File", "Best Match", "Album", "Action", "▶"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)  # #
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)  # Score
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)           # File
        tbl.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)           # Best Match
        tbl.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)  # Album
        tbl.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)  # Action
        tbl.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)  # Play
        tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        tbl.setAlternatingRowColors(True)
        tbl.verticalHeader().setVisible(False)

        # Row tint colours — QColor is already imported at module level
        _AUTOSKIP_BG   = QColor("#2a2000")   # dark amber — auto-skip rows
        _AUTOSKIP_FG   = QColor("#f0c040")   # WARNING yellow
        _AUTOMATCH_BG  = QColor("#002010")   # dark green — auto-apply rows
        _AUTOMATCH_FG  = QColor("#4caf82")   # SUCCESS green

        action_combos      = []
        auto_skipped       = 0
        auto_applied       = 0
        already_applied_rows = []   # row indices where action was Apply at open time
        for row, entry in enumerate(self._lc_entries):
            suggested_title  = entry.get("title", "") or ""
            suggested_artist = entry.get("artist", "") or ""
            filename_stem    = Path(entry["file"]).stem

            is_autoskip  = self._title_needs_autoskip(suggested_title)
            # Auto-apply: filename closely matches suggestion AND not a mashup/edit
            match_score  = (
                0.0 if is_autoskip else
                self._filename_match_score(filename_stem, suggested_artist, suggested_title)
            )
            is_automatch = match_score >= self._AUTOMATCH_THRESHOLD

            if is_autoskip:
                auto_skipped += 1
            # auto_applied counted after combo decision (see below)

            # ── Column 0: row number ──────────────────────────────────────
            num_item = QTableWidgetItem(f"{row + 1}")
            num_item.setTextAlignment(Qt.AlignCenter)
            num_item.setToolTip(f"Track {row + 1} of {n_entries:,}")
            tbl.setItem(row, 0, num_item)

            # ── Column 1: score ───────────────────────────────────────────
            score_item = QTableWidgetItem(f"{entry.get('score', 0) * 100:.1f}%")
            tbl.setItem(row, 1, score_item)

            # ── Column 2: filename ────────────────────────────────────────
            fname     = Path(entry["file"]).name
            if is_autoskip:
                disp_name = "⚡ " + fname
            elif is_automatch:
                disp_name = "✓ " + fname
            else:
                disp_name = fname
            file_item = QTableWidgetItem(disp_name)
            file_item.setToolTip(
                entry["file"] + (
                    f"\nFilename match score: {match_score:.0%}" if is_automatch else ""
                )
            )
            tbl.setItem(row, 2, file_item)

            # ── Column 3: AcoustID best match ─────────────────────────────
            match_text = f"{entry.get('artist', '—')} — {entry.get('title', '—')}"
            match_item = QTableWidgetItem(match_text)
            tbl.setItem(row, 3, match_item)

            # ── Column 4: album ───────────────────────────────────────────
            album_item = QTableWidgetItem(entry.get("album", "—"))
            tbl.setItem(row, 4, album_item)

            # ── Column 5: action combo ────────────────────────────────────
            # Decide action first — tinting is applied afterwards so it always
            # matches what the combo actually shows.
            #
            # Priority:
            #   1. ACTION_APPLY or ACTION_QUARANTINE — explicit saved decision,
            #      always respected.
            #   2. auto-skip  — mashup/edit keyword; fires even if saved as Skip
            #      (re-confirming the right call on re-open).
            #   3. auto-match — filename corroborates AcoustID; fires when action
            #      is None OR Skip, because unreviewed entries collapse to Skip on
            #      save and that must not suppress the auto-match on re-open.
            #   4. Skip       — explicitly saved as Skip after being reviewed.
            #      Treated as a real decision only when NOT caught by auto-match
            #      (i.e. is_automatch is False).
            #   5. Unreviewed — action is None, no auto rule fired.
            saved_action = entry.get("action")
            combo = QComboBox()
            combo.addItems(["— Not reviewed —", "Apply Tags", "Quarantine", "Skip"])

            if saved_action == ACTION_APPLY:
                combo.setCurrentIndex(1)
                already_applied_rows.append(row)
            elif saved_action == ACTION_QUARANTINE:
                combo.setCurrentIndex(2)
            elif is_autoskip:
                # Mashup/edit keyword — Skip regardless of saved state
                combo.setCurrentIndex(3)
            elif is_automatch:
                # Filename matches — Apply regardless of whether saved as Skip
                # (unreviewed entries are saved as Skip, so Skip ≠ explicit decision)
                combo.setCurrentIndex(1)
            elif saved_action == ACTION_SKIP:
                # Explicit Skip that didn't trigger auto-match — respect it
                combo.setCurrentIndex(3)
            else:
                combo.setCurrentIndex(0)   # unreviewed

            # ── Row tinting: mirrors the combo decision ───────────────────
            # Determined after combo so colour always matches the shown action.
            final_idx = combo.currentIndex()
            if is_autoskip:
                # Amber for autoskip regardless — always a warning
                tint_bg, tint_fg = _AUTOSKIP_BG, _AUTOSKIP_FG
            elif is_automatch and final_idx == 1:
                # Green only when auto-match actually resulted in Apply
                tint_bg, tint_fg = _AUTOMATCH_BG, _AUTOMATCH_FG
            else:
                tint_bg, tint_fg = None, None

            if tint_bg is not None:
                for col in range(5):
                    item = tbl.item(row, col)
                    if item:
                        item.setBackground(tint_bg)
                        item.setForeground(tint_fg)

            # Set filename prefix now that combo decision and tint are final
            if is_autoskip:
                tbl.item(row, 2).setText("⚡ " + fname)
            elif is_automatch and final_idx == 1:
                tbl.item(row, 2).setText("✓ " + fname)
            # else: plain fname already set

            if is_automatch and final_idx == 1:
                auto_applied += 1

            tbl.setCellWidget(row, 5, combo)
            action_combos.append(combo)

            # ── Column 6: play button ─────────────────────────────────────
            play_btn = QPushButton("▶")
            play_btn.setFixedSize(32, 24)
            play_btn.setToolTip("Open in default media player")
            play_btn.clicked.connect(
                lambda checked, fp=entry["file"]: self._open_in_default_player(fp)
            )
            tbl.setCellWidget(row, 6, play_btn)

        dlg_layout.addWidget(tbl)

        # ── Wire hide-applied checkbox now that tbl and row list are ready ─
        def _apply_hide_filter(checked: bool):
            for r in already_applied_rows:
                tbl.setRowHidden(r, checked)

        hide_applied_chk.toggled.connect(_apply_hide_filter)
        _apply_hide_filter(hide_applied_chk.isChecked())   # apply initial state

        # ── Summary notes: auto-apply and auto-skip ───────────────────────────
        if auto_applied:
            apply_note = QLabel(
                f"✓ {auto_applied:,} track(s) auto-set to Apply — "
                f"filename closely matches the AcoustID suggestion "
                f"(≥{self._AUTOMATCH_THRESHOLD:.0%} similarity). "
                "Override individually if needed."
            )
            apply_note.setObjectName("subheading")
            apply_note.setStyleSheet("color: #4caf82;")
            dlg_layout.addWidget(apply_note)

        if auto_skipped:
            skip_note = QLabel(
                f"⚡ {auto_skipped:,} track(s) auto-set to Skip — "
                "suggested title contains a mashup/bootleg/edit keyword. "
                "Override individually if needed."
            )
            skip_note.setObjectName("subheading")
            skip_note.setStyleSheet("color: #f0c040;")
            dlg_layout.addWidget(skip_note)

        # ── Bulk set row ──────────────────────────────────────────────────────
        bulk_row = QHBoxLayout()
        bulk_lbl = QLabel("Set all to:")
        bulk_lbl.setObjectName("subheading")
        bulk_row.addWidget(bulk_lbl)
        for label, idx in [("Apply Tags", 1), ("Quarantine", 2), ("Skip", 3)]:
            btn = QPushButton(label)
            btn.setFixedHeight(28)
            btn.clicked.connect(
                lambda checked, i=idx, combos=action_combos, t=tbl:
                [c.setCurrentIndex(i) for r, c in enumerate(combos)
                 if not t.isRowHidden(r)]
            )
            bulk_row.addWidget(btn)
        bulk_row.addStretch()
        # Progress note: show how many are still "Not reviewed"
        self._review_progress_lbl = QLabel("")
        self._review_progress_lbl.setObjectName("subheading")
        bulk_row.addWidget(self._review_progress_lbl)

        def _update_progress_lbl():
            remaining = sum(1 for c in action_combos if c.currentIndex() == 0)
            if remaining == 0:
                self._review_progress_lbl.setText("✔ All reviewed")
                self._review_progress_lbl.setStyleSheet("color: #4caf82;")
            else:
                self._review_progress_lbl.setText(f"{remaining:,} not yet reviewed")
                self._review_progress_lbl.setStyleSheet("color: #888888;")

        # Connect progress update to every combo change
        for combo in action_combos:
            combo.currentIndexChanged.connect(lambda _: _update_progress_lbl())
        _update_progress_lbl()   # initial state

        dlg_layout.addLayout(bulk_row)

        btn_box = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btn_box.button(QDialogButtonBox.Save).setText("Save Actions")
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        dlg_layout.addWidget(btn_box)

        if dlg.exec() != QDialog.Accepted:
            return

        # Write actions back to entries.
        # Index 0 ("Not reviewed") is treated as Skip — if the user saves without
        # reviewing a track they want it left alone, which is exactly what Skip does.
        # Leaving it as None (unreviewed) would exclude it from the batch actions
        # and the count would be confusing.
        action_map = {0: ACTION_SKIP, 1: ACTION_APPLY, 2: ACTION_QUARANTINE, 3: ACTION_SKIP}
        for i, combo in enumerate(action_combos):
            self._lc_entries[i]["action"] = action_map.get(combo.currentIndex(), ACTION_SKIP)

        # Persist to disk
        if self._lc_batch_path:
            LowConfidenceManager.save_batch(
                self._lc_entries, self._lc_batch_path, self._lc_metadata
            )

        unreviewed = LowConfidenceManager.unreviewed_count(self._lc_entries)
        apply_ct   = len(LowConfidenceManager.entries_by_action(self._lc_entries, ACTION_APPLY))
        q_ct       = len(LowConfidenceManager.entries_by_action(self._lc_entries, ACTION_QUARANTINE))
        skip_ct    = len(LowConfidenceManager.entries_by_action(self._lc_entries, ACTION_SKIP))
        self.lc_status.setText(
            f"Actions saved — Apply: {apply_ct:,}  Quarantine: {q_ct:,}  "
            f"Skip: {skip_ct:,}  Unreviewed: {unreviewed:,}"
        )
        # Update info line to refresh unreviewed count
        self.lc_info.setText(
            self.lc_info.text().rsplit("Unreviewed:", 1)[0]
            + f"Unreviewed: {unreviewed:,}"
        )
    def _lc_quarantine_all(self):
        """Set all entries to quarantine action and execute immediately."""
        if not self._lc_entries or not HAS_LC_MANAGER:
            return

        count = len(self._lc_entries)
        reply = QMessageBox.question(
            self, "Quarantine All Low-Confidence Files",
            f"Move all {count:,} low-confidence files to:\n"
            f"  <source>/_QUARANTINE/_LOW_CONFIDENCE_TAGS/\n\n"
            f"Their relative folder structure will be preserved.\n\n"
            "Proceed?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        for entry in self._lc_entries:
            entry["action"] = ACTION_QUARANTINE

        self._execute_lc_actions()

    def _execute_lc_actions(self):
        """Execute all pending LC actions (apply / quarantine) in a TaskRunner."""
        if not self._lc_entries or not HAS_LC_MANAGER:
            return

        self.lc_review_btn.setEnabled(False)
        self.lc_quarantine_all_btn.setEnabled(False)
        self.lc_status.setText("Working…")

        # Determine quarantine root from profile source path
        quarantine_root = self.settings_manager.get_quarantine_dir_for_source(
            self._last_source
        )

        # Run in TaskRunner to avoid blocking the UI
        entries      = list(self._lc_entries)
        source_root  = self._last_source

        def _run():
            succeeded_apply, failed_apply = LowConfidenceManager.apply_tags_for_entries(entries)
            moved, failed_q = LowConfidenceManager.quarantine_entries(
                entries, quarantine_root, source_root
            )
            skipped = LowConfidenceManager.entries_by_action(entries, ACTION_SKIP)
            skipped += LowConfidenceManager.entries_by_action(entries, None)
            report  = LowConfidenceManager.write_session_report(
                succeeded_apply, failed_apply, moved, failed_q, skipped
            )
            return succeeded_apply, failed_apply, moved, failed_q, skipped, report

        self._lc_runner = TaskRunner(_run)
        self._lc_runner.finished_signal.connect(self._on_lc_actions_done)
        self._lc_runner.start()

    def _on_lc_actions_done(self, result):
        succeeded_apply, failed_apply, moved, failed_q, skipped, report = result or (
            [], [], [], [], [], None
        )

        total_ok  = len(succeeded_apply) + len(moved)
        total_err = len(failed_apply) + len(failed_q)

        self.lc_status.setText(
            f"✔  Done — "
            f"Tags applied: {len(succeeded_apply):,}  "
            f"Quarantined: {len(moved):,}  "
            f"Skipped: {len(skipped):,}"
            + (f"  ✘ Errors: {total_err:,}" if total_err else "")
        )
        if report:
            self._log(f"📋 Low confidence session report: {report}")
        if total_err:
            self._log(
                f"⚠  {total_err:,} low-confidence operation(s) failed — see session report."
            )

        self.lc_review_btn.setEnabled(True)
        # Clear entries so the card can't be re-executed without reloading
        self._lc_entries = []
        self.lc_quarantine_all_btn.setEnabled(False)
        self._set_lc_card_active(False)

    def _open_in_default_player(self, file_path: str):
        """Open the audio file in the OS default application. Cross-platform."""
        import subprocess, sys
        try:
            if sys.platform.startswith("linux"):
                subprocess.Popen(["xdg-open", file_path])
            elif sys.platform == "darwin":
                subprocess.Popen(["open", file_path])
            elif sys.platform == "win32":
                os.startfile(file_path)   # type: ignore[attr-defined]
            else:
                self._log(f"\u26a0  Cannot open file — unsupported platform: {sys.platform}")
        except Exception as exc:
            self._log(f"\u26a0  Could not open in default player: {exc}\n   Path: {file_path}")

    def _handle_perm_errors(self):
        """
        Permission-denied recovery flow:
          1. Show dialog with OS-specific fix instructions.
          2. On OK, re-run AcoustID pipeline on just the permission-denied files.
        """
        if not self._last_perm_denied_files:
            return

        import sys
        count = len(self._last_perm_denied_files)
        paths_preview = "\n".join(
            f"  {Path(f).name}" for f in self._last_perm_denied_files[:10]
        )
        if count > 10:
            paths_preview += f"\n  \u2026 and {count - 10:,} more"

        if sys.platform.startswith("linux") or sys.platform == "darwin":
            fix_instructions = 'Run:  chmod -R u+w "<your music folder>"  then click OK.'
        elif sys.platform == "win32":
            fix_instructions = (
                "Right-click files in Explorer \u2192 Properties \u2192 Security "
                "and grant your user Write permission, then click OK."
            )
        else:
            fix_instructions = "Ensure the files are not read-only, then click OK."

        reply = QMessageBox.question(
            self,
            f"Permission Denied \u2014 {count:,} file(s)",
            f"{count:,} file(s) could not be tagged \u2014 permission denied:\n\n"
            f"{paths_preview}\n\n"
            f"How to fix:\n{fix_instructions}\n\n"
            "Click OK after fixing permissions to retry just these files.\n"
            "Click Cancel to dismiss.",
            QMessageBox.Ok | QMessageBox.Cancel,
            QMessageBox.Cancel,
        )
        if reply != QMessageBox.Ok:
            return

        api_key = self.settings_manager.get_setting("acoustid_api_key") or ""
        if not api_key:
            self._log("\u26a0  No AcoustID API key set \u2014 cannot retry.")
            return

        thresh = self.settings_manager.get_active_thresholds()
        rps    = self.settings_manager.get_acoustid_rps()
        _sep = "\u2500" * 48
        self._log(
            f"\n{_sep}\n"
            f"\U0001f512  PERMISSION RETRY \u2014 {count:,} file(s)\n"
            f"{_sep}"
        )
        self._start_runner(
            source_path=self._last_source or "",
            api_key=api_key, thresh=thresh, rps=rps,
            retry_files=self._last_perm_denied_files,
        )

    # ═══════════════════════════════════════════════════════════════════
    # Rename Logic
    # ═══════════════════════════════════════════════════════════════════

    def _rename_source_path(self) -> str:
        """Return the effective folder path for rename operations.

        Priority:
          1. Whatever is typed / browsed into the path field
          2. _last_source (set when a Tag Finder run starts)
          3. Empty string (button will be disabled — no path available)

        This is the single source of truth for the rename path. All rename
        methods call this instead of reading _last_source directly.
        """
        if self._rename_path_field is not None:
            text = self._rename_path_field.text().strip()
            if text:
                return text
        return self._last_source or ""

    def _update_rename_btn(self):
        """Enable the rename button iff a folder path is available.

        Called on: path field text change, profile load, run start/finish.
        Does NOT require a Tag Finder run to have completed — any valid path
        is sufficient.
        """
        has_path = bool(self._rename_source_path())
        self.rename_btn.setEnabled(has_path)

    def _browse_rename_path(self):
        """Let the user pick a folder to rename, independent of any run."""
        start = self._rename_source_path() or str(Path.home())
        folder = QFileDialog.getExistingDirectory(
            self, "Choose folder to rename", start
        )
        if folder:
            self._rename_path_field.setText(folder)
            # _update_rename_btn is connected to textChanged — fires automatically

    def run_rename_preview(self):
        """Dry-run rename_files_to_tags with live progress bar + ETA."""
        source = self._rename_source_path()
        if not source:
            self._log("⚠  No folder set — enter a path or browse to a folder above.")
            return

        self._rename_phase      = "scan"
        self._rename_start_time = datetime.now()
        self.rename_btn.setEnabled(False)
        self.undo_rename_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.rename_status.setText("Scanning tags…")
        self.stage_label.setText("Rename scan: walking library…")
        self.stage_label.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(0)    # indeterminate until file count known
        self.progress_bar.setVisible(True)
        self._log(f"\n{'─' * 48}\nScanning for rename candidates in:\n  {source}")

        self._rename_runner = RenameRunner("scan", root=source)
        self._rename_runner.progress.connect(self._on_rename_progress)
        self._rename_runner.finished.connect(self._on_rename_preview_done)
        self._rename_runner.start()

    def _on_rename_progress(self, current: int, total: int, filename: str):
        """Handle per-file progress from RenameRunner — update bar + ETA label.

        ``total`` is 0 during the initial file-walk phase of a scan (file count
        not yet known). Once the walk completes the engine emits real totals.
        """
        if total <= 0:
            # Still walking — indeterminate spinner, no ETA yet
            self.progress_bar.setMaximum(0)
            phase_label = "Rename scan" if self._rename_phase == "scan" else "Renaming"
            self.stage_label.setText(f"{phase_label}: reading tags…  {filename}")
            self.rename_status.setText(f"{phase_label}…")
            return

        # Real progress — deterministic bar
        if self.progress_bar.maximum() != total:
            self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)

        # ETA calculation — simple elapsed / done * remaining
        eta_str = "—"
        if self._rename_start_time and current > 1:
            elapsed = (datetime.now() - self._rename_start_time).total_seconds()
            rate    = current / elapsed           # files per second
            remaining = total - current
            if rate > 0:
                secs = remaining / rate
                if secs < 60:
                    eta_str = "< 1 min"
                else:
                    eta_str = f"{int(secs / 60)} min"

        phase_label = "Rename scan" if self._rename_phase == "scan" else "Renaming"
        self.stage_label.setText(
            f"{phase_label}:  {current:,} / {total:,}   ETA {eta_str}   {filename}"
        )
        self.rename_status.setText(
            f"{phase_label}: {current:,} / {total:,}  —  ETA {eta_str}"
        )

    def _rename_hide_progress(self):
        """Hide shared progress bar and stage label after a rename phase ends."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(100)
        self.stage_label.setVisible(False)
        self.stage_label.setText("")
        self.stop_btn.setEnabled(False)

    def _on_rename_preview_done(self, planned):
        self._rename_hide_progress()

        planned         = planned or []
        to_rename       = [(o, d) for o, d in planned if Path(o).name != Path(d).name]
        already_correct = len(planned) - len(to_rename)

        if already_correct:
            self._log(f"  {already_correct:,} files already have correct filenames — skipped.")

        if not to_rename:
            self._log("✔  All filenames already match their tags. Nothing to rename.")
            diag_dir   = Path.home() / ".dj_library_manager" / "logs"
            diag_files = sorted(diag_dir.glob("rename_scan_diag_*.txt"),
                                key=lambda p: p.stat().st_mtime, reverse=True)
            if diag_files:
                self._log(f"📋 Scan diagnostic (check for skipped files): {diag_files[0]}")
            self.rename_status.setText("All filenames already correct.")
            self._update_rename_btn()
            self._check_undo_available()
            return

        self._log(f"Found {len(to_rename):,} files to rename. Showing preview...")
        diag_dir   = Path.home() / ".dj_library_manager" / "logs"
        diag_files = sorted(diag_dir.glob("rename_scan_diag_*.txt"),
                            key=lambda p: p.stat().st_mtime, reverse=True)
        if diag_files:
            self._log(f"📋 Scan diagnostic: {diag_files[0]}")

        from PySide6.QtWidgets import (
            QDialog, QVBoxLayout, QListWidget, QDialogButtonBox
        )
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Rename Preview — {len(to_rename):,} files")
        dlg.setMinimumSize(700, 500)
        dlg_layout = QVBoxLayout(dlg)
        dlg_layout.setSpacing(12)
        dlg_layout.setContentsMargins(16, 16, 16, 16)

        summary = QLabel(
            f"{len(to_rename):,} files will be renamed to  Artist — Title  format.\n"
            f"Review below, then click Apply to proceed."
        )
        summary.setObjectName("subheading")
        summary.setWordWrap(True)
        dlg_layout.addWidget(summary)

        list_widget = QListWidget()
        list_widget.setAlternatingRowColors(True)
        for orig, dest in to_rename:
            list_widget.addItem(f"{Path(orig).name}  →  {Path(dest).name}")
        dlg_layout.addWidget(list_widget)

        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.button(QDialogButtonBox.Ok).setText(f"Apply {len(to_rename):,} renames")
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        dlg_layout.addWidget(btn_box)

        if dlg.exec() != QDialog.Accepted:
            self._log("Rename cancelled by user.")
            self.rename_status.setText("Rename cancelled.")
            self._update_rename_btn()
            self._check_undo_available()
            return

        # ── Launch apply phase with its own progress tracking ─────────────
        self._rename_phase      = "apply"
        self._rename_start_time = datetime.now()
        self.stop_btn.setEnabled(True)
        self.rename_status.setText(f"Renaming {len(to_rename):,} files…")
        self.stage_label.setText(f"Renaming:  0 / {len(to_rename):,}")
        self.stage_label.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(len(to_rename))
        self.progress_bar.setVisible(True)
        self._log(f"Applying {len(to_rename):,} renames...")

        self._rename_runner = RenameRunner("apply", pairs=to_rename)
        self._rename_runner.progress.connect(self._on_rename_progress)
        self._rename_runner.finished.connect(self._on_rename_apply_done)
        self._rename_runner.start()

    def _on_rename_apply_done(self, result):
        self._rename_hide_progress()
        moved, report = result if isinstance(result, tuple) else (result or [], None)
        self._log(f"✔  Renamed {len(moved):,} files.")
        if report:
            self._log(f"📋 Undo report saved: {report}")
        self.rename_status.setText(f"✔  {len(moved):,} files renamed.")
        self._update_rename_btn()
        self._check_undo_available()

    def run_undo_renames(self):
        """Revert the most recent rename batch using its JSON report."""
        report_dir = Path.home() / ".dj_library_manager" / "logs"
        try:
            reports = sorted(
                report_dir.glob("rename_report_*.json"),
                key=lambda p: p.stat().st_mtime, reverse=True
            )
        except Exception:
            reports = []

        if not reports:
            QMessageBox.information(
                self, "No Undo Report",
                f"No rename reports found.\nExpected location: {report_dir}"
            )
            return

        report_path = str(reports[0])
        self._log(f"\n{'─' * 48}\nUndo: loading report\n  {report_path}")

        import json
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            entries = data.get("renames", [])
        except Exception as e:
            self._log(f"✘  Could not read report: {e}")
            return

        if not entries:
            self._log("Report is empty — nothing to revert.")
            return

        lines = []
        for e in entries:
            dest   = e.get("dest", "")
            orig   = e.get("orig", "")
            marker = " [exists]" if Path(dest).exists() else " [missing]"
            lines.append(f"{Path(dest).name}{marker}  →  {Path(orig).name}")

        preview_text = "\n".join(lines[:300])
        if len(lines) > 300:
            preview_text += f"\n\n... and {len(lines) - 300:,} more"

        undo_preview_header = (
            f"Showing first 300 of {len(entries):,} — all will be reverted."
            if len(lines) > 300
            else f"All {len(entries):,} entries shown below."
        )
        reply = QMessageBox.question(
            self,
            f"Confirm Undo — {len(entries):,} renames",
            f"This will revert {len(entries):,} renames from:\n  {report_path}\n\n"
            f"{undo_preview_header}\n\n{preview_text}\n\nProceed?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            self._log("Undo cancelled by user.")
            return

        self.undo_rename_btn.setEnabled(False)
        self.rename_status.setText("Reverting renames...")

        from engine.tagging import revert_from_report
        self._rename_runner = TaskRunner(revert_from_report, report_path, False)
        self._rename_runner.finished_signal.connect(self._on_undo_done)
        self._rename_runner.output_signal.connect(self._log)
        self._rename_runner.start()

    def _on_undo_done(self, result):
        moved, report = result if isinstance(result, tuple) else (result or [], None)
        self._log(f"✔  Reverted {len(moved):,} renames.")
        if report:
            self._log(f"📋 Revert report saved: {report}")
        self.rename_status.setText(f"✔  {len(moved):,} renames reverted.")
        self._check_undo_available()

    def _check_undo_available(self):
        report_dir = Path.home() / ".dj_library_manager" / "logs"
        try:
            has_report = any(report_dir.glob("rename_report_*.json"))
        except Exception:
            has_report = False
        self.undo_rename_btn.setEnabled(has_report)

    # ═══════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════

    def _log(self, text: str):
        self.log.append(text)
        self.log.moveCursor(QTextCursor.End)

    def _tick_elapsed(self):
        if self._run_start_time:
            elapsed_secs = int((datetime.now() - self._run_start_time).total_seconds())
            self.bench_elapsed.setText(f"Elapsed:  {self._fmt_elapsed(elapsed_secs)}")

        # Overall ETA — shown in benchmark card
        overall_eta = self._compute_overall_eta()
        if overall_eta:
            self.bench_eta.setText(f"Overall ETA:  {overall_eta}")
            self.bench_eta.setVisible(True)

        # Stage label update — driven by timer so rapid-fire progress signals
        # don't cause flickering. Priority order: Stage 2 > Stage 1 > tag check.
        # (tag check happens before Stage 1 so it gets lowest priority here —
        # once Stage 1 starts, tag check is finished.)
        current = self._fp_last_current
        total   = self._fp_last_total

        if self._lookup_start_time is not None and self._lookup_last_total > 0:
            # Stage 2 is active
            idx  = self._lookup_last_idx
            tot  = self._lookup_last_total
            rps  = self.settings_manager.get_acoustid_rps()
            rps_note = f"{int(rps)} RPS" if rps >= 3 else f"{rps:.1f} RPS  ⚠ reduced"
            eta_str  = self._compute_eta(
                idx, tot, self._lookup_start_time, stage=2, rps=rps
            )
            self.stage_label.setText(
                f"Stage 2/2: AcoustID lookup  ({rps_note}) — {idx:,}/{tot:,}{eta_str}"
            )
        elif current > 0 and total > 0 and current < total:
            # Stage 1 is active
            eta_str = self._compute_eta(
                current, total, self._fp_start_time, stage=1
            )
            self.stage_label.setText(
                f"Stage 1/2: Fingerprinting — {current:,}/{total:,}{eta_str}"
            )
        elif (self._tag_check_total > 0
              and self._tag_check_current < self._tag_check_total):
            # Tag check is active
            tc_cur  = self._tag_check_current
            tc_tot  = self._tag_check_total
            eta_str = self._compute_eta(
                tc_cur, tc_tot, self._tag_check_start_time, stage=0
            )
            self.stage_label.setText(
                f"Checking tags — {tc_cur:,}/{tc_tot:,}{eta_str}"
            )

    def _compute_overall_eta(self) -> str:
        """Return overall ETA string for the benchmark card, or '' if not ready.

        During Stage 1:
            remaining_fp_secs  = remaining files / rolling window rate
            stage2_secs        = total_files / rps   (full Stage 2 still ahead)
            overall            = remaining_fp_secs + stage2_secs

        During Stage 2:
            overall            = remaining_lookups / rps   (Stage 1 is done)

        Uses same minute-resolution format as per-stage ETA.
        Returns '' until we have enough data for a meaningful estimate.
        """
        rps = self.settings_manager.get_acoustid_rps()
        if not rps or rps <= 0:
            return ""

        current = self._fp_last_current
        total   = self._fp_last_total

        if self._lookup_start_time is not None and self._lookup_last_total > 0:
            # Stage 2 active — straightforward: remaining lookups / rps
            remaining = self._lookup_last_total - self._lookup_last_idx
            if remaining <= 0:
                return ""
            secs = remaining / rps

        elif current > 0 and total > 0 and self._fp_start_time is not None:
            # Stage 1 active — need to project both remaining Stage 1 and full Stage 2
            elapsed = (datetime.now() - self._fp_start_time).total_seconds()
            if elapsed < 3.0:
                return ""   # too early

            # Stage 1 remaining: use rolling window rate
            if len(self._fp_window) >= 2:
                oldest_ts, oldest_count = self._fp_window[0]
                newest_ts, newest_count = self._fp_window[-1]
                win_elapsed = (newest_ts - oldest_ts).total_seconds()
                win_count   = newest_count - oldest_count
                if win_elapsed > 0.5 and win_count > 0:
                    fp_rate = win_count / win_elapsed
                else:
                    fp_rate = current / elapsed
            else:
                fp_rate = current / elapsed

            if fp_rate <= 0:
                return ""

            fp_remaining_secs = (total - current) / fp_rate

            # Stage 2: full library at configured RPS
            # We use total (all audio files) not current fingerprinted count,
            # since fingerprint failures still go through the lookup queue
            stage2_secs = total / rps

            secs = fp_remaining_secs + stage2_secs
        else:
            return ""

        mins = secs / 60.0
        return self._fmt_eta_mins(mins)

    @staticmethod
    def _fmt_elapsed(total_seconds: int) -> str:
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    @staticmethod
    def _fmt_eta_mins(mins: float) -> str:
        """Format a minute-valued ETA for display.

        < 1 min   →  '< 1 min'
        1–59 min  →  '~N min'
        ≥ 60 min  →  'hh:mm'  (hours and whole minutes, no seconds)
        """
        if mins < 1.0:
            return "< 1 min"
        rounded_mins = int(round(mins))
        if rounded_mins < 60:
            return f"~{rounded_mins} min"
        h = rounded_mins // 60
        m = rounded_mins % 60
        return f"{h:02d}:{m:02d}"

    def _set_status(self, text: str, object_name: str):
        self.status_label.setText(text)
        self.status_label.setObjectName(object_name)
        self.status_label.style().unpolish(self.status_label)
        self.status_label.style().polish(self.status_label)

    def _divider(self):
        line = QFrame()
        line.setObjectName("divider")
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        return line
