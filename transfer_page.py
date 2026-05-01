"""
DJ Library Manager — Transfer Page  (v0.5.0)

Hash-verified file transfer UI. Wraps TransferEngine with a four-state
interface: Idle → Running → Dry Run Complete → Live Run Complete.

State machine:
  IDLE              — paths set, options chosen, only DRY RUN enabled.
  RUNNING           — progress bar live, STOP enabled, controls locked.
  DRY_RUN_COMPLETE  — summary shown, LIVE TRANSFER unlocked, exceptions listed.
  LIVE_COMPLETE     — summary updated to actuals, LIVE TRANSFER re-locked.
                      User must run DRY RUN again to re-enable LIVE TRANSFER.

Profile integration:
  Source path is loaded from the active profile (source_path key).
  Destination is chosen by the user from detected removable drives or Browse —
  it is never auto-populated. Last-used destination is NOT persisted to avoid
  accidentally transferring to the wrong drive next session.

Architectural rules honoured:
  - No blocking operations on the main thread — engine runs in TaskRunner.
  - All OS logic goes through PlatformAdapter.
  - Dry run enforced in UI — LIVE TRANSFER disabled until dry run completes.
  - OVERWRITE mode requires a secondary confirmation dialog before live run.
"""

from __future__ import annotations

import os
from pathlib import Path
from enum import Enum, auto

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QRadioButton, QButtonGroup, QCheckBox, QProgressBar,
    QFileDialog, QMessageBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QSizePolicy, QScrollArea,
)
from PySide6.QtCore import Qt, Signal, QObject, Slot
from PySide6.QtGui import QColor

from engine.command_runner import TaskRunner
from engine.transfer_engine import (
    TransferEngine, TransferReport, CollisionMode, Outcome,
    write_transfer_report,
)

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/platform_adapter.py  →  PlatformAdapter.get_removable_drives()
#             PlatformAdapter.get_os()
# Why: All OS-specific logic (removable drive detection, path guards) must go
#      through PlatformAdapter. Never put OS conditionals directly in UI files.
# If you change get_removable_drives() dict keys or get_os() behaviour,
# update _refresh_drives() and _fmt_drive_label() in this file.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from engine.platform_adapter import PlatformAdapter
except ImportError:
    PlatformAdapter = None

# ── Outcome colours (mirrors theme.py constants) ──────────────────────────────
_COL_FAILED   = "#e05050"
_COL_WARNED   = "#f0c040"
_COL_RENAMED  = "#888888"
_COL_COPIED   = "#4caf82"
_COL_SKIPPED  = "#555555"

_BG_FAILED    = "#2b0a0a"
_BG_WARNED    = "#2b2000"
_BG_RENAMED   = "#1e1e1e"
_BG_COPIED    = "#0d2b1e"
_BG_SKIPPED   = "#161616"

# Outcomes that always appear in the exceptions table
_EXCEPTION_OUTCOMES = {
    Outcome.FAILED, Outcome.PATH_REFUSED, Outcome.PATH_WARNED, Outcome.COLLISION,
}

# ── Page states ───────────────────────────────────────────────────────────────

class _State(Enum):
    IDLE             = auto()
    RUNNING          = auto()
    DRY_RUN_COMPLETE = auto()
    LIVE_COMPLETE    = auto()


# ── Thread-safe progress bridge ───────────────────────────────────────────────
# QMetaObject.invokeMethod with Q_ARG is unreliable in PySide6 — it requires
# exact @Slot type registration and throws a hard C++ exception on mismatch,
# killing the entire process. The safe pattern is a QObject with a proper
# Signal that lives on the main thread. The background thread calls emit()
# which Qt safely marshals across the thread boundary via the event queue.

class _ProgressBridge(QObject):
    """Carries progress updates from the engine thread to the UI thread."""
    progress    = Signal(int, int, str)   # (current, total, path)
    hash_result = Signal(bool, str)       # (verified_ok, filename)
    stage       = Signal(str)             # (message) — prepare/scan phase


# ── Transfer page ─────────────────────────────────────────────────────────────

class TransferPage(QWidget):
    """Transfer page — source → destination with hash verification."""

    def __init__(self, settings_manager, profile_manager):
        super().__init__()
        self.settings_manager = settings_manager
        self.profile_manager  = profile_manager

        self._source_path    = ""
        self._dest_path      = ""
        self._drives         = []       # list of drive dicts from PlatformAdapter
        self._drive_radios   = []       # parallel list of QRadioButton widgets
        self._runner         = None     # TaskRunner reference
        self._last_report    = None     # TransferReport from most recent run
        self._show_all       = False    # exceptions table: show all vs exceptions only
        self._dry_run_done   = False    # guards LIVE TRANSFER button
        self._state          = _State.IDLE

        # Thread-safe progress bridge — lives on main thread, receives signals
        # from engine thread. Must be created before _build_ui().
        self._bridge = _ProgressBridge()
        self._bridge.progress.connect(self._update_progress)
        self._bridge.hash_result.connect(self._show_hash_badge)
        self._bridge.stage.connect(self._update_stage)

        self._build_ui()

        # Load last profile on init
        last = self.settings_manager.get_last_profile()
        if last:
            p = self.profile_manager.load_profile(last)
            if p:
                self._apply_source(p.get("source_path", ""))

    # ═══════════════════════════════════════════════════════════════════
    # Profile integration
    # ═══════════════════════════════════════════════════════════════════

    def set_profile(self, profile_name: str):
        """Called by MainWindow when the active profile changes.

        Only the source path is profile-driven. Destination is always
        chosen manually by the user — never auto-populated from a profile.
        """
        if not profile_name:
            return
        profile = self.profile_manager.load_profile(profile_name)
        if not profile:
            return
        self._apply_source(profile.get("source_path", ""))

    def _apply_source(self, path: str):
        self._source_path = path or ""
        display = self._source_path if self._source_path else "No profile source set"
        self.source_path_label.setText(display)
        # Never tear down mid-run — the source path is already locked in for the
        # current transfer. _reset_to_idle() disables the stop button and clears
        # the runner reference while the background thread is still alive, which
        # causes a crash. Silently skip the reset; when the run finishes it will
        # transition state cleanly via _on_run_finished().
        if self._state != _State.RUNNING:
            self._reset_to_idle()

    # ═══════════════════════════════════════════════════════════════════
    # UI Construction
    # ═══════════════════════════════════════════════════════════════════

    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(24, 20, 24, 20)
        root.setSpacing(16)
        root.setAlignment(Qt.AlignTop)

        # Heading
        heading = QLabel("TRANSFER")
        heading.setObjectName("heading")
        root.addWidget(heading)

        sub = QLabel(
            "Copy your library to an external drive with hash verification — "
            "every file is checked after copying to confirm it arrived intact."
        )
        sub.setObjectName("subheading")
        sub.setWordWrap(True)
        root.addWidget(sub)

        root.addWidget(self._divider())

        # Source → Destination side by side (left to right, like reading)
        path_row = QHBoxLayout()
        path_row.setSpacing(12)
        path_row.addWidget(self._build_source_card(), 1)

        arrow = QLabel("▶")
        arrow.setStyleSheet("color: #444; font-size: 18px;")
        arrow.setAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        arrow.setFixedWidth(24)
        path_row.addWidget(arrow)

        path_row.addWidget(self._build_dest_card(), 1)
        root.addLayout(path_row)

        root.addWidget(self._build_options_card())
        root.addWidget(self._divider())
        root.addLayout(self._build_action_row())
        root.addWidget(self._build_progress_area())
        root.addWidget(self._build_summary_card())
        root.addWidget(self._build_exceptions_card())
        root.addStretch()

        self._set_state(_State.IDLE)

    # ── Source card ───────────────────────────────────────────────────

    def _build_source_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 12, 16, 14)
        layout.setSpacing(8)

        title = QLabel("SOURCE — LIBRARY")
        title.setObjectName("subheading")
        layout.addWidget(title)

        row = QHBoxLayout()
        self.source_path_label = QLabel("No profile source set")
        self.source_path_label.setStyleSheet("color: #888; font-size: 11px;")
        self.source_path_label.setWordWrap(True)
        row.addWidget(self.source_path_label, 1)

        browse_src_btn = QPushButton("BROWSE")
        browse_src_btn.setFixedWidth(90)
        browse_src_btn.setToolTip(
            "Override the profile source path for this transfer only.\n"
            "The profile itself is not changed."
        )
        browse_src_btn.clicked.connect(self._browse_source)
        row.addWidget(browse_src_btn)
        layout.addLayout(row)

        note = QLabel("Source path comes from the active profile.")
        note.setObjectName("subheading")
        layout.addWidget(note)

        return card

    # ── Destination card ──────────────────────────────────────────────

    def _build_dest_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 12, 16, 14)
        layout.setSpacing(10)

        title_row = QHBoxLayout()
        title = QLabel("DESTINATION — EXTERNAL DRIVE")
        title.setObjectName("subheading")
        title_row.addWidget(title)
        title_row.addStretch()

        self.refresh_drives_btn = QPushButton("↻  REFRESH DRIVES")
        self.refresh_drives_btn.setFixedWidth(160)
        self.refresh_drives_btn.setToolTip(
            "Scan for newly plugged-in drives.\n"
            "Click this after connecting a USB drive if it doesn't appear below."
        )
        self.refresh_drives_btn.clicked.connect(self._refresh_drives)
        title_row.addWidget(self.refresh_drives_btn)
        layout.addLayout(title_row)

        # Drive list area — populated by _refresh_drives()
        self.drives_area = QVBoxLayout()
        self.drives_area.setSpacing(6)
        layout.addLayout(self.drives_area)

        self.no_drives_label = QLabel("No removable drives detected.")
        self.no_drives_label.setObjectName("subheading")
        self.drives_area.addWidget(self.no_drives_label)

        layout.addWidget(self._subdivider())

        # Custom path row
        custom_row = QHBoxLayout()
        custom_row.setSpacing(8)

        self.custom_dest_radio = QRadioButton()
        self.custom_dest_radio.setToolTip("Select this to use a manually browsed destination folder.")
        custom_row.addWidget(self.custom_dest_radio)

        self.custom_dest_label = QLabel("Custom path:")
        self.custom_dest_label.setObjectName("subheading")
        custom_row.addWidget(self.custom_dest_label)

        self.custom_dest_path_label = QLabel("None selected")
        self.custom_dest_path_label.setStyleSheet("color: #888; font-size: 11px;")
        self.custom_dest_path_label.setWordWrap(True)
        custom_row.addWidget(self.custom_dest_path_label, 1)

        browse_dest_btn = QPushButton("BROWSE")
        browse_dest_btn.setFixedWidth(90)
        browse_dest_btn.setToolTip(
            "Choose any folder as the destination.\n\n"
            "Use this if your drive isn't detected above — large external SSDs\n"
            "sometimes don't appear in the drive list because the operating system\n"
            "treats them as internal disks. Browse lets you select them directly."
        )
        browse_dest_btn.clicked.connect(self._browse_dest)
        custom_row.addWidget(browse_dest_btn)
        layout.addLayout(custom_row)

        # Not seeing your drive note
        not_seen = QLabel("⚠  Not seeing your drive?  Use Browse — large SSDs sometimes aren't listed.")
        not_seen.setObjectName("subheading")
        not_seen.setStyleSheet("color: #888; font-size: 11px; padding-top: 2px;")
        layout.addWidget(not_seen)

        # All destination radios share one group (drives + custom)
        self._dest_radio_group = QButtonGroup(self)
        self._dest_radio_group.addButton(self.custom_dest_radio, 9999)
        self._dest_radio_group.buttonClicked.connect(self._on_dest_selected)

        # Populate on first build
        self._refresh_drives()

        return card

    # ── Options card ──────────────────────────────────────────────────

    def _build_options_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 10, 16, 10)
        layout.setSpacing(6)

        # Single slim row: collision label → radios → vertical sep → hash checkbox
        options_row = QHBoxLayout()
        options_row.setSpacing(14)

        collision_label = QLabel("IF FILE EXISTS:")
        collision_label.setObjectName("subheading")
        collision_label.setToolTip(
            "A 'collision' happens when a file with the same name already exists\n"
            "in the destination folder.\n\n"
            "SKIP — Leave the existing file untouched. The source file is not copied.\n"
            "       Safe choice. Nothing is overwritten or renamed.\n\n"
            "RENAME — Copy the file anyway, adding _1, _2 etc. to the filename.\n"
            "         Useful when consolidating two libraries that share filenames.\n\n"
            "OVERWRITE — Replace the existing destination file with the source file.\n"
            "            Destructive — the original destination file is gone permanently."
        )
        options_row.addWidget(collision_label)

        self.collision_group = QButtonGroup(self)

        skip_radio = QRadioButton("SKIP  ✓")
        skip_radio.setChecked(True)
        skip_radio.setToolTip(
            "If a file with the same name already exists at the destination,\n"
            "leave it untouched and move on. The safest option.\n"
            "Nothing is overwritten or deleted."
        )
        self.collision_group.addButton(skip_radio, 0)
        options_row.addWidget(skip_radio)

        rename_radio = QRadioButton("RENAME")
        rename_radio.setToolTip(
            "If a file with the same name already exists at the destination,\n"
            "copy the file anyway with _1, _2 etc. added to the filename.\n"
            "Use this when merging two libraries that share track names."
        )
        self.collision_group.addButton(rename_radio, 1)
        options_row.addWidget(rename_radio)

        self.overwrite_radio = QRadioButton("OVERWRITE ⚠")
        self.overwrite_radio.setToolTip(
            "If a file with the same name already exists at the destination,\n"
            "replace it with the version from the source.\n\n"
            "⚠ WARNING — The original destination file is permanently deleted.\n"
            "This cannot be undone. A second confirmation will appear before\n"
            "any live transfer when this mode is selected."
        )
        self.overwrite_radio.setStyleSheet("color: #e05050;")
        self.collision_group.addButton(self.overwrite_radio, 2)
        options_row.addWidget(self.overwrite_radio)

        # Vertical separator
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFixedWidth(1)
        sep.setStyleSheet("background-color: #333; border: none;")
        options_row.addWidget(sep)

        # Hash verify — inline with collision options
        self.verify_hash_cb = QCheckBox("VERIFY HASH")
        self.verify_hash_cb.setChecked(True)
        self.verify_hash_cb.setToolTip(
            "After copying each file, the app re-reads both the original and\n"
            "the copy and compares their SHA256 fingerprints.\n\n"
            "If the fingerprints don't match — meaning the copy is corrupted —\n"
            "the bad copy is deleted and the file is logged as FAILED.\n\n"
            "This catches data corruption caused by bad cables, failing drives,\n"
            "or filesystem errors. Strongly recommended — it adds a small amount\n"
            "of time but guarantees every copied file is bit-perfect.\n\n"
            "Only disable this if speed is critical and you accept the risk."
        )
        options_row.addWidget(self.verify_hash_cb)
        options_row.addStretch()

        self.collision_group.buttonClicked.connect(self._on_collision_changed)
        layout.addLayout(options_row)

        # Overwrite warning banner — shown below the row when OVERWRITE selected
        self.overwrite_banner = QLabel(
            "⚠  OVERWRITE MODE — files at the destination with the same name will be "
            "permanently replaced. A confirmation dialog will appear before any live transfer."
        )
        self.overwrite_banner.setWordWrap(True)
        self.overwrite_banner.setContentsMargins(12, 8, 12, 8)
        self.overwrite_banner.setStyleSheet(
            "background-color: #2b0a0a; color: #e05050; "
            "border: 1px solid #e05050; border-radius: 2px; "
            "font-family: 'Courier New'; font-size: 11px; padding: 8px;"
        )
        self.overwrite_banner.setVisible(False)
        layout.addWidget(self.overwrite_banner)

        return card

    # ── Action row ────────────────────────────────────────────────────

    def _build_action_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.setSpacing(10)

        self.dry_run_btn = QPushButton("DRY RUN")
        self.dry_run_btn.setObjectName("primary")
        self.dry_run_btn.setFixedHeight(36)
        self.dry_run_btn.setToolTip(
            "Simulate the entire transfer without copying any files.\n\n"
            "A Dry Run walks through every file in the source and works out\n"
            "exactly what would happen — which files would be copied, which\n"
            "would be skipped, and any potential problems — then shows you a\n"
            "report before anything touches the destination drive.\n\n"
            "You must complete a Dry Run before the Live Transfer button\n"
            "becomes available. This is intentional."
        )
        self.dry_run_btn.clicked.connect(self._start_dry_run)
        row.addWidget(self.dry_run_btn)

        self.live_btn = QPushButton("⚡  LIVE TRANSFER")
        self.live_btn.setObjectName("success")
        self.live_btn.setFixedHeight(36)
        self.live_btn.setEnabled(False)
        self.live_btn.setToolTip(
            "Copy files from source to destination for real.\n\n"
            "Only available after a successful Dry Run. Review the Dry Run\n"
            "results before proceeding — check for any failures or warnings."
        )
        self.live_btn.clicked.connect(self._start_live_transfer)
        row.addWidget(self.live_btn)

        row.addStretch()

        self.stop_btn = QPushButton("■  STOP")
        self.stop_btn.setObjectName("danger")
        self.stop_btn.setFixedHeight(36)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setToolTip(
            "Stop the current run.\n\n"
            "Files already copied will remain at the destination. The run\n"
            "report will include only the files processed before stopping."
        )
        self.stop_btn.clicked.connect(self._stop_run)
        row.addWidget(self.stop_btn)

        return row

    # ── Progress area ─────────────────────────────────────────────────

    def _build_progress_area(self) -> QFrame:
        frame = QFrame()
        frame.setObjectName("panel")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(0, 4, 0, 4)
        layout.setSpacing(4)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(6)
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        progress_detail_row = QHBoxLayout()
        progress_detail_row.setSpacing(10)

        self.progress_label = QLabel("")
        self.progress_label.setObjectName("subheading")
        self.progress_label.setStyleSheet("color: #888; font-size: 11px;")
        self.progress_label.setVisible(False)
        progress_detail_row.addWidget(self.progress_label, 1)

        # Hash status badge — shown per-file during a live transfer with hash verify on
        self.hash_badge = QLabel("")
        self.hash_badge.setFixedWidth(130)
        self.hash_badge.setAlignment(Qt.AlignCenter)
        self.hash_badge.setStyleSheet(
            "font-family: 'Courier New'; font-size: 11px; "
            "padding: 2px 8px; border-radius: 2px;"
        )
        self.hash_badge.setVisible(False)
        progress_detail_row.addWidget(self.hash_badge)

        layout.addLayout(progress_detail_row)
        return frame

    # ── Summary card ──────────────────────────────────────────────────

    def _build_summary_card(self) -> QFrame:
        self.summary_card = QFrame()
        self.summary_card.setObjectName("card")
        layout = QVBoxLayout(self.summary_card)
        layout.setContentsMargins(16, 12, 16, 14)
        layout.setSpacing(6)

        self.summary_title = QLabel("DRY RUN RESULTS")
        self.summary_title.setObjectName("subheading")
        layout.addWidget(self.summary_title)

        self.summary_label = QLabel("")
        self.summary_label.setStyleSheet("color: #f0f0f0; font-size: 12px;")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.summary_card.setVisible(False)
        return self.summary_card

    # ── Exceptions card ───────────────────────────────────────────────

    def _build_exceptions_card(self) -> QFrame:
        self.exc_card = QFrame()
        self.exc_card.setObjectName("card")
        layout = QVBoxLayout(self.exc_card)
        layout.setContentsMargins(16, 12, 16, 14)
        layout.setSpacing(8)

        header_row = QHBoxLayout()
        self.exc_title = QLabel("FAILURES & WARNINGS")
        self.exc_title.setObjectName("subheading")
        header_row.addWidget(self.exc_title)
        header_row.addStretch()

        self.show_all_btn = QPushButton("SHOW ALL FILES")
        self.show_all_btn.setFixedWidth(150)
        self.show_all_btn.setToolTip(
            "Toggle between showing only exceptions (failures, warnings, collisions)\n"
            "and showing every file in the transfer."
        )
        self.show_all_btn.clicked.connect(self._toggle_show_all)
        header_row.addWidget(self.show_all_btn)
        layout.addLayout(header_row)

        self.exc_table = QTableWidget(0, 3)
        self.exc_table.setHorizontalHeaderLabels(["Outcome", "File", "Note"])
        self.exc_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.exc_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.exc_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.exc_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.exc_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.exc_table.setAlternatingRowColors(False)
        self.exc_table.setMinimumHeight(180)
        layout.addWidget(self.exc_table)

        self.report_path_label = QLabel("")
        self.report_path_label.setObjectName("subheading")
        self.report_path_label.setStyleSheet("color: #555; font-size: 11px;")
        self.report_path_label.setWordWrap(True)
        layout.addWidget(self.report_path_label)

        self.exc_card.setVisible(False)
        return self.exc_card

    # ═══════════════════════════════════════════════════════════════════
    # Drive detection
    # ═══════════════════════════════════════════════════════════════════

    def _refresh_drives(self):
        """Scan for removable drives and rebuild the drive list UI."""
        # Remove existing drive radio buttons from layout and group
        for rb in self._drive_radios:
            self.drives_area.removeWidget(rb)
            self._dest_radio_group.removeButton(rb)
            rb.deleteLater()
        self._drive_radios.clear()
        self._drives.clear()

        if PlatformAdapter:
            try:
                self._drives = PlatformAdapter.get_removable_drives()
            except Exception:
                self._drives = []

        if not self._drives:
            self.no_drives_label.setVisible(True)
        else:
            self.no_drives_label.setVisible(False)
            for idx, drive in enumerate(self._drives):
                rb = QRadioButton(self._fmt_drive_label(drive))
                rb.setToolTip(
                    f"Mountpoint: {drive['mountpoint']}\n"
                    f"Filesystem: {drive['fstype'] or 'unknown'}\n"
                    f"Capacity:   {_fmt_bytes(drive['size_bytes'])}\n"
                    f"Free space: {_fmt_bytes(drive['free_bytes'])}"
                )
                self._dest_radio_group.addButton(rb, idx)
                self._dest_radio_group.buttonClicked.connect(self._on_dest_selected)
                self.drives_area.insertWidget(idx, rb)
                self._drive_radios.append(rb)

        # Reset destination path — never auto-select
        self._dest_path = ""
        self._dest_selected_label_update()

    def _fmt_drive_label(self, drive: dict) -> str:
        size_str = _fmt_bytes(drive["size_bytes"]) if drive["size_bytes"] else "? GB"
        free_str = _fmt_bytes(drive["free_bytes"]) if drive["free_bytes"] else "?"
        return f"{drive['label']}   —   {size_str} total / {free_str} free   ({drive['mountpoint']})"

    def _on_dest_selected(self, button):
        btn_id = self._dest_radio_group.id(button)
        if btn_id == 9999:
            # Custom path radio — path already set by _browse_dest
            pass
        elif 0 <= btn_id < len(self._drives):
            self._dest_path = self._drives[btn_id]["mountpoint"]
        self._dest_selected_label_update()
        self._reset_to_idle()

    def _dest_selected_label_update(self):
        """Keep custom_dest_path_label in sync with _dest_path."""
        checked = self._dest_radio_group.checkedButton()
        if checked and self._dest_radio_group.id(checked) == 9999:
            self.custom_dest_path_label.setText(self._dest_path or "None selected")
        else:
            self.custom_dest_path_label.setText("None selected")

    # ═══════════════════════════════════════════════════════════════════
    # Browse handlers
    # ═══════════════════════════════════════════════════════════════════

    def _browse_source(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Source Library Folder", self._source_path or str(Path.home())
        )
        if path:
            self._apply_source(path)

    def _browse_dest(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Destination Folder", self._dest_path or str(Path.home())
        )
        if path:
            self._dest_path = path
            self.custom_dest_path_label.setText(path)
            self.custom_dest_radio.setChecked(True)
            # Deselect drive radios
            for rb in self._drive_radios:
                rb.setChecked(False)
            self._reset_to_idle()

    # ═══════════════════════════════════════════════════════════════════
    # Options handlers
    # ═══════════════════════════════════════════════════════════════════

    def _on_collision_changed(self, button):
        is_overwrite = (self.collision_group.id(button) == 2)
        self.overwrite_banner.setVisible(is_overwrite)
        self._reset_to_idle()

    def _collision_mode(self) -> CollisionMode:
        bid = self.collision_group.checkedId()
        return {0: CollisionMode.SKIP, 1: CollisionMode.RENAME, 2: CollisionMode.OVERWRITE}.get(
            bid, CollisionMode.SKIP
        )

    # ═══════════════════════════════════════════════════════════════════
    # Run handlers
    # ═══════════════════════════════════════════════════════════════════

    def _validate_inputs(self) -> str | None:
        """Return an error message string if inputs are invalid, else None."""
        if not self._source_path:
            return "No source path set. Select a profile with a library path, or use Browse."
        if not Path(self._source_path).exists():
            return f"Source path does not exist:\n{self._source_path}"
        if not self._dest_path:
            return "No destination selected. Choose a drive from the list or use Browse."
        if Path(self._source_path) == Path(self._dest_path):
            return "Source and destination cannot be the same folder."
        return None

    def _start_dry_run(self):
        err = self._validate_inputs()
        if err:
            QMessageBox.warning(self, "Cannot Start", err)
            return
        self._run(dry_run=True)

    def _start_live_transfer(self):
        if not self._dry_run_done:
            QMessageBox.warning(
                self, "Dry Run Required",
                "You must complete a Dry Run before starting a live transfer.\n"
                "Click DRY RUN first and review the results."
            )
            return

        # OVERWRITE confirmation
        if self._collision_mode() == CollisionMode.OVERWRITE:
            answer = QMessageBox.warning(
                self,
                "Confirm OVERWRITE Mode",
                "You have selected OVERWRITE mode.\n\n"
                "Any file at the destination with the same name as a source file\n"
                "will be permanently replaced. This cannot be undone.\n\n"
                "Are you sure you want to proceed with OVERWRITE?",
                QMessageBox.Yes | QMessageBox.Cancel,
                QMessageBox.Cancel,
            )
            if answer != QMessageBox.Yes:
                return

        self._run(dry_run=False)

    def _run(self, dry_run: bool):
        self._set_state(_State.RUNNING)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Preparing…")

        engine = TransferEngine(
            source_root    = self._source_path,
            dest_root      = self._dest_path,
            collision_mode = self._collision_mode(),
            verify_hash    = self.verify_hash_cb.isChecked(),
            progress_cb    = self._on_progress,
            hash_cb        = self._on_hash_result if self.verify_hash_cb.isChecked() else None,
            stage_cb       = self._on_stage,
        )

        task = engine.dry_run if dry_run else engine.run_transfer
        self._runner = TaskRunner(task)
        self._runner.finished_signal.connect(lambda r: self._on_run_finished(r, dry_run))
        self._runner.start()

    def _on_stage(self, message: str):
        """Called from engine thread during prepare/scan phase — route via bridge."""
        try:
            self._bridge.stage.emit(message)
        except Exception:
            pass

    @Slot(str)
    def _update_stage(self, message: str):
        """Main-thread handler — show stage message on indeterminate bar."""
        self.progress_bar.setRange(0, 0)    # indeterminate spinner
        self.progress_bar.setVisible(True)
        self.progress_label.setText(message)
        self.progress_label.setVisible(True)

    def _on_hash_result(self, ok: bool, filename: str):
        """Called from engine thread — route via bridge signal."""
        try:
            self._bridge.hash_result.emit(ok, filename)
        except Exception:
            pass

    def _stop_run(self):
        if self._runner and self._runner.isRunning():
            # TaskRunner has no stop mechanism — request interruption and orphan
            self._runner.requestInterruption()
            self._runner = None
        self._set_state(_State.IDLE)
        self.progress_label.setText("Stopped.")

    # ═══════════════════════════════════════════════════════════════════
    # Progress callback (called from background thread)
    # ═══════════════════════════════════════════════════════════════════

    def _on_progress(self, current: int, total: int, path: str):
        """Receives (current_idx, total, current_path) from TransferEngine.

        Called from the background engine thread. Emits via the _ProgressBridge
        signal which Qt safely marshals to the main thread through the event
        queue. Do NOT touch any Qt widgets directly from this method.
        """
        try:
            self._bridge.progress.emit(current, total, path)
        except Exception:
            pass

    @Slot(int, int, str)
    def _update_progress(self, current: int, total: int, path: str):
        """Main-thread progress update — called via Signal from _ProgressBridge."""
        if total > 0:
            # Switch from indeterminate spinner (used during prepare) to determinate
            if self.progress_bar.maximum() == 0:
                self.progress_bar.setRange(0, 100)
            pct = int((current / total) * 100)
            self.progress_bar.setValue(pct)
        name = Path(path).name if path else ""
        self.progress_label.setText(f"{current:,} / {total:,}  —  {name}")
        # Reset hash badge to neutral while file is in flight
        if self.verify_hash_cb.isChecked():
            self.hash_badge.setText("  HASHING…  ")
            self.hash_badge.setStyleSheet(
                "background-color: #1e1e1e; color: #888; "
                "font-family: 'Courier New'; font-size: 11px; "
                "padding: 2px 8px; border-radius: 2px;"
            )
            self.hash_badge.setVisible(True)
        else:
            self.hash_badge.setVisible(False)

    @Slot(bool, str)
    def _show_hash_badge(self, ok: bool, filename: str):
        """Show per-file hash verification result badge."""
        if ok:
            self.hash_badge.setText("  ✔ VERIFIED  ")
            self.hash_badge.setStyleSheet(
                f"background-color: {_BG_COPIED}; color: {_COL_COPIED}; "
                "font-family: 'Courier New'; font-size: 11px; "
                "padding: 2px 8px; border-radius: 2px;"
            )
        else:
            self.hash_badge.setText("  ✖ HASH FAIL  ")
            self.hash_badge.setStyleSheet(
                f"background-color: {_BG_FAILED}; color: {_COL_FAILED}; "
                "font-family: 'Courier New'; font-size: 11px; "
                "padding: 2px 8px; border-radius: 2px;"
            )
        self.hash_badge.setVisible(True)

    # ═══════════════════════════════════════════════════════════════════
    # Run completion
    # ═══════════════════════════════════════════════════════════════════

    def _on_run_finished(self, report, dry_run: bool):
        if report is None:
            self._set_state(_State.IDLE)
            QMessageBox.critical(self, "Transfer Error", "The transfer failed unexpectedly. Check logs.")
            return

        self._last_report = report

        # Write report to disk
        report_path = None
        try:
            report_path = write_transfer_report(report)
        except Exception:
            pass

        # Update summary card
        self._populate_summary(report, dry_run)

        # Populate exceptions table
        self._populate_exceptions(report)

        # Show report path
        if report_path:
            self.report_path_label.setText(f"Report saved: {report_path}")
        else:
            self.report_path_label.setText("")

        if dry_run:
            self._dry_run_done = True
            self._set_state(_State.DRY_RUN_COMPLETE)
        else:
            self._dry_run_done = False   # must dry-run again to re-enable
            self._set_state(_State.LIVE_COMPLETE)

    # ═══════════════════════════════════════════════════════════════════
    # Results display
    # ═══════════════════════════════════════════════════════════════════

    def _populate_summary(self, report: TransferReport, dry_run: bool):
        label = "DRY RUN COMPLETE" if dry_run else "TRANSFER COMPLETE"
        self.summary_title.setText(label)

        parts = [
            f"{report.total:,} files",
            f"{_fmt_bytes(report.bytes_copied)}",
        ]
        if report.copied:
            parts.append(f"{report.copied:,} copied")
        if report.skipped:
            parts.append(f"{report.skipped:,} skipped")
        if report.collisions:
            parts.append(f"{report.collisions:,} renamed")
        if report.overwritten:
            parts.append(f"{report.overwritten:,} overwritten")
        if report.warned:
            parts.append(f"⚠ {report.warned:,} path warnings")
        if report.refused:
            parts.append(f"✖ {report.refused:,} path refused")
        if report.failed:
            parts.append(f"✖ {report.failed:,} FAILED")

        self.summary_label.setText("  ·  ".join(parts))

        # Colour the summary based on failures
        if report.failed or report.refused:
            self.summary_label.setStyleSheet(f"color: {_COL_FAILED}; font-size: 12px;")
        elif report.warned:
            self.summary_label.setStyleSheet(f"color: {_COL_WARNED}; font-size: 12px;")
        else:
            self.summary_label.setStyleSheet(f"color: {_COL_COPIED}; font-size: 12px;")

        # Hash verification result badge in summary
        if not dry_run and self.verify_hash_cb.isChecked():
            verified = report.copied + report.overwritten + report.collisions
            failures = report.failed
            if failures == 0 and verified > 0:
                self.hash_badge.setText(f"  ✔ {verified:,} VERIFIED  ")
                self.hash_badge.setStyleSheet(
                    f"background-color: {_BG_COPIED}; color: {_COL_COPIED}; "
                    "font-family: 'Courier New'; font-size: 11px; "
                    "padding: 2px 8px; border-radius: 2px;"
                )
            elif failures > 0:
                self.hash_badge.setText(f"  ✖ {failures:,} HASH FAIL  ")
                self.hash_badge.setStyleSheet(
                    f"background-color: {_BG_FAILED}; color: {_COL_FAILED}; "
                    "font-family: 'Courier New'; font-size: 11px; "
                    "padding: 2px 8px; border-radius: 2px;"
                )
            self.hash_badge.setVisible(True)
        else:
            self.hash_badge.setVisible(False)

    def _populate_exceptions(self, report: TransferReport):
        """Fill the exceptions table. Respects _show_all toggle."""
        self.exc_table.setRowCount(0)

        if self._show_all:
            rows = report.results
        else:
            rows = [r for r in report.results if r.outcome in _EXCEPTION_OUTCOMES]

        if not rows and not self._show_all:
            self.exc_title.setText("NO FAILURES OR WARNINGS")
        elif self._show_all:
            self.exc_title.setText(f"ALL FILES  ({len(rows):,})")
        else:
            self.exc_title.setText(f"FAILURES & WARNINGS  ({len(rows):,})")

        for result in rows:
            row = self.exc_table.rowCount()
            self.exc_table.insertRow(row)

            outcome_item = QTableWidgetItem(result.outcome.value)
            file_item    = QTableWidgetItem(Path(result.src_path).name)
            note_item    = QTableWidgetItem(result.reason or "")

            # Tooltip on file cell shows full path
            file_item.setToolTip(result.src_path)
            if result.dest_path and result.dest_path != result.src_path:
                dest_name = Path(result.dest_path).name
                if dest_name != Path(result.src_path).name:
                    note_item.setText(f"→ {dest_name}  {result.reason or ''}")

            bg, fg = _outcome_colours(result.outcome)
            for item in (outcome_item, file_item, note_item):
                item.setBackground(QColor(bg))
                item.setForeground(QColor(fg))

            self.exc_table.setItem(row, 0, outcome_item)
            self.exc_table.setItem(row, 1, file_item)
            self.exc_table.setItem(row, 2, note_item)

    def _toggle_show_all(self):
        self._show_all = not self._show_all
        self.show_all_btn.setText("SHOW EXCEPTIONS ONLY" if self._show_all else "SHOW ALL FILES")
        if self._last_report:
            self._populate_exceptions(self._last_report)

    # ═══════════════════════════════════════════════════════════════════
    # State machine
    # ═══════════════════════════════════════════════════════════════════

    def _set_state(self, state: _State):
        self._state = state

        running = (state == _State.RUNNING)
        idle    = (state == _State.IDLE)

        # Action buttons
        self.dry_run_btn.setEnabled(not running)
        self.live_btn.setEnabled(state == _State.DRY_RUN_COMPLETE)
        self.stop_btn.setEnabled(running)

        # Live button tooltip updates depending on state
        if state == _State.DRY_RUN_COMPLETE:
            self.live_btn.setToolTip(
                "Dry Run complete — ready to transfer for real.\n"
                "Review the results above before proceeding."
            )
        elif state == _State.LIVE_COMPLETE:
            self.live_btn.setToolTip(
                "Transfer complete. Run Dry Run again to enable another live transfer."
            )
        else:
            self.live_btn.setToolTip(
                "Only available after a successful Dry Run.\n"
                "Click DRY RUN first and review the results."
            )

        # Progress area
        self.progress_bar.setVisible(running)
        self.progress_label.setVisible(running or state in (_State.DRY_RUN_COMPLETE, _State.LIVE_COMPLETE))
        if not running and state not in (_State.DRY_RUN_COMPLETE, _State.LIVE_COMPLETE):
            self.hash_badge.setVisible(False)

        # Results area
        results_visible = state in (_State.DRY_RUN_COMPLETE, _State.LIVE_COMPLETE)
        self.summary_card.setVisible(results_visible)
        self.exc_card.setVisible(results_visible)

        # Config controls — lock during run
        self.refresh_drives_btn.setEnabled(not running)
        for rb in self._drive_radios:
            rb.setEnabled(not running)
        self.custom_dest_radio.setEnabled(not running)

    def _reset_to_idle(self):
        """Reset to IDLE and clear results. Called when config changes."""
        self._dry_run_done = False
        self._last_report  = None
        self._show_all     = False
        self.show_all_btn.setText("SHOW ALL FILES")
        self.exc_table.setRowCount(0)
        self._set_state(_State.IDLE)

    # ═══════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════

    def _divider(self) -> QFrame:
        line = QFrame()
        line.setObjectName("divider")
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        return line

    def _subdivider(self) -> QFrame:
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #2a2a2a; border: none;")
        return line


# ── Module-level helpers ─────────────────────────────────────────────────────

def _outcome_colours(outcome: Outcome) -> tuple[str, str]:
    """Return (bg_hex, fg_hex) for a given outcome code."""
    return {
        Outcome.FAILED:       (_BG_FAILED,  _COL_FAILED),
        Outcome.PATH_REFUSED: (_BG_FAILED,  _COL_FAILED),
        Outcome.PATH_WARNED:  (_BG_WARNED,  _COL_WARNED),
        Outcome.COLLISION:    (_BG_RENAMED, _COL_RENAMED),
        Outcome.COPIED:       (_BG_COPIED,  _COL_COPIED),
        Outcome.OVERWRITTEN:  (_BG_WARNED,  _COL_WARNED),
        Outcome.SKIPPED:      (_BG_SKIPPED, _COL_SKIPPED),
    }.get(outcome, ("#1e1e1e", "#f0f0f0"))


def _fmt_bytes(n: int) -> str:
    if not n:
        return "0 B"
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024.0
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
    return f"{n:.1f} TB"
