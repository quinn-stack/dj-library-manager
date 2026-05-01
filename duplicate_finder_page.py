"""
DJ Library Manager — Fuzzy Find — Duplicate Finder Page (High Performance)
v0.4.12 - Ambiguous files held in a separate review card below main results.
          Users can Quarantine, Delete, Keep, or Ignore ambiguous files
          individually or in bulk. Section persists across scans as a reminder.

v0.4.11 - Undo Quarantine button.
v0.4.10 - _QUARANTINE directories skipped during scan.
v0.4.9  - Fuzzy Find title, best candidate highlighting, bulk actions.
v0.4.8  - set_profile() fix, correct source_path key.
"""

import os
import shutil
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextEdit, QFrame, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QStyledItemDelegate, QStyle, QStyleOptionButton,
    QMessageBox, QSizePolicy
)
from PySide6.QtCore import Qt, QRect
from PySide6.QtGui import QColor, QFont

from engine.duplicate_finder import find_duplicates, apply_duplicate_actions
from engine.command_runner import TaskRunner
try:
    from ui.dialogs import confirm_delete as _confirm_delete
except ImportError:
    from dialogs import confirm_delete as _confirm_delete

# ── Column Maps ───────────────────────────────────────────────────────────────

# Main duplicates table
COL_FILE, COL_SIZE, COL_GROUP = 0, 1, 2
COL_Q, COL_D, COL_N = 3, 4, 5

# Ambiguous files table
ACOL_FILE, ACOL_Q, ACOL_D, ACOL_N, ACOL_IGNORE = 0, 1, 2, 3, 4

# ── Colours ───────────────────────────────────────────────────────────────────

BG_BEST  = QColor("#0d2b1e")
FG_BEST  = QColor("#4caf82")
BG_DUP_A = QColor("#1e1e1e")
BG_DUP_B = QColor("#252525")

# Ambiguous section — muted amber tint to signal "needs attention"
BG_AMB   = QColor("#1e1800")
FG_AMB   = QColor("#f0c040")
BG_AMB_B = QColor("#231c00")


# ── Delegates ─────────────────────────────────────────────────────────────────

class RadioButtonDelegate(QStyledItemDelegate):
    """Paints radio buttons as shapes rather than instantiating widget objects.

    Critical for large libraries — creating a QRadioButton per cell causes the
    UI to hang at scale. The delegate paints O(visible rows) regardless of
    total library size.
    """

    def paint(self, painter, option, index):
        self.initStyleOption(option, index)
        painter.fillRect(option.rect, option.backgroundBrush)

        radio_opt  = QStyleOptionButton()
        radio_size = option.widget.style().subElementRect(
            QStyle.SE_RadioButtonIndicator, radio_opt, option.widget
        ).size()

        radio_opt.rect = QRect(
            option.rect.left() + (option.rect.width() - radio_size.width()) // 2,
            option.rect.top()  + (option.rect.height() - radio_size.height()) // 2,
            radio_size.width(), radio_size.height()
        )

        is_checked = index.data(Qt.UserRole + 1)
        radio_opt.state |= QStyle.State_On if is_checked else QStyle.State_Off
        option.widget.style().drawControl(QStyle.CE_RadioButton, radio_opt, painter)

    def editorEvent(self, event, model, option, index):
        if event.type() == event.MouseButtonRelease:
            row = index.row()
            # Clear sibling radio columns — works for both table layouts
            # by reading which columns carry radio data from UserRole+1
            for col in range(model.columnCount()):
                item = model.item(row, col)
                if item and item.data(Qt.UserRole + 1) is not None:
                    model.setData(model.index(row, col), False, Qt.UserRole + 1)
            model.setData(index, True, Qt.UserRole + 1)
            return True
        return False


# ── Undo Quarantine (module-level for TaskRunner) ─────────────────────────────

def _undo_quarantine(quarantine_dir: str, source_root: str) -> dict:
    """Move all files from quarantine_dir back to their original locations.

    Reconstructs original paths by treating each file's path relative to
    quarantine_dir as its path relative to source_root — exact inverse of
    LibraryCleaner.move_to_quarantine().

    Removes empty directories bottom-up after restoring files.
    """
    summary = {"moved": 0, "skipped": 0, "errors": 0, "dirs_removed": 0}

    q_path = Path(quarantine_dir)
    r_path = Path(source_root)

    if not q_path.exists():
        return summary

    files_to_restore = []
    for dirpath, _, filenames in os.walk(q_path):
        for fn in filenames:
            src = Path(dirpath) / fn
            try:
                rel  = src.relative_to(q_path)
                dest = r_path / rel
                files_to_restore.append((src, dest))
            except ValueError:
                summary["errors"] += 1

    for src, dest in files_to_restore:
        try:
            if dest.exists():
                summary["skipped"] += 1
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            summary["moved"] += 1
        except Exception:
            summary["errors"] += 1

    for dirpath, _, _ in os.walk(q_path, topdown=False):
        d = Path(dirpath)
        try:
            if not any(d.iterdir()):
                d.rmdir()
                summary["dirs_removed"] += 1
        except Exception:
            pass

    return summary


# ── Page ──────────────────────────────────────────────────────────────────────



class DuplicateFinderPage(QWidget):
    def __init__(self, settings_manager, profile_manager):
        super().__init__()
        self.settings_manager = settings_manager
        self.profile_manager  = profile_manager
        self._source_path     = ""
        self._runner          = None
        self._apply_runner    = None
        self._undo_runner     = None

        self._build_ui()

        last_profile = self.settings_manager.get_last_profile()
        if last_profile:
            p = self.profile_manager.load_profile(last_profile)
            if p:
                self.refresh_profile(p.get("source_path", ""))

    # ═══════════════════════════════════════════════════════════════════
    # UI Construction
    # ═══════════════════════════════════════════════════════════════════

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(12)

        # ── Header ────────────────────────────────────────────────────
        header = QHBoxLayout()
        title  = QLabel("FUZZY FIND — DUPLICATE FINDER")
        title.setObjectName("heading")
        header.addWidget(title)
        header.addStretch()

        self.scan_btn = QPushButton("START SCAN")
        self.scan_btn.setObjectName("primary")
        self.scan_btn.clicked.connect(self._start_scan)
        header.addWidget(self.scan_btn)
        layout.addLayout(header)

        self.path_label = QLabel("Source: None")
        self.path_label.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self.path_label)

        # ── Legend ────────────────────────────────────────────────────
        legend = QHBoxLayout()
        legend.setSpacing(20)

        best_swatch = QLabel("  ★ BEST CANDIDATE  —  auto-selected Keep")
        best_swatch.setStyleSheet(
            f"background-color: {BG_BEST.name()}; color: {FG_BEST.name()}; "
            "padding: 3px 10px; font-size: 11px; font-family: 'Courier New';"
        )
        legend.addWidget(best_swatch)

        dup_swatch = QLabel("  DUPLICATE  —  auto-selected Quarantine")
        dup_swatch.setStyleSheet(
            f"background-color: {BG_DUP_A.name()}; color: #888888; "
            "padding: 3px 10px; font-size: 11px; font-family: 'Courier New';"
        )
        legend.addWidget(dup_swatch)
        legend.addStretch()
        layout.addLayout(legend)

        # ── Main duplicates table ──────────────────────────────────────
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(
            ["File", "Size (KB)", "Group", "Quarantine", "Delete", "Keep"]
        )
        self.table.horizontalHeader().setSectionResizeMode(COL_FILE,  QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(COL_SIZE,  QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(COL_GROUP, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(COL_Q,     QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(COL_D,     QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(COL_N,     QHeaderView.ResizeToContents)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.NoSelection)

        delegate = RadioButtonDelegate(self.table)
        for col in [COL_Q, COL_D, COL_N]:
            self.table.setItemDelegateForColumn(col, delegate)

        layout.addWidget(self.table)

        # ── Bulk actions card ──────────────────────────────────────────
        bulk_frame = QFrame()
        bulk_frame.setObjectName("card")
        bulk_layout = QVBoxLayout(bulk_frame)
        bulk_layout.setContentsMargins(12, 10, 12, 10)
        bulk_layout.setSpacing(6)

        bulk_label = QLabel(
            "BULK ACTIONS  —  keeps all ★ best candidates, acts on remaining duplicates:"
        )
        bulk_label.setObjectName("subheading")
        bulk_layout.addWidget(bulk_label)

        bulk_buttons = QHBoxLayout()
        bulk_buttons.setSpacing(10)

        self.bulk_quarantine_btn = QPushButton("★ KEEP BEST  +  QUARANTINE DUPLICATES")
        self.bulk_quarantine_btn.setObjectName("primary")
        self.bulk_quarantine_btn.setFixedHeight(34)
        self.bulk_quarantine_btn.setEnabled(False)
        self.bulk_quarantine_btn.setToolTip(
            "Keeps the ★ best candidate in every group.\n"
            "Moves all other copies to quarantine — recoverable."
        )
        self.bulk_quarantine_btn.clicked.connect(self._bulk_keep_and_quarantine)
        bulk_buttons.addWidget(self.bulk_quarantine_btn)

        self.bulk_delete_btn = QPushButton("★ KEEP BEST  +  DELETE DUPLICATES")
        self.bulk_delete_btn.setObjectName("danger")
        self.bulk_delete_btn.setFixedHeight(34)
        self.bulk_delete_btn.setEnabled(False)
        self.bulk_delete_btn.setToolTip(
            "Keeps the ★ best candidate in every group.\n"
            "Permanently deletes all other copies. Cannot be undone."
        )
        self.bulk_delete_btn.clicked.connect(self._bulk_keep_and_delete)
        bulk_buttons.addWidget(self.bulk_delete_btn)

        self.undo_quarantine_btn = QPushButton("↩  UNDO QUARANTINE")
        self.undo_quarantine_btn.setObjectName("warning")
        self.undo_quarantine_btn.setFixedHeight(34)
        self.undo_quarantine_btn.setEnabled(False)
        self.undo_quarantine_btn.setToolTip(
            "Moves all files from _QUARANTINE back to their original locations\n"
            "and removes any empty directories left behind."
        )
        self.undo_quarantine_btn.clicked.connect(self._undo_quarantine)
        bulk_buttons.addWidget(self.undo_quarantine_btn)

        bulk_buttons.addStretch()
        bulk_layout.addLayout(bulk_buttons)
        layout.addWidget(bulk_frame)

        # ── Ambiguous files card (hidden until scan finds some) ────────
        self.ambiguous_card = QFrame()
        self.ambiguous_card.setObjectName("card")
        self.ambiguous_card.setVisible(False)
        amb_layout = QVBoxLayout(self.ambiguous_card)
        amb_layout.setContentsMargins(12, 10, 12, 12)
        amb_layout.setSpacing(8)

        amb_header = QHBoxLayout()

        amb_title = QLabel("⚠  UNRESOLVABLE FILES — REVIEW REQUIRED")
        amb_title.setStyleSheet(
            f"color: {FG_AMB.name()}; font-family: 'Courier New'; "
            "font-size: 13px; font-weight: bold;"
        )
        amb_header.addWidget(amb_title)
        amb_header.addStretch()

        self.amb_count_label = QLabel("")
        self.amb_count_label.setObjectName("subheading")
        amb_header.addWidget(self.amb_count_label)

        amb_layout.addLayout(amb_header)

        amb_sub = QLabel(
            "These files have ambiguous filenames (e.g. 'Track 01', 'Various — Unknown') "
            "and no usable tags — they cannot be matched reliably. "
            "Review each one and choose an action, or Ignore to leave them untouched."
        )
        amb_sub.setObjectName("subheading")
        amb_sub.setWordWrap(True)
        amb_layout.addWidget(amb_sub)

        # Ambiguous table
        self.amb_table = QTableWidget(0, 5)
        self.amb_table.setHorizontalHeaderLabels(
            ["File", "Quarantine", "Delete", "Keep", ""]
        )
        self.amb_table.horizontalHeader().setSectionResizeMode(
            ACOL_FILE, QHeaderView.Stretch
        )
        for col in [ACOL_Q, ACOL_D, ACOL_N]:
            self.amb_table.horizontalHeader().setSectionResizeMode(
                col, QHeaderView.ResizeToContents
            )
        self.amb_table.horizontalHeader().setSectionResizeMode(
            ACOL_IGNORE, QHeaderView.ResizeToContents
        )
        self.amb_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.amb_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.amb_table.setMaximumHeight(220)

        amb_delegate = RadioButtonDelegate(self.amb_table)
        for col in [ACOL_Q, ACOL_D, ACOL_N]:
            self.amb_table.setItemDelegateForColumn(col, amb_delegate)

        amb_layout.addWidget(self.amb_table)

        # Ambiguous bulk actions
        amb_actions = QHBoxLayout()
        amb_actions.setSpacing(10)

        self.amb_apply_btn = QPushButton("APPLY SELECTED ACTIONS")
        self.amb_apply_btn.setFixedHeight(30)
        self.amb_apply_btn.setToolTip(
            "Applies the Quarantine / Delete / Keep selection for each row."
        )
        self.amb_apply_btn.clicked.connect(self._amb_apply_actions)
        amb_actions.addWidget(self.amb_apply_btn)

        self.amb_ignore_all_btn = QPushButton("IGNORE ALL")
        self.amb_ignore_all_btn.setFixedHeight(30)
        self.amb_ignore_all_btn.setToolTip(
            "Dismisses all unresolvable files from this list without moving\n"
            "or deleting anything. Filenames are logged to duplicates.log."
        )
        self.amb_ignore_all_btn.clicked.connect(self._amb_ignore_all)
        amb_actions.addWidget(self.amb_ignore_all_btn)

        amb_actions.addStretch()
        amb_layout.addLayout(amb_actions)

        layout.addWidget(self.ambiguous_card)

        # ── Bottom: log + manual apply ─────────────────────────────────
        bottom = QHBoxLayout()

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setFixedHeight(90)
        bottom.addWidget(self.log_box, 3)

        self.apply_btn = QPushButton("APPLY SELECTED ACTIONS")
        self.apply_btn.setFixedHeight(90)
        self.apply_btn.setToolTip(
            "Applies whatever Quarantine / Delete / Keep radio\n"
            "selections are currently set in the main table."
        )
        self.apply_btn.clicked.connect(self._apply_actions)
        bottom.addWidget(self.apply_btn, 1)

        layout.addLayout(bottom)

    # ═══════════════════════════════════════════════════════════════════
    # Profile Interface
    # ═══════════════════════════════════════════════════════════════════

    def set_profile(self, profile_name: str):
        """Called by MainWindow when the active profile changes.

        Loads the named profile and forwards source_path to refresh_profile().
        This method MUST exist — main_window.load_selected_profile() calls it
        inside a try/except that silently swallows AttributeError.
        """
        if not profile_name:
            return
        profile = self.profile_manager.load_profile(profile_name)
        if not profile:
            return
        self.refresh_profile(profile.get("source_path", ""))

    def refresh_profile(self, new_path: str):
        """Update displayed source path and reset the main table for the new directory.

        The ambiguous card is intentionally NOT cleared on profile change —
        it persists as a reminder that those files still need attention.
        A new scan will reset it.

        If a scan is currently running, we update _source_path (so the UI label
        reflects the new profile) but skip clearing the table and log — the
        running scan owns those until _on_scan_done fires. Without this guard,
        scan results would be populated into a cleared table and attributed to
        the wrong source path.
        """
        self._source_path = new_path
        self.path_label.setText(f"Source: {new_path}")
        # Don't disrupt a running scan — it captured source at start time and
        # will fire _on_scan_done when done; let it finish cleanly.
        scan_running = (
            self._runner is not None and self._runner.isRunning()
        )
        if scan_running:
            return
        self.table.setRowCount(0)
        self.log_box.clear()
        self.bulk_quarantine_btn.setEnabled(False)
        self.bulk_delete_btn.setEnabled(False)
        if new_path:
            self.log_box.append(f"Ready to scan: {new_path}")
            self._refresh_undo_button()

    def _get_quarantine_dir(self) -> str:
        profile = self.profile_manager.load_profile(
            self.settings_manager.get_last_profile()
        )
        return (
            profile.get("quarantine_path") if profile else None
        ) or str(Path(self._source_path) / "_QUARANTINE")

    def _refresh_undo_button(self):
        if not self._source_path:
            self.undo_quarantine_btn.setEnabled(False)
            return
        q = Path(self._get_quarantine_dir())
        has_files = False
        if q.exists():
            for _, _, files in os.walk(q):
                if files:
                    has_files = True
                    break
        self.undo_quarantine_btn.setEnabled(has_files)

    # ═══════════════════════════════════════════════════════════════════
    # Scan
    # ═══════════════════════════════════════════════════════════════════

    def _start_scan(self):
        if not self._source_path or not os.path.exists(self._source_path):
            self.log_box.append("ERROR: Invalid source path. Check the active profile.")
            return

        self.scan_btn.setEnabled(False)
        self.bulk_quarantine_btn.setEnabled(False)
        self.bulk_delete_btn.setEnabled(False)
        self.table.setRowCount(0)

        # Reset ambiguous card for the new scan
        self.amb_table.setRowCount(0)
        self.ambiguous_card.setVisible(False)

        self.log_box.append("Scanning for duplicates...")

        self._runner = TaskRunner(find_duplicates, self._source_path)
        self._runner.finished_signal.connect(self._on_scan_done)
        self._runner.start()

    def _on_scan_done(self, result):
        self.scan_btn.setEnabled(True)
        self._refresh_undo_button()

        # Check for TaskRunner error dict — emitted when find_duplicates raised
        if isinstance(result, dict) and "__task_error__" in result:
            self.log_box.append(
                f"SCAN ERROR — {result['__task_error__']}\n"
                f"Check ~/.dj_library_manager/logs/duplicates.log for details.\n"
                f"Full traceback:\n{result.get('__traceback__', '(none)')}"
            )
            return

        # Handle None (should no longer occur after TaskRunner fix, but guard anyway)
        if result is None:
            self.log_box.append(
                "SCAN ERROR — scan returned no result. "
                "Check ~/.dj_library_manager/logs/duplicates.log"
            )
            return

        # Unpack the tuple return — (GroupList, AmbiguousList) since v0.4.12
        if isinstance(result, tuple) and len(result) == 2:
            groups, ambiguous_files = result
        else:
            # Unexpected shape — log it clearly rather than silently showing "no results"
            self.log_box.append(
                f"SCAN ERROR — unexpected result type: {type(result).__name__}. "
                "This may indicate a version mismatch between engine and UI."
            )
            return

        # ── Populate main duplicates table ─────────────────────────────
        if not groups:
            self.log_box.append("No duplicates found.")
        else:
            self.table.setRowCount(0)

            for i, group in enumerate(groups):
                dup_bg = BG_DUP_A if i % 2 == 0 else BG_DUP_B

                for pos, (path, size) in enumerate(group):
                    is_best = (pos == 0)
                    row     = self.table.rowCount()
                    self.table.insertRow(row)

                    bg = BG_BEST if is_best else dup_bg
                    fg = FG_BEST if is_best else QColor("#cccccc")

                    name         = os.path.basename(path)
                    display_name = f"★  {name}" if is_best else f"    {name}"

                    file_item = QTableWidgetItem(display_name)
                    file_item.setData(Qt.UserRole,      path)
                    file_item.setData(Qt.UserRole + 10, is_best)
                    file_item.setData(Qt.UserRole + 11, i)
                    file_item.setBackground(bg)
                    file_item.setForeground(fg)
                    if is_best:
                        font = file_item.font()
                        font.setBold(True)
                        file_item.setFont(font)
                    self.table.setItem(row, COL_FILE, file_item)

                    size_item = QTableWidgetItem(str(size))
                    size_item.setBackground(bg)
                    size_item.setForeground(fg)
                    self.table.setItem(row, COL_SIZE, size_item)

                    group_item = QTableWidgetItem(str(i + 1))
                    group_item.setBackground(bg)
                    group_item.setForeground(fg)
                    self.table.setItem(row, COL_GROUP, group_item)

                    defaults = {COL_Q: not is_best, COL_D: False, COL_N: is_best}
                    for col, checked in defaults.items():
                        r_item = QTableWidgetItem()
                        r_item.setBackground(bg)
                        r_item.setData(Qt.UserRole + 1, checked)
                        self.table.setItem(row, col, r_item)

            total_dups = sum(len(g) - 1 for g in groups)
            self.log_box.append(
                f"Found {len(groups)} duplicate groups — "
                f"{total_dups} duplicate files auto-selected for quarantine."
            )
            self.bulk_quarantine_btn.setEnabled(True)
            self.bulk_delete_btn.setEnabled(True)

        # ── Populate ambiguous card ────────────────────────────────────
        if ambiguous_files:
            self._populate_ambiguous_table(ambiguous_files)
            self.log_box.append(
                f"{len(ambiguous_files)} file(s) held — ambiguous filename "
                f"and no usable tags. See review card below."
            )

    # ═══════════════════════════════════════════════════════════════════
    # Ambiguous Files Section
    # ═══════════════════════════════════════════════════════════════════

    def _populate_ambiguous_table(self, paths: list):
        """Fill the ambiguous review table and make the card visible."""
        self.amb_table.setRowCount(0)

        for i, path in enumerate(paths):
            bg = BG_AMB if i % 2 == 0 else BG_AMB_B
            row = self.amb_table.rowCount()
            self.amb_table.insertRow(row)

            name      = os.path.basename(path)
            file_item = QTableWidgetItem(name)
            file_item.setData(Qt.UserRole, path)
            file_item.setBackground(bg)
            file_item.setForeground(FG_AMB)
            file_item.setToolTip(path)
            self.amb_table.setItem(row, ACOL_FILE, file_item)

            # Radio defaults — Keep selected by default (safest)
            defaults = {ACOL_Q: False, ACOL_D: False, ACOL_N: True}
            for col, checked in defaults.items():
                r_item = QTableWidgetItem()
                r_item.setBackground(bg)
                r_item.setData(Qt.UserRole + 1, checked)
                self.amb_table.setItem(row, col, r_item)

            # Per-row IGNORE button
            ignore_btn = QPushButton("IGNORE")
            ignore_btn.setFixedHeight(24)
            ignore_btn.setFixedWidth(70)
            ignore_btn.setToolTip(
                "Dismiss this file from the list without moving or deleting it.\n"
                "Logged to duplicates.log."
            )
            ignore_btn.clicked.connect(
                lambda checked=False, r=row, p=path: self._amb_ignore_row(r, p)
            )
            self.amb_table.setCellWidget(row, ACOL_IGNORE, ignore_btn)

        self.amb_count_label.setText(
            f"{self.amb_table.rowCount()} file(s) require review"
        )
        self.ambiguous_card.setVisible(True)

    def _amb_ignore_row(self, row: int, path: str):
        """Ignore a single ambiguous file — remove from table, log it."""
        from engine.duplicate_finder import _write_log_batch
        _write_log_batch([f"IGNORED: {path}"])

        # Find the actual current row by matching the stored path, since
        # row indices shift as rows are removed above it.
        for r in range(self.amb_table.rowCount()):
            item = self.amb_table.item(r, ACOL_FILE)
            if item and item.data(Qt.UserRole) == path:
                self.amb_table.removeRow(r)
                break

        self._update_ambiguous_count()

    def _amb_ignore_all(self):
        """Ignore all remaining ambiguous files — clear table, log all paths."""
        from engine.duplicate_finder import _write_log_batch
        paths = []
        for row in range(self.amb_table.rowCount()):
            item = self.amb_table.item(row, ACOL_FILE)
            if item:
                paths.append(item.data(Qt.UserRole))

        if paths:
            _write_log_batch([f"IGNORED: {p}" for p in paths])
            self.log_box.append(
                f"Ignored {len(paths)} unresolvable file(s) — logged to duplicates.log."
            )

        self.amb_table.setRowCount(0)
        self._update_ambiguous_count()

    def _amb_apply_actions(self):
        """Apply Quarantine / Delete / Keep radio selections from the ambiguous table."""
        actions = {}
        for row in range(self.amb_table.rowCount()):
            file_item = self.amb_table.item(row, ACOL_FILE)
            if not file_item:
                continue
            path   = file_item.data(Qt.UserRole)
            q_item = self.amb_table.item(row, ACOL_Q)
            d_item = self.amb_table.item(row, ACOL_D)
            if q_item and q_item.data(Qt.UserRole + 1):
                actions[path] = "quarantine"
            elif d_item and d_item.data(Qt.UserRole + 1):
                actions[path] = "delete"
            # Keep — no action needed, file stays in place

        if not actions:
            self.log_box.append(
                "No quarantine or delete actions selected in the review section."
            )
            return

        delete_count = sum(1 for a in actions.values() if a == "delete")
        if delete_count > 0:
            # For >= 100 files, require typed "DELETE" confirmation.
            if not _confirm_delete(
                self, delete_count,
                "file(s) from the unresolvable files review"
            ):
                return

        self.log_box.append(
            f"Applying {len(actions)} action(s) from unresolvable files review..."
        )

        q_dir = self._get_quarantine_dir()
        self._amb_apply_btn_state(False)
        self._last_amb_actions = actions   # store before TaskRunner — _on_amb_apply_done reads this
        self._apply_runner = TaskRunner(
            apply_duplicate_actions, actions, q_dir, self._source_path
        )
        self._apply_runner.finished_signal.connect(self._on_amb_apply_done)
        self._apply_runner.start()

    def _on_amb_apply_done(self, summary):
        self._amb_apply_btn_state(True)
        q = summary.get("quarantined", 0)
        d = summary.get("deleted",     0)
        e = summary.get("errors",      0)
        self.log_box.append(
            f"Review section — Quarantined: {q}  |  Deleted: {d}  |  Errors: {e}"
        )

        # Remove rows that were successfully actioned from the table
        actioned_paths = set()
        if q:
            actioned_paths.update(
                p for p, a in self._last_amb_actions.items() if a == "quarantine"
            )
        if d:
            actioned_paths.update(
                p for p, a in self._last_amb_actions.items() if a == "delete"
            )

        rows_to_remove = []
        for row in range(self.amb_table.rowCount()):
            item = self.amb_table.item(row, ACOL_FILE)
            if item and item.data(Qt.UserRole) in actioned_paths:
                rows_to_remove.append(row)
        for row in reversed(rows_to_remove):
            self.amb_table.removeRow(row)

        self._update_ambiguous_count()
        self._refresh_undo_button()

    def _update_ambiguous_count(self):
        """Refresh the count label; hide the card if the table is now empty."""
        remaining = self.amb_table.rowCount()
        if remaining == 0:
            self.ambiguous_card.setVisible(False)
        else:
            self.amb_count_label.setText(f"{remaining} file(s) require review")

    def _amb_apply_btn_state(self, enabled: bool):
        self.amb_apply_btn.setEnabled(enabled)
        self.amb_ignore_all_btn.setEnabled(enabled)

    # ═══════════════════════════════════════════════════════════════════
    # Undo Quarantine
    # ═══════════════════════════════════════════════════════════════════

    def _undo_quarantine(self):
        q_dir  = self._get_quarantine_dir()
        q_path = Path(q_dir)

        if not q_path.exists():
            self.log_box.append("No quarantine directory found — nothing to undo.")
            return

        file_count = sum(len(files) for _, _, files in os.walk(q_path))
        if file_count == 0:
            self.log_box.append("Quarantine directory is already empty.")
            self._refresh_undo_button()
            return

        reply = QMessageBox.question(
            self,
            "Confirm Undo Quarantine",
            f"This will move {file_count} file(s) from:\n  {q_dir}\n\n"
            f"back to their original locations in:\n  {self._source_path}\n\n"
            f"Empty quarantine directories will be removed.\n"
            f"Files that already exist at the destination will be skipped.\n\n"
            f"Proceed?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply != QMessageBox.Yes:
            self.log_box.append("Undo quarantine cancelled.")
            return

        self.log_box.append(f"Restoring {file_count} file(s) from quarantine...")
        self.undo_quarantine_btn.setEnabled(False)

        self._undo_runner = TaskRunner(_undo_quarantine, q_dir, self._source_path)
        self._undo_runner.finished_signal.connect(self._on_undo_done)
        self._undo_runner.start()

    def _on_undo_done(self, summary):
        m = summary.get("moved",        0)
        s = summary.get("skipped",      0)
        e = summary.get("errors",       0)
        d = summary.get("dirs_removed", 0)
        self.log_box.append(
            f"Undo complete — Restored: {m}  |  "
            f"Skipped (already exist): {s}  |  "
            f"Errors: {e}  |  Empty dirs removed: {d}"
        )
        if s:
            self.log_box.append(
                f"  {s} file(s) skipped — destination already exists. "
                "Check your library for conflicts."
            )
        if e:
            self.log_box.append(
                "  Some files could not be moved. "
                "Check ~/.dj_library_manager/logs/duplicates.log"
            )
        self._refresh_undo_button()

    # ═══════════════════════════════════════════════════════════════════
    # Bulk Actions (main table)
    # ═══════════════════════════════════════════════════════════════════

    def _collect_bulk_actions(self, duplicate_action: str) -> dict:
        actions = {}
        for row in range(self.table.rowCount()):
            file_item = self.table.item(row, COL_FILE)
            if not file_item:
                continue
            if file_item.data(Qt.UserRole + 10):   # is_best — never touch
                continue
            path = file_item.data(Qt.UserRole)
            if path:
                actions[path] = duplicate_action
        return actions

    def _bulk_keep_and_quarantine(self):
        actions = self._collect_bulk_actions("quarantine")
        if not actions:
            self.log_box.append("No duplicate files to quarantine.")
            return
        self.log_box.append(
            f"Quarantining {len(actions)} duplicate files "
            f"(keeping all ★ best candidates)..."
        )
        self._run_apply(actions)

    def _bulk_keep_and_delete(self):
        actions = self._collect_bulk_actions("delete")
        if not actions:
            self.log_box.append("No duplicate files to delete.")
            return
        # For >= 100 files, require the user to type "DELETE" to confirm.
        if not _confirm_delete(
            self, len(actions),
            "duplicate file(s) — the best copy in each group (★) will be kept"
        ):
            return
        self.log_box.append(
            f"Deleting {len(actions)} duplicate files "
            f"(keeping all ★ best candidates)..."
        )
        self._run_apply(actions)

    # ═══════════════════════════════════════════════════════════════════
    # Manual Apply (main table)
    # ═══════════════════════════════════════════════════════════════════

    def _apply_actions(self):
        actions = {}
        for row in range(self.table.rowCount()):
            file_item = self.table.item(row, COL_FILE)
            if not file_item:
                continue
            path   = file_item.data(Qt.UserRole)
            q_item = self.table.item(row, COL_Q)
            d_item = self.table.item(row, COL_D)
            if q_item and q_item.data(Qt.UserRole + 1):
                actions[path] = "quarantine"
            elif d_item and d_item.data(Qt.UserRole + 1):
                actions[path] = "delete"

        if not actions:
            self.log_box.append("No quarantine or delete actions selected.")
            return

        delete_count = sum(1 for a in actions.values() if a == "delete")
        if delete_count > 0:
            # For >= 100 files, require typed "DELETE" confirmation.
            if not _confirm_delete(
                self, delete_count,
                "file(s) — files marked Quarantine will be moved, not deleted"
            ):
                return

        self.log_box.append(f"Applying {len(actions)} manual action(s)...")
        self._run_apply(actions)

    # ═══════════════════════════════════════════════════════════════════
    # Shared Apply Runner
    # ═══════════════════════════════════════════════════════════════════

    def _run_apply(self, actions: dict):
        q_dir = self._get_quarantine_dir()
        self._disable_action_buttons()
        self._apply_runner = TaskRunner(
            apply_duplicate_actions, actions, q_dir, self._source_path
        )
        self._apply_runner.finished_signal.connect(self._on_apply_done)
        self._apply_runner.start()

    def _on_apply_done(self, summary):
        self._enable_action_buttons()
        q = summary.get("quarantined", 0)
        d = summary.get("deleted",     0)
        e = summary.get("errors",      0)
        self.log_box.append(
            f"Done — Quarantined: {q}  |  Deleted: {d}  |  Errors: {e}"
        )
        if e:
            self.log_box.append(
                "Some errors occurred. "
                "Check ~/.dj_library_manager/logs/duplicates.log"
            )
        self._refresh_undo_button()

    # ═══════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════

    def _disable_action_buttons(self):
        self.apply_btn.setEnabled(False)
        self.bulk_quarantine_btn.setEnabled(False)
        self.bulk_delete_btn.setEnabled(False)

    def _enable_action_buttons(self):
        self.apply_btn.setEnabled(True)
        self.bulk_quarantine_btn.setEnabled(True)
        self.bulk_delete_btn.setEnabled(True)
