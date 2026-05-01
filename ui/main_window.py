from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QComboBox, QStackedWidget,
    QLineEdit, QFileDialog, QFormLayout, QTextEdit,
    QCheckBox, QScrollArea,
    QFrame, QSizePolicy, QMessageBox
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl
import os
from pathlib import Path

from engine.profile_manager import ProfileManager
from engine.settings_manager import SettingsManager
from engine.command_runner import TaskRunner
from engine.platform_adapter import PlatformAdapter
from engine.validator import Validator, ValidatorRunner
from engine.library_clean import LibraryCleaner
from ui.settings_page import SettingsPage
from ui.tag_finder_page import TagFinderPage
from ui.duplicate_finder_page import DuplicateFinderPage
from ui.transfer_page import TransferPage
try:
    from ui.dialogs import confirm_delete as _confirm_delete
except ImportError:
    from dialogs import confirm_delete as _confirm_delete




class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("DJ Library Manager")
        self.setMinimumSize(1100, 700)

        self.profile_manager = ProfileManager()
        self.settings_manager = SettingsManager()
        self.runner = None
        self._profile_dirty = False   # True when profile form has unsaved changes

        self._sidebar_buttons = []
        self._active_sidebar_btn = None

        self._build_ui()

    # ═══════════════════════════════════════════════════════════════════
    # UI Construction
    # ═══════════════════════════════════════════════════════════════════

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        root_layout.addWidget(self._build_header())

        accent_bar = QFrame()
        accent_bar.setObjectName("accent_bar")
        accent_bar.setFixedHeight(2)
        root_layout.addWidget(accent_bar)

        body = QWidget()
        body_layout = QHBoxLayout(body)
        # stack must exist before _build_sidebar() references it
        self.stack = QStackedWidget()

        body_layout.addWidget(self._build_sidebar())

        divider = QFrame()
        divider.setObjectName("divider")
        divider.setFixedWidth(1)
        divider.setFrameShape(QFrame.VLine)
        body_layout.addWidget(divider)

        body_layout.addWidget(self.stack)

        root_layout.addWidget(body)

        self.statusBar().showMessage("Ready")
        # small hint shown when the window is narrow or content is scrollable
        try:
            self.scroll_hint = QLabel("Tip: Use the right scrollbar to view content")
            self.scroll_hint.setObjectName("subheading")
            self.scroll_hint.setVisible(False)
            self.statusBar().addPermanentWidget(self.scroll_hint)
        except Exception:
            self.scroll_hint = None

        # Pages (validation comes before Tag Finder per user request)
        self.page_tag_finder  = TagFinderPage(self.settings_manager, self.profile_manager)
        self.page_profiles   = self._wrap_page_with_scroll(self._create_profiles_page())
        self.page_validation = self._wrap_page_with_scroll(self._create_validation_page())
        self.page_beets      = self._wrap_page_with_scroll(self.page_tag_finder)
        self.page_clean      = self._wrap_page_with_scroll(self._create_clean_page())
        self.page_settings   = self._wrap_page_with_scroll(SettingsPage(self.settings_manager))
        # Duplicate finder manages its own layout — do NOT wrap in scroll
        self.page_duplicates  = DuplicateFinderPage(self.settings_manager, self.profile_manager)
        # Transfer page manages its own layout — do NOT wrap in scroll
        self.page_transfer    = TransferPage(self.settings_manager, self.profile_manager)
        self.page_placeholder = self._wrap_page_with_scroll(self._create_placeholder_page("COMING SOON"))

        self.stack.addWidget(self.page_profiles)
        self.stack.addWidget(self.page_validation)
        self.stack.addWidget(self.page_beets)
        self.stack.addWidget(self.page_clean)
        self.stack.addWidget(self.page_settings)
        self.stack.addWidget(self.page_duplicates)
        self.stack.addWidget(self.page_transfer)
        self.stack.addWidget(self.page_placeholder)

        self.refresh_profiles_dropdown()
        self.restore_last_profile()

        self._set_active_sidebar(self._sidebar_buttons[0])
        self.stack.setCurrentWidget(self.page_profiles)

    def _wrap_page_with_scroll(self, page: QWidget) -> QScrollArea:
        """Wrap a page widget in a QScrollArea so content can scroll vertically.

        Returns the QScrollArea which should be used as the page in the stack.
        """
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(page)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        return scroll

    def _build_header(self):
        header = QWidget()
        header.setObjectName("panel")
        header.setFixedHeight(56)
        layout = QHBoxLayout(header)
        layout.setContentsMargins(20, 0, 20, 0)

        title = QLabel("DJ LIBRARY MANAGER")
        title.setObjectName("title")
        layout.addWidget(title)

        layout.addStretch()

        profile_label = QLabel("PROFILE:")
        profile_label.setObjectName("subheading")
        layout.addWidget(profile_label)

        self.profile_dropdown = QComboBox()
        self.profile_dropdown.setMinimumWidth(200)
        # Use index change to reliably load selected profile
        self.profile_dropdown.currentIndexChanged.connect(self.profile_selected_by_index)
        layout.addWidget(self.profile_dropdown)

        return header

    def _build_sidebar(self):
        sidebar = QWidget()
        sidebar.setObjectName("panel")
        sidebar.setFixedWidth(210)
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(0, 16, 0, 16)
        layout.setSpacing(2)

        nav_label = QLabel("NAVIGATION")
        nav_label.setObjectName("subheading")
        nav_label.setContentsMargins(16, 0, 0, 8)
        layout.addWidget(nav_label)

        nav_items = [
            ("⚙  PROFILES",      self.stack, "page_profiles"),
            ("🏷  TAG FINDER",    self.stack, "page_beets"),
            ("🧹  LIBRARY CLEAN", self.stack, "page_clean"),
            ("👯  DUPLICATES",    self.stack, "page_duplicates"),
            ("✔  VALIDATION",    self.stack, "page_validation"),
            ("🔊  HEALTH",        self.stack, "page_placeholder"),
            ("📦  TRANSFER",      self.stack, "page_transfer"),
            ("📋  LOGS",          self.stack, "page_placeholder"),
            ("⚙  SETTINGS",      self.stack, "page_settings"),
        ]

        for label, _, page_attr in nav_items:
            btn = QPushButton(label)
            btn.setObjectName("sidebar")
            btn.setFixedHeight(44)
            btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            btn.clicked.connect(
                lambda checked, attr=page_attr, b=btn: self._nav_click(b, attr)
            )
            layout.addWidget(btn)
            self._sidebar_buttons.append(btn)

        layout.addStretch()

        ver = QLabel("v0.5.8")
        ver.setObjectName("subheading")
        ver.setAlignment(Qt.AlignCenter)
        layout.addWidget(ver)

        return sidebar

    def _mark_profile_dirty(self):
        """Called whenever a profile form field changes."""
        self._profile_dirty = True

    def _clear_profile_dirty(self):
        """Called after a successful save or after loading a profile."""
        self._profile_dirty = False

    def _nav_click(self, btn, page_attr):
        # If the profile form has unsaved changes and the user is navigating away,
        # ask whether to save, discard, or stay.
        currently_on_profiles = (
            self.stack.currentWidget() is self.page_profiles
        )
        navigating_away = (page_attr != "page_profiles")
        if currently_on_profiles and navigating_away and self._profile_dirty:
            reply = QMessageBox.question(
                self,
                "Unsaved Profile Changes",
                "You have unsaved changes to the profile.\n\n"
                "Would you like to save them before leaving?",
                QMessageBox.Save | QMessageBox.Discard | QMessageBox.Cancel,
                QMessageBox.Save,
            )
            if reply == QMessageBox.Cancel:
                return   # stay on profiles page, do nothing
            if reply == QMessageBox.Save:
                self.save_profile()
                # save_profile() clears dirty flag — fall through to navigate
            else:  # Discard
                self._clear_profile_dirty()
        self._set_active_sidebar(btn)
        page = getattr(self, page_attr, self.page_placeholder)
        self.stack.setCurrentWidget(page)

    def _set_active_sidebar(self, active_btn):
        for btn in self._sidebar_buttons:
            btn.setObjectName("sidebar")
            btn.style().unpolish(btn)
            btn.style().polish(btn)
        active_btn.setObjectName("sidebar_active")
        active_btn.style().unpolish(active_btn)
        active_btn.style().polish(active_btn)
        self._active_sidebar_btn = active_btn

    # ═══════════════════════════════════════════════════════════════════
    # Profiles Page
    # ═══════════════════════════════════════════════════════════════════

    def _create_profiles_page(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(16)

        heading = QLabel("PROFILES")
        heading.setObjectName("heading")
        layout.addWidget(heading)
        layout.addWidget(self._divider())

        card = QFrame()
        card.setObjectName("card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(16, 12, 16, 16)
        card_layout.setSpacing(12)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        form.setSpacing(10)

        self.profile_name_input = QLineEdit()
        self.profile_name_input.setPlaceholderText("e.g. Wedding Set 2026 or My Club Crate")
        form.addRow("PROFILE NAME:", self.profile_name_input)

        source_row = QHBoxLayout()
        self.source_path_input = QLineEdit()
        self.source_path_input.setPlaceholderText("Select source folder containing your music (click BROWSE)")
        source_btn = QPushButton("BROWSE")
        source_btn.setFixedWidth(90)
        source_btn.clicked.connect(self.browse_source)
        source_row.addWidget(self.source_path_input)
        source_row.addWidget(source_btn)
        form.addRow("SOURCE PATH:", source_row)

        dest_row = QHBoxLayout()
        self.destination_path_input = QLineEdit()
        self.destination_path_input.setPlaceholderText("Select destination folder for cleaned library (click BROWSE)")
        dest_btn = QPushButton("BROWSE")
        dest_btn.setFixedWidth(90)
        dest_btn.clicked.connect(self.browse_destination)
        dest_row.addWidget(self.destination_path_input)
        dest_row.addWidget(dest_btn)
        form.addRow("DEST PATH:", dest_row)

        # Mark form dirty whenever any field changes
        self.profile_name_input.textChanged.connect(self._mark_profile_dirty)
        self.source_path_input.textChanged.connect(self._mark_profile_dirty)
        self.destination_path_input.textChanged.connect(self._mark_profile_dirty)

        card_layout.addLayout(form)
        layout.addWidget(card)

        btn_row = QHBoxLayout()
        self.save_profile_btn = QPushButton("SAVE PROFILE")
        self.save_profile_btn.setObjectName("primary")
        self.save_profile_btn.clicked.connect(self.save_profile)

        self.delete_profile_btn = QPushButton("DELETE PROFILE")
        self.delete_profile_btn.setObjectName("danger")
        self.delete_profile_btn.clicked.connect(self.delete_profile)

        btn_row.addWidget(self.save_profile_btn)
        btn_row.addWidget(self.delete_profile_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        layout.addStretch()
        return page

    # ═══════════════════════════════════════════════════════════════════
    # Beets Page
    # ═══════════════════════════════════════════════════════════════════

    def _refresh_beets_info(self):
        # Deprecated — delegates to TagFinderPage
        try:
            self.page_tag_finder._refresh_info()
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════
    # Placeholder Page
    # ═══════════════════════════════════════════════════════════════════

    def _create_placeholder_page(self, text):
        page = QWidget()
        layout = QVBoxLayout(page)
        label = QLabel(text)
        label.setObjectName("heading")
        label.setAlignment(Qt.AlignCenter)
        sub = QLabel("This module is under development.\nCheck the roadmap for planned features.")
        sub.setObjectName("subheading")
        sub.setAlignment(Qt.AlignCenter)
        layout.addStretch()
        layout.addWidget(label)
        layout.addWidget(sub)
        layout.addStretch()
        return page

    # ═══════════════════════════════════════════════════════════════════
    # Validation Page
    # ═══════════════════════════════════════════════════════════════════

    def _create_validation_page(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(16)

        heading = QLabel("VALIDATION — PRE-TRANSFER CHECKS")
        heading.setObjectName("heading")
        layout.addWidget(heading)
        layout.addWidget(self._divider())

        # Info card
        self.validation_info = QFrame()
        self.validation_info.setObjectName("card")
        info_layout = QVBoxLayout(self.validation_info)
        info_layout.setContentsMargins(16, 12, 16, 12)
        info_layout.setSpacing(6)

        self.validation_profile_label = QLabel("Profile source: —")
        self.validation_profile_label.setObjectName("subheading")
        info_layout.addWidget(self.validation_profile_label)

        layout.addWidget(self.validation_info)

        # Controls
        controls = QHBoxLayout()

        self.btn_pathlen = QPushButton("Run Path-Length Scan")
        self.btn_pathlen.setObjectName("primary")
        self.btn_pathlen.clicked.connect(self.run_path_length_scan)
        controls.addWidget(self.btn_pathlen)

        self.btn_dupname = QPushButton("Find Duplicate Filenames")
        self.btn_dupname.clicked.connect(self.run_dup_filename_scan)
        controls.addWidget(self.btn_dupname)

        self.btn_duphash = QPushButton("Find Duplicate Hashes (slow)")
        self.btn_duphash.clicked.connect(self.run_dup_hash_scan)
        controls.addWidget(self.btn_duphash)

        # Detected audio types (checkable) for hash scan
        from PySide6.QtWidgets import QListWidget, QListWidgetItem
        self.dup_ext_list = QListWidget()
        self.dup_ext_list.setMaximumWidth(220)
        self.dup_ext_list.setMaximumHeight(120)
        controls.addWidget(self.dup_ext_list)

        self.validation_status = QLabel("IDLE")
        self.validation_status.setObjectName("status_idle")
        controls.addWidget(self.validation_status)
        controls.addStretch()

        layout.addLayout(controls)

        # Left: log, Right: review items
        mid = QHBoxLayout()

        left_col = QVBoxLayout()
        log_label = QLabel("▌ VALIDATION LOG")
        log_label.setObjectName("subheading")
        left_col.addWidget(log_label)

        self.validation_log = QTextEdit()
        self.validation_log.setReadOnly(True)
        self.validation_log.setMinimumHeight(300)
        left_col.addWidget(self.validation_log)

        mid.addLayout(left_col, 2)

        right_col = QVBoxLayout()
        review_label = QLabel("REVIEW ITEMS")
        review_label.setObjectName("subheading")
        right_col.addWidget(review_label)

        from PySide6.QtWidgets import QListWidget
        self.validation_items = QListWidget()
        self.validation_items.setMinimumWidth(360)
        right_col.addWidget(self.validation_items)

        action_row = QHBoxLayout()
        self.btn_reveal = QPushButton("Reveal in File Manager")
        self.btn_reveal.clicked.connect(self.reveal_selected_item)
        action_row.addWidget(self.btn_reveal)

        self.btn_resolve = QPushButton("Mark Resolved")
        self.btn_resolve.clicked.connect(self.resolve_selected_item)
        action_row.addWidget(self.btn_resolve)

        right_col.addLayout(action_row)
        mid.addLayout(right_col, 1)

        layout.addLayout(mid)

        return page

    # ═══════════════════════════════════════════════════════════════════
    # Profile Logic
    # ═══════════════════════════════════════════════════════════════════

    def browse_source(self):
        path = QFileDialog.getExistingDirectory(self, "Select Source Folder")
        if path:
            self.source_path_input.setText(path)

    def browse_destination(self):
        path = QFileDialog.getExistingDirectory(self, "Select Destination Folder")
        if path:
            self.destination_path_input.setText(path)

    def save_profile(self):
        profile_data = {
            "profile_name": self.profile_name_input.text().strip(),
            "source_path": self.source_path_input.text().strip(),
            "destination_path": self.destination_path_input.text().strip()
        }
        if not profile_data["profile_name"]:
            self.statusBar().showMessage("Profile name cannot be empty.")
            return
        self.profile_manager.save_profile(profile_data)
        # Refresh dropdown and select the newly saved profile.
        # Block currentIndexChanged while manipulating the dropdown so that
        # setCurrentIndex() does not fire profile_selected_by_index() →
        # load_selected_profile() → page_transfer.set_profile() →
        # _apply_source() → _reset_to_idle() while a transfer may be running.
        self.refresh_profiles_dropdown()
        name = profile_data['profile_name']
        idx = self.profile_dropdown.findText(name)
        if idx != -1:
            self.profile_dropdown.blockSignals(True)
            self.profile_dropdown.setCurrentIndex(idx)
            self.profile_dropdown.blockSignals(False)
            # persist as last selected profile
            self.settings_manager.set_last_profile(name)
            self.load_selected_profile(name)
        self._clear_profile_dirty()
        self.statusBar().showMessage(f"Profile '{profile_data['profile_name']}' saved.")

    def delete_profile(self):
        name = self.profile_name_input.text().strip()
        if not name:
            return

        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Delete profile '{name}'? This cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            self.profile_manager.delete_profile(name)
            # If the deleted profile was the last-selected, clear it from settings
            last = self.settings_manager.get_last_profile()
            if last == name:
                self.settings_manager.set_last_profile(None)

            self.refresh_profiles_dropdown()
            # clear fields
            self.profile_name_input.clear()
            self.source_path_input.clear()
            self.destination_path_input.clear()
            self.statusBar().showMessage(f"Profile '{name}' deleted.")

    def refresh_profiles_dropdown(self):
        self.profile_dropdown.blockSignals(True)
        self.profile_dropdown.clear()
        profiles = self.profile_manager.list_profiles()
        self.profile_dropdown.addItems(profiles)
        self.profile_dropdown.blockSignals(False)
        # Try to restore last selected profile after refresh
        last = self.settings_manager.get_last_profile()
        if last:
            idx = self.profile_dropdown.findText(last)
            if idx != -1:
                # set selection and load profile into form
                self.profile_dropdown.setCurrentIndex(idx)
                self.load_selected_profile(last)

    def profile_changed(self, name):
        if not name:
            return
        self.settings_manager.set_last_profile(name)
        self.load_selected_profile(name)
        self.statusBar().showMessage(f"Loaded profile: {name}")

    def restore_last_profile(self):
        last_profile = self.settings_manager.get_last_profile()
        if last_profile:
            index = self.profile_dropdown.findText(last_profile)
            if index != -1:
                self.profile_dropdown.setCurrentIndex(index)

    def load_selected_profile(self, name):
        profile = self.profile_manager.load_profile(name)
        if not profile:
            return
        # Block signals while programmatically populating fields so loading
        # a profile doesn't immediately mark the form dirty.
        for widget in (self.profile_name_input, self.source_path_input,
                       self.destination_path_input):
            widget.blockSignals(True)
        self.profile_name_input.setText(profile.get("profile_name", ""))
        self.source_path_input.setText(profile.get("source_path", ""))
        self.destination_path_input.setText(profile.get("destination_path", ""))
        for widget in (self.profile_name_input, self.source_path_input,
                       self.destination_path_input):
            widget.blockSignals(False)
        self._clear_profile_dirty()
        # update validation info card
        src = profile.get("source_path", "—")
        self.validation_profile_label.setText(f"Profile source: {src}")
        # Notify Tag Finder of profile change
        try:
            self.page_tag_finder.set_profile(name)
        except Exception:
            pass
        # Notify Duplicate Finder of profile change
        try:
            self.page_duplicates.set_profile(name)
        except Exception:
            pass
        # Notify Transfer page of profile change (updates source path)
        try:
            self.page_transfer.set_profile(name)
        except Exception:
            pass

    def profile_selected_by_index(self, index):
        if index < 0:
            return
        name = self.profile_dropdown.itemText(index)
        if not name:
            return
        # Load the profile into the form
        self.load_selected_profile(name)
        # Persist last selected
        try:
            self.settings_manager.set_last_profile(name)
        except Exception:
            pass
        self.statusBar().showMessage(f"Loaded profile: {name}")

    # ═══════════════════════════════════════════════════════════════════
    # Validation Runners
    # ═══════════════════════════════════════════════════════════════════

    def _validation_log_append(self, text):
        self.validation_log.append(text)
        self.validation_log.moveCursor(QTextCursor.End)

    def run_path_length_scan(self):
        current_profile = self.profile_dropdown.currentText()
        if not current_profile:
            self._validation_log_append("⚠  No profile selected.")
            return
        profile = self.profile_manager.load_profile(current_profile)
        if not profile:
            self._validation_log_append("⚠  Profile could not be loaded.")
            return
        source = profile.get("source_path")
        if not source:
            self._validation_log_append("⚠  Source path not set in profile.")
            return
        self.btn_pathlen.setEnabled(False)
        self._validation_log_append(f"Starting path-length scan for: {source}")
        # update status clearly
        self.validation_status.setText("RUNNING")
        self.validation_status.setObjectName("status_running")
        self.validation_status.style().unpolish(self.validation_status)
        self.validation_status.style().polish(self.validation_status)
        limit = self.settings_manager.get_validation_settings().get("path_length_limit", 240)
        retention = self.settings_manager.get_validation_settings().get("log_retention", 20)
        self.validation_runner = ValidatorRunner("path_length", source, limit=limit, retention=retention)
        self.validation_runner.output.connect(self._validation_log_append)
        self.validation_runner.finished.connect(self.validation_finished)
        self.validation_runner.start()

    def run_dup_filename_scan(self):
        current_profile = self.profile_dropdown.currentText()
        if not current_profile:
            self._validation_log_append("⚠  No profile selected.")
            return
        profile = self.profile_manager.load_profile(current_profile)
        if not profile:
            self._validation_log_append("⚠  Profile could not be loaded.")
            return
        source = profile.get("source_path")
        if not source:
            self._validation_log_append("⚠  Source path not set in profile.")
            return

        self.btn_dupname.setEnabled(False)
        self._validation_log_append(f"Starting duplicate-filename scan for: {source}")
        self.validation_status.setText("RUNNING")
        self.validation_status.setObjectName("status_running")
        self.validation_status.style().unpolish(self.validation_status)
        self.validation_status.style().polish(self.validation_status)
        retention = self.settings_manager.get_validation_settings().get("log_retention", 20)
        self.validation_runner = ValidatorRunner("dup_filename", source, retention=retention)
        self.validation_runner.output.connect(self._validation_log_append)
        self.validation_runner.finished.connect(self.validation_finished)
        self.validation_runner.start()

    def run_dup_hash_scan(self):
        current_profile = self.profile_dropdown.currentText()
        if not current_profile:
            self._validation_log_append("⚠  No profile selected.")
            return
        profile = self.profile_manager.load_profile(current_profile)
        if not profile:
            self._validation_log_append("⚠  Profile could not be loaded.")
            return
        source = profile.get("source_path")
        if not source:
            self._validation_log_append("⚠  Source path not set in profile.")
            return
        self.btn_duphash.setEnabled(False)
        self._validation_log_append(f"Starting duplicate-hash scan for: {source}")
        self.validation_status.setText("RUNNING")
        self.validation_status.setObjectName("status_running")
        self.validation_status.style().unpolish(self.validation_status)
        self.validation_status.style().polish(self.validation_status)
        # gather checked extensions from the detected list
        exts = []
        try:
            for i in range(self.dup_ext_list.count()):
                it = self.dup_ext_list.item(i)
                if it.checkState() == Qt.Checked:
                    exts.append(it.text().lower())
        except Exception:
            exts = None
        if exts == []:
            exts = None
        retention = self.settings_manager.get_validation_settings().get("log_retention", 20)
        self.validation_runner = ValidatorRunner("dup_hash", source, exts=exts, retention=retention)
        self.validation_runner.output.connect(self._validation_log_append)
        self.validation_runner.finished.connect(self.validation_finished)
        self.validation_runner.start()

    def validation_finished(self, report_path):
        self.btn_pathlen.setEnabled(True)
        self.btn_dupname.setEnabled(True)
        self.btn_duphash.setEnabled(True)
        if report_path:
            self._validation_log_append(f"\n✔  Validation report saved: {report_path}")
            # Populate interactive review items from the report
            try:
                self.load_report_items(report_path)
            except Exception:
                pass
            self.statusBar().showMessage("Validation complete.")
            self.validation_status.setText("COMPLETE")
            self.validation_status.setObjectName("status_ok")
        else:
            self._validation_log_append("\n✘  Validation finished — no report generated or errors occurred.")
            self.validation_status.setText("ERROR")
            self.validation_status.setObjectName("status_error")
        self.validation_status.style().unpolish(self.validation_status)
        self.validation_status.style().polish(self.validation_status)

    def load_report_items(self, report_path):
        """Parse a simple validation report and populate the review list with paths."""
        self.validation_items.clear()
        resolved = set()
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # skip header lines
                    if line.startswith("#") or line.lower().startswith("generated") or line.lower().startswith("path length report"):
                        continue
                    # duplicates report: skip section headers
                    if line.startswith("==="):
                        continue
                    # path-length lines often start with a number then path
                    parts = line.split()
                    # take last token if it looks like a path
                    candidate = parts[-1]
                    if candidate.startswith("/") or ":\\" in candidate or os.path.exists(candidate):
                        self.validation_items.addItem(candidate)
        except Exception:
            pass

    def populate_dup_ext_list(self, source_path):
        """Scan `source_path` for file extensions and populate the dup_ext_list
        with common audio types found. Default-selects all detected types.
        """
        audio_exts = {"mp3", "m4a", "flac", "wav", "aac", "ogg", "opus", "wma", "aiff", "alac"}
        found = set()
        try:
            count = 0
            for dirpath, dirnames, filenames in os.walk(source_path):
                for fn in filenames:
                    count += 1
                    ext = Path(fn).suffix.lower().lstrip('.')
                    if ext in audio_exts:
                        found.add(ext)
                # avoid walking enormous trees forever; stop after 20000 files scanned
                if count > 20000:
                    break
        except Exception:
            found = set()

        # populate list widget
        self.dup_ext_list.clear()
        if not found:
            # show common defaults but unchecked
            for e in sorted(audio_exts):
                item = QListWidgetItem(e)
                item.setCheckState(Qt.Unchecked)
                self.dup_ext_list.addItem(item)
            return

        for e in sorted(found):
            item = QListWidgetItem(e)
            item.setCheckState(Qt.Checked)
            self.dup_ext_list.addItem(item)

    def _create_clean_page(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(16)

        heading = QLabel("LIBRARY CLEAN — REMOVE NON-AUDIO FILES")
        heading.setObjectName("heading")
        layout.addWidget(heading)
        layout.addWidget(self._divider())

        controls = QHBoxLayout()
        self.clean_scan_btn = QPushButton("Scan for Non-Audio Files")
        self.clean_scan_btn.setObjectName("primary")
        self.clean_scan_btn.clicked.connect(self.run_clean_scan)
        controls.addWidget(self.clean_scan_btn)

        self.clean_quarantine_btn = QPushButton("Move Selected to Quarantine")
        self.clean_quarantine_btn.setObjectName("primary")
        self.clean_quarantine_btn.clicked.connect(self.run_clean_quarantine_selected)
        self.clean_quarantine_btn.setEnabled(False)
        controls.addWidget(self.clean_quarantine_btn)

        self.clean_remove_btn = QPushButton("Delete Selected")
        self.clean_remove_btn.clicked.connect(self.run_clean_remove_selected)
        self.clean_remove_btn.setEnabled(False)
        controls.addWidget(self.clean_remove_btn)

        self.clean_status = QLabel("IDLE")
        self.clean_status.setObjectName("status_idle")
        controls.addWidget(self.clean_status)
        controls.addStretch()
        layout.addLayout(controls)

        mid = QHBoxLayout()
        left_col = QVBoxLayout()
        log_label = QLabel("▌ CLEAN LOG")
        log_label.setObjectName("subheading")
        left_col.addWidget(log_label)

        self.clean_log = QTextEdit()
        self.clean_log.setReadOnly(True)
        self.clean_log.setMinimumHeight(300)
        left_col.addWidget(self.clean_log)
        mid.addLayout(left_col, 2)

        right_col = QVBoxLayout()
        list_label = QLabel("CANDIDATES")
        list_label.setObjectName("subheading")
        right_col.addWidget(list_label)

        from PySide6.QtWidgets import QListWidget, QAbstractItemView
        self.clean_list = QListWidget()
        self.clean_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.clean_list.setMinimumWidth(360)
        right_col.addWidget(self.clean_list)

        # Quarantine controls (dry-run + quarantine dir)
        self.quarantine_row = QHBoxLayout()
        self.quarantine_checkbox = QCheckBox("Dry run (preview only)")
        self.quarantine_checkbox.setChecked(True)
        self.quarantine_row.addWidget(self.quarantine_checkbox)

        # show an initial placeholder; effective path will be updated when a
        # profile/source is loaded or when a scan starts
        self.quarantine_dir_label = QLabel("(will use profile source/_QUARANTINE)")
        self.quarantine_dir_label.setMinimumWidth(160)
        self.quarantine_row.addWidget(self.quarantine_dir_label)

        self.quarantine_browse = QPushButton("Change")
        self.quarantine_browse.setFixedWidth(80)
        self.quarantine_browse.clicked.connect(self.browse_quarantine_dir)
        self.quarantine_row.addWidget(self.quarantine_browse)

        right_col.addLayout(self.quarantine_row)
        mid.addLayout(right_col, 1)

        layout.addLayout(mid)
        return page

    def run_clean_scan(self):
        current_profile = self.profile_dropdown.currentText()
        if not current_profile:
            self.clean_log.append("⚠  No profile selected.")
            return
        profile = self.profile_manager.load_profile(current_profile)
        if not profile:
            self.clean_log.append("⚠  Profile could not be loaded.")
            return
        source = profile.get("source_path")
        if not source:
            self.clean_log.append("⚠  Source path not set in profile.")
            return

        self.clean_scan_btn.setEnabled(False)
        self.clean_remove_btn.setEnabled(False)
        self.clean_quarantine_btn.setEnabled(False)
        # update displayed effective quarantine path based on this profile's source
        try:
            q = self.settings_manager.get_quarantine_dir_for_source(source)
            self.quarantine_dir_label.setText(q)
        except Exception:
            pass
        self.clean_log.append(f"Scanning for non-audio files in: {source}")
        self.clean_runner = TaskRunner(LibraryCleaner.detect_non_audio, source)
        self.clean_runner.output_signal.connect(self.clean_log.append)
        self.clean_runner.finished_signal.connect(self.clean_scan_finished)
        self.clean_runner.start()

    def clean_scan_finished(self, result):
        self.clean_scan_btn.setEnabled(True)
        self.clean_list.clear()
        if not result:
            self.clean_log.append("No non-audio files found.")
            self.clean_remove_btn.setEnabled(False)
            self.clean_quarantine_btn.setEnabled(False)
            return
        for p in result:
            self.clean_list.addItem(p)
        self.clean_log.append(f"Found {len(result)} candidate files.")
        self.clean_remove_btn.setEnabled(True)
        self.clean_quarantine_btn.setEnabled(True)

    def run_clean_remove_selected(self):
        items = [it.text() for it in self.clean_list.selectedItems()]
        if not items:
            return
        if not _confirm_delete(self, len(items), "selected non-audio file(s)"):
            return
        self.clean_remove_btn.setEnabled(False)
        self.clean_quarantine_btn.setEnabled(False)
        self.clean_log.append(f"Removing {len(items)} files...")
        self.clean_runner = TaskRunner(LibraryCleaner.remove_paths, items)
        self.clean_runner.output_signal.connect(self.clean_log.append)
        self.clean_runner.finished_signal.connect(self.clean_remove_finished)
        self.clean_runner.start()

    def clean_remove_finished(self, result):
        try:
            removed = int(result) if result is not None else 0
        except Exception:
            removed = 0
        self.clean_log.append(f"Removed {removed} files.")
        try:
            self.run_clean_scan()
        except Exception:
            pass

    def browse_quarantine_dir(self):
        path = QFileDialog.getExistingDirectory(self, "Select Quarantine Folder")
        if not path:
            return
        try:
            self.settings_manager.set_quarantine_dir(path)
        except Exception:
            pass
        try:
            # show the customized path immediately
            self.quarantine_dir_label.setText(self.settings_manager.get_quarantine_dir())
        except Exception:
            pass

    def run_clean_quarantine_selected(self):
        items = [it.text() for it in self.clean_list.selectedItems()]
        if not items:
            return
        # compute effective quarantine dir using profile source as default
        profile = self.profile_manager.load_profile(self.profile_dropdown.currentText())
        source = profile.get("source_path") if profile else None
        quarantine = self.settings_manager.get_quarantine_dir_for_source(source)
        dry = bool(self.quarantine_checkbox.isChecked())
        msg = (
            f"Move {len(items)} selected files to quarantine at:\n{quarantine}\n"
            f"(Dry run: {'YES' if dry else 'NO'})\n\nProceed?"
        )
        reply = QMessageBox.question(
            self,
            "Confirm Quarantine",
            msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        self.clean_quarantine_btn.setEnabled(False)
        self.clean_log.append(f"Moving {len(items)} files to quarantine (dry_run={dry})...")
        # call move_to_quarantine(root, paths, quarantine_dir, dry_run)
        # remember dry-run state so the finished handler can present a preview
        self._last_quarantine_dry = bool(dry)
        self.clean_runner = TaskRunner(LibraryCleaner.move_to_quarantine, source, items, quarantine, dry)
        self.clean_runner.output_signal.connect(self.clean_log.append)
        self.clean_runner.finished_signal.connect(self.clean_quarantine_finished)
        self.clean_runner.start()

    def clean_quarantine_finished(self, result):
        # result is list of (orig, dest)
        moved = result or []
        dry = bool(getattr(self, "_last_quarantine_dry", False))
        if dry:
            self.clean_log.append(f"DRY RUN: {len(moved)} files would be moved to quarantine.")
        else:
            self.clean_log.append(f"Moved {len(moved)} files to quarantine.")
        # If this was a dry-run, present a small preview dialog listing planned moves
        if dry and moved:
            try:
                # build a readable list; truncate if too long
                lines = [f"{o} → {d}" for o, d in moved]
                text = "\n".join(lines)
                max_len = 16000
                if len(text) > max_len:
                    text = text[:max_len] + "\n... (truncated)"
                # append full list to log as well
                self.clean_log.append("\nDRY RUN PREVIEW (orig -> dest):")
                for l in lines[:200]:
                    self.clean_log.append(l)
                if len(lines) > 200:
                    self.clean_log.append(f"... (and {len(lines)-200} more)\n")

                QMessageBox.information(self, "Dry-run Preview", text)
            except Exception:
                pass
        self.clean_quarantine_btn.setEnabled(True)
        try:
            # refresh scan to reflect moved/deleted files
            self.run_clean_scan()
        except Exception:
            pass

    def reveal_selected_item(self):
        item = self.validation_items.currentItem()
        if not item:
            return
        path = item.text()
        folder = os.path.dirname(path)
        if not folder:
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(folder))

    def resolve_selected_item(self):
        item = self.validation_items.currentItem()
        if not item:
            return
        row = self.validation_items.currentRow()
        self.validation_items.takeItem(row)

    # ═══════════════════════════════════════════════════════════════════
    # Qt Events
    # ═══════════════════════════════════════════════════════════════════

    def showEvent(self, event):
        super().showEvent(event)
        try:
            self.page_tag_finder._refresh_info()
        except Exception:
            pass

    def closeEvent(self, event):
        """Ensure background threads are stopped cleanly on window close."""
        try:
            if getattr(self, "runner", None):
                try:
                    self.runner.stop()
                except Exception:
                    pass
                try:
                    if self.runner.isRunning():
                        self.runner.wait(2000)
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if getattr(self, "page_tag_finder", None):
                if self.page_tag_finder._runner and self.page_tag_finder._runner.isRunning():
                    self.page_tag_finder._runner.stop()
                    self.page_tag_finder._runner.wait(2000)
        except Exception:
            pass

        try:
            if getattr(self, "validation_runner", None):
                try:
                    if self.validation_runner.isRunning():
                        # best-effort terminate
                        try:
                            self.validation_runner.terminate()
                        except Exception:
                            pass
                        try:
                            self.validation_runner.wait(2000)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        super().closeEvent(event)

    def resizeEvent(self, event):
        """Show a small hint when the window is narrow or when the current
        page has vertical overflow, reminding users to use the scrollbar.
        """
        try:
            show_hint = False
            # prefer explicit narrow-window hint
            if self.width() < 1000:
                show_hint = True
            else:
                # if the current stacked widget page is a QScrollArea, check
                # whether vertical scrolling is possible
                try:
                    cur = self.stack.currentWidget()
                    from PySide6.QtWidgets import QScrollArea
                    if isinstance(cur, QScrollArea):
                        v = cur.verticalScrollBar()
                        if v and v.maximum() > 0:
                            show_hint = True
                except Exception:
                    pass
            if getattr(self, "scroll_hint", None):
                self.scroll_hint.setVisible(show_hint)
        except Exception:
            pass
        super().resizeEvent(event)

    # ═══════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════

    def _divider(self):
        line = QFrame()
        line.setObjectName("divider")
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        return line
