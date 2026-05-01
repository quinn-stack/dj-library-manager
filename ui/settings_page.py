"""
DJ Library Manager — Settings Page
Persistent settings for AcoustID API key and tagging certainty preset.
Designed so any DJ can configure the app without touching YAML or terminal.
"""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QButtonGroup, QRadioButton,
    QFrame, QSizePolicy, QSpinBox, QDoubleSpinBox, QFormLayout
)
from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices

try:
    from engine.platform_adapter import PlatformAdapter
except ImportError:
    try:
        from platform_adapter import PlatformAdapter
    except ImportError:
        PlatformAdapter = None


# Hard limit imposed by the AcoustID API — do not raise this.
_ACOUSTID_RPS_MAX = 3


class SettingsPage(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings_mgr = settings_manager
        self._build_ui()
        self._load_values()

    # ═══════════════════════════════════════════════════════════════════
    # UI Construction
    # ═══════════════════════════════════════════════════════════════════

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 20, 24, 20)
        layout.setSpacing(20)
        layout.setAlignment(Qt.AlignTop)

        heading = QLabel("SETTINGS")
        heading.setObjectName("heading")
        layout.addWidget(heading)
        layout.addWidget(self._divider())

        layout.addWidget(self._build_api_section())
        layout.addWidget(self._divider())
        layout.addWidget(self._build_threshold_section())
        layout.addWidget(self._divider())
        layout.addWidget(self._build_validation_section())

        layout.addStretch()

    def _build_api_section(self):
        section = QWidget()
        layout  = QVBoxLayout(section)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        title = QLabel("ACOUSTID API KEY")
        title.setObjectName("heading")
        layout.addWidget(title)

        sub = QLabel(
            "Required for fingerprint-based tagging. Each DJ should register "
            "their own free key — it takes 30 seconds."
        )
        sub.setObjectName("subheading")
        sub.setWordWrap(True)
        layout.addWidget(sub)

        key_card   = QFrame()
        key_card.setObjectName("card")
        key_layout = QVBoxLayout(key_card)
        key_layout.setContentsMargins(16, 12, 16, 16)
        key_layout.setSpacing(8)

        key_label = QLabel("LOOKUP KEY:")
        key_label.setObjectName("subheading")
        key_layout.addWidget(key_label)

        key_row = QHBoxLayout()
        self.api_input = QLineEdit()
        self.api_input.setPlaceholderText("Paste your AcoustID Lookup Key here")
        self.api_input.setEchoMode(QLineEdit.Password)
        self.api_input.textChanged.connect(self._save_api_key)
        key_row.addWidget(self.api_input)

        self.show_key_btn = QPushButton("SHOW")
        self.show_key_btn.setFixedWidth(70)
        self.show_key_btn.setCheckable(True)
        self.show_key_btn.toggled.connect(self._toggle_key_visibility)
        key_row.addWidget(self.show_key_btn)
        key_layout.addLayout(key_row)

        get_key_btn = QPushButton("GET FREE API KEY  ↗")
        get_key_btn.setObjectName("primary")
        get_key_btn.setFixedWidth(220)
        get_key_btn.clicked.connect(
            lambda: QDesktopServices.openUrl(QUrl("https://acoustid.org/new-application"))
        )
        key_layout.addWidget(get_key_btn)

        key_note = QLabel("You need a Lookup Key, not the Submit Key — they are different.")
        key_note.setObjectName("subheading")
        key_note.setWordWrap(True)
        key_layout.addWidget(key_note)

        layout.addWidget(key_card)
        return section

    def _build_threshold_section(self):
        section = QWidget()
        layout  = QVBoxLayout(section)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        title = QLabel("TAGGING CERTAINTY")
        title.setObjectName("heading")
        layout.addWidget(title)

        sub = QLabel(
            "Controls how confident AcoustID must be before a tag is applied. "
            "Higher certainty means fewer matches but fewer mistakes."
        )
        sub.setObjectName("subheading")
        sub.setWordWrap(True)
        layout.addWidget(sub)

        self.btn_group  = QButtonGroup(self)
        self.radio_cards = {}

        presets = [
            ("Certainty", "✔  CERTAINTY — SAFE",
             "strong=0.95  medium=0.90",
             "Highest accuracy. Only applies tags when the match is near-certain.\nMinimal manual review required. Recommended for all DJ libraries."),
            ("Close", "▲  CLOSE — MODERATE",
             "strong=0.90  medium=0.80",
             "Tags may occasionally be incorrect, particularly for remixes or\nlive versions. Review recommended after import."),
            ("Unsure", "⚠  UNSURE — RISKY",
             "strong=0.80  medium=0.70",
             "Many tags will likely be wrong. Use only if you plan to manually\naudit every track. Not recommended for active libraries."),
        ]

        for i, (key, label, thresh_display, description) in enumerate(presets):
            card = self._build_preset_card(i, key, label, thresh_display, description)
            layout.addWidget(card)
            self.radio_cards[key] = card

        self.warning_banner = QLabel("")
        self.warning_banner.setWordWrap(True)
        self.warning_banner.setContentsMargins(16, 12, 16, 12)
        self.warning_banner.setMinimumHeight(48)
        layout.addWidget(self.warning_banner)

        return section

    def _build_preset_card(self, idx, key, label, thresh_display, description):
        card        = QFrame()
        card.setObjectName("card")
        card_layout = QHBoxLayout(card)
        card_layout.setContentsMargins(16, 12, 16, 12)
        card_layout.setSpacing(16)

        radio = QRadioButton()
        self.btn_group.addButton(radio, idx)
        radio.clicked.connect(lambda checked, k=key: self._set_preset(k))
        card_layout.addWidget(radio)
        setattr(self, f"radio_{key.lower()}", radio)

        text_col = QVBoxLayout()
        text_col.setSpacing(2)

        label_widget  = QLabel(label)
        label_widget.setObjectName("heading")
        thresh_widget = QLabel(thresh_display)
        thresh_widget.setObjectName("subheading")
        desc_widget   = QLabel(description)
        desc_widget.setObjectName("subheading")
        desc_widget.setWordWrap(True)

        text_col.addWidget(label_widget)
        text_col.addWidget(thresh_widget)
        text_col.addWidget(desc_widget)
        card_layout.addLayout(text_col)
        card_layout.addStretch()

        return card

    def _build_validation_section(self):
        section = QWidget()
        layout  = QVBoxLayout(section)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        title = QLabel("VALIDATION SETTINGS")
        title.setObjectName("heading")
        layout.addWidget(title)

        sub = QLabel("Controls for pre-transfer validation, log retention, and API rate limiting.")
        sub.setObjectName("subheading")
        sub.setWordWrap(True)
        layout.addWidget(sub)

        card        = QFrame()
        card.setObjectName("card")
        card_layout = QFormLayout(card)
        card_layout.setLabelAlignment(Qt.AlignRight)

        # ── Path length limit ────────────────────────────────────────────────
        self.pathlen_input = QSpinBox()
        self.pathlen_input.setRange(100, 4096)
        _os_limit = PlatformAdapter.get_path_limit() if PlatformAdapter else 260
        self.pathlen_input.setValue(_os_limit)
        self.pathlen_input.setToolTip(
            f"Default for this OS: {_os_limit}  "
            "(Windows=260, macOS=1024, Linux=4096)"
        )
        self.pathlen_input.valueChanged.connect(self._save_validation_settings)
        card_layout.addRow("Path length limit:", self.pathlen_input)

        # ── Low-confidence cutoff ─────────────────────────────────────────────
        self.cutoff_input = QDoubleSpinBox()
        self.cutoff_input.setRange(0.0, 1.0)
        self.cutoff_input.setSingleStep(0.01)
        self.cutoff_input.setDecimals(2)
        self.cutoff_input.valueChanged.connect(self._save_validation_settings)
        card_layout.addRow("Low-confidence cutoff:", self.cutoff_input)

        # ── Log retention ─────────────────────────────────────────────────────
        self.retention_input = QSpinBox()
        self.retention_input.setRange(1, 200)
        self.retention_input.setValue(20)
        self.retention_input.valueChanged.connect(self._save_validation_settings)
        card_layout.addRow("Keep validation logs:", self.retention_input)

        # ── AcoustID RPS ──────────────────────────────────────────────────────
        # Separate widget + label row so we can attach the warning banner below it.
        self.rps_input = QSpinBox()
        self.rps_input.setRange(1, _ACOUSTID_RPS_MAX)
        self.rps_input.setValue(_ACOUSTID_RPS_MAX)
        self.rps_input.setSuffix(" RPS")
        self.rps_input.setToolTip(
            "AcoustID API rate limit (requests per second).\n"
            f"Maximum allowed by the API is {_ACOUSTID_RPS_MAX} RPS.\n"
            "Reduce to 1 or 2 if you are getting a high number of API errors\n"
            "during a tagging run — this gives the server more breathing room."
        )
        self.rps_input.valueChanged.connect(self._on_rps_changed)
        card_layout.addRow("AcoustID lookup rate:", self.rps_input)

        layout.addWidget(card)

        # Warning banner — shown when RPS is reduced below max
        self.rps_banner = QLabel("")
        self.rps_banner.setWordWrap(True)
        self.rps_banner.setContentsMargins(16, 10, 16, 10)
        self.rps_banner.setVisible(False)
        layout.addWidget(self.rps_banner)

        return section

    # ═══════════════════════════════════════════════════════════════════
    # Logic
    # ═══════════════════════════════════════════════════════════════════

    def _load_values(self):
        # API key
        key = self.settings_mgr.get_setting("acoustid_api_key") or ""
        self.api_input.setText(key)

        # Threshold preset
        preset = self.settings_mgr.get_setting("threshold_preset") or "Certainty"
        radio  = getattr(self, f"radio_{preset.lower()}", self.radio_certainty)
        radio.setChecked(True)
        self._update_warning(preset)

        # Validation settings
        _os_limit = PlatformAdapter.get_path_limit() if PlatformAdapter else 260
        v         = self.settings_mgr.get_validation_settings()

        self.pathlen_input.setValue(int(v.get("path_length_limit", _os_limit)))

        cutoff = v.get("low_confidence_cutoff")
        if cutoff is None:
            self.cutoff_input.setValue(
                float(self.settings_mgr.get_active_thresholds().get("medium", 0.9))
            )
        else:
            self.cutoff_input.setValue(float(cutoff))

        self.retention_input.setValue(int(v.get("log_retention", 20)))

        # RPS — load stored value, clamp to [1, max]
        stored_rps = int(v.get("acoustid_rps", _ACOUSTID_RPS_MAX))
        stored_rps = max(1, min(stored_rps, _ACOUSTID_RPS_MAX))
        self.rps_input.setValue(stored_rps)
        self._update_rps_banner(stored_rps)

    def _save_validation_settings(self):
        settings = self.settings_mgr.load_settings()
        v        = settings.get("validation", {})
        v["path_length_limit"]    = int(self.pathlen_input.value())
        v["low_confidence_cutoff"] = float(self.cutoff_input.value())
        v["log_retention"]        = int(self.retention_input.value())
        v["acoustid_rps"]         = int(self.rps_input.value())
        settings["validation"]    = v
        self.settings_mgr.save_settings(settings)

    def _on_rps_changed(self, value: int):
        """Save and update the RPS warning banner whenever the spinbox changes."""
        self._save_validation_settings()
        self._update_rps_banner(value)

    def _update_rps_banner(self, rps: int):
        """Show a contextual note about the current RPS setting."""
        if rps >= _ACOUSTID_RPS_MAX:
            self.rps_banner.setVisible(False)
            self.rps_banner.setText("")
        elif rps == 2:
            self.rps_banner.setText(
                "▲  Rate reduced to 2 RPS. Use this if you are seeing occasional "
                "API errors. Tagging will take ~50% longer per file."
            )
            self.rps_banner.setStyleSheet(
                "background-color: #2b2000; color: #f0c040; "
                "border: 1px solid #f0c040; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )
            self.rps_banner.setVisible(True)
        else:  # 1 RPS
            self.rps_banner.setText(
                "⚠  Rate reduced to 1 RPS — server stress mode. Use only when the "
                "server is consistently returning errors. Tagging will take ~3× longer."
            )
            self.rps_banner.setStyleSheet(
                "background-color: #2b0a0a; color: #e05050; "
                "border: 1px solid #e05050; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )
            self.rps_banner.setVisible(True)

    def _save_api_key(self, text):
        self.settings_mgr.update_setting("acoustid_api_key", text)

    def _toggle_key_visibility(self, checked):
        if checked:
            self.api_input.setEchoMode(QLineEdit.Normal)
            self.show_key_btn.setText("HIDE")
        else:
            self.api_input.setEchoMode(QLineEdit.Password)
            self.show_key_btn.setText("SHOW")

    def _set_preset(self, preset):
        self.settings_mgr.update_setting("threshold_preset", preset)
        self._update_warning(preset)

    def _update_warning(self, preset):
        for key, card in self.radio_cards.items():
            card.setObjectName("card")
            card.style().unpolish(card)
            card.style().polish(card)

        if preset == "Certainty":
            self.warning_banner.setText(
                "✔  Best for DJs. High precision — tags will be correct or not applied at all."
            )
            self.warning_banner.setStyleSheet(
                "background-color: #0d2b1e; color: #4caf82; "
                "border: 1px solid #4caf82; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )
        elif preset == "Close":
            self.warning_banner.setText(
                "▲  WARNING — Tags may occasionally be incorrect. "
                "Review your library after import before using at a gig."
            )
            self.warning_banner.setStyleSheet(
                "background-color: #2b2000; color: #f0c040; "
                "border: 1px solid #f0c040; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )
        elif preset == "Unsure":
            self.warning_banner.setText(
                "⚠  DANGER — Many tags will be wrong. "
                "Full manual audit required before this library is gig-ready. "
                "Do not use on your active library without reviewing every track."
            )
            self.warning_banner.setStyleSheet(
                "background-color: #2b0a0a; color: #e05050; "
                "border: 1px solid #e05050; border-radius: 2px; "
                "font-family: 'Courier New'; font-size: 12px; padding: 12px;"
            )

    # ═══════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════

    def _divider(self):
        line = QFrame()
        line.setObjectName("divider")
        line.setFrameShape(QFrame.HLine)
        line.setFixedHeight(1)
        return line
