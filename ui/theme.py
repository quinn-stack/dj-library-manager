"""
DJ Library Manager — Theme
Dark DJ booth aesthetic: near-black background, warm orange accent, monospace type.

Font sizes are dynamic — call build_stylesheet(font_size) to get a stylesheet
at any base font size. DARK_STYLESHEET is the default (12px) for convenience.
"""

ACCENT       = "#ff6b35"
ACCENT_DIM   = "#cc4f1f"
BG_DARK      = "#0d0d0d"
BG_PANEL     = "#1a1a1a"
BG_CARD      = "#222222"
TEXT_PRIMARY = "#f0f0f0"
TEXT_DIM     = "#888888"
SUCCESS      = "#4caf82"
WARNING      = "#f0c040"
DANGER       = "#e05050"
BORDER       = "#333333"

# Default font size (px). Slider range is 10–22.
DEFAULT_FONT_SIZE = 12


def build_stylesheet(font_size: int = DEFAULT_FONT_SIZE) -> str:
    """Return the full application stylesheet at the given base font size.

    Heading and subheading sizes scale proportionally:
      heading    = font_size + 2
      title      = font_size + 8
      subheading = max(font_size - 1, 10)
      sidebar    = font_size
    """
    fs        = int(font_size)
    fs_title  = fs + 8
    fs_head   = fs + 2
    fs_sub    = max(fs - 1, 10)
    fs_side   = fs
    return f"""
QWidget {{
    background-color: {BG_DARK};
    color: {TEXT_PRIMARY};
    font-family: "Courier New", Courier, monospace;
    font-size: {fs}px;
}}

QMainWindow {{
    background-color: {BG_DARK};
}}

QLabel {{
    color: {TEXT_PRIMARY};
    background-color: transparent;
}}

QLabel#title {{
    color: {ACCENT};
    font-size: {fs_title}px;
    font-weight: bold;
    letter-spacing: 3px;
}}

QLabel#heading {{
    color: {ACCENT};
    font-size: {fs_head}px;
    font-weight: bold;
}}

QLabel#subheading {{
    color: {TEXT_DIM};
    font-size: {fs_sub}px;
}}

QLabel#status_idle {{
    color: {TEXT_DIM};
    font-size: {fs_sub}px;
}}

QLabel#status_running {{
    color: {WARNING};
    font-size: {fs_sub}px;
    font-weight: bold;
}}

QLabel#status_ok {{
    color: {SUCCESS};
    font-size: {fs_sub}px;
    font-weight: bold;
}}

QLabel#status_error {{
    color: {DANGER};
    font-size: {fs_sub}px;
    font-weight: bold;
}}

QPushButton {{
    background-color: {BG_CARD};
    color: {TEXT_PRIMARY};
    border: 1px solid {BORDER};
    padding: 7px 14px;
    border-radius: 2px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs_sub}px;
    font-weight: bold;
    letter-spacing: 1px;
}}

QPushButton:hover {{
    background-color: {ACCENT};
    color: {BG_DARK};
    border: 1px solid {ACCENT};
}}

QPushButton:pressed {{
    background-color: {ACCENT_DIM};
    color: {BG_DARK};
}}

QPushButton:disabled {{
    background-color: {BG_PANEL};
    color: {BORDER};
    border: 1px solid {BG_CARD};
}}

QPushButton#primary {{
    background-color: {ACCENT};
    color: {BG_DARK};
    border: none;
    font-weight: bold;
}}

QPushButton#primary:hover {{
    background-color: {ACCENT_DIM};
    color: {BG_DARK};
}}

QPushButton#primary:disabled {{
    background-color: {BORDER};
    color: {BG_CARD};
}}

QPushButton#danger {{
    background-color: {BG_CARD};
    color: {DANGER};
    border: 1px solid {DANGER};
}}

QPushButton#danger:hover {{
    background-color: {DANGER};
    color: {BG_DARK};
}}

QPushButton#warning {{
    background-color: {BG_CARD};
    color: {WARNING};
    border: 1px solid {WARNING};
}}

QPushButton#warning:hover {{
    background-color: {WARNING};
    color: {BG_DARK};
}}

QPushButton#success {{
    background-color: {BG_CARD};
    color: {SUCCESS};
    border: 1px solid {SUCCESS};
}}

QPushButton#success:hover {{
    background-color: {SUCCESS};
    color: {BG_DARK};
}}

QPushButton#sidebar {{
    background-color: transparent;
    color: {TEXT_DIM};
    border: none;
    border-left: 3px solid transparent;
    border-radius: 0px;
    text-align: left;
    padding: 10px 16px;
    font-size: {fs_side}px;
    letter-spacing: 2px;
    font-weight: bold;
}}

QPushButton#sidebar:hover {{
    background-color: {BG_CARD};
    color: {TEXT_PRIMARY};
    border-left: 3px solid {BORDER};
}}

QPushButton#sidebar_active {{
    background-color: {BG_CARD};
    color: {ACCENT};
    border: none;
    border-left: 3px solid {ACCENT};
    border-radius: 0px;
    text-align: left;
    padding: 10px 16px;
    font-size: {fs_side}px;
    font-weight: bold;
    letter-spacing: 2px;
}}

QLineEdit {{
    background-color: {BG_PANEL};
    color: {TEXT_PRIMARY};
    border: 1px solid {BORDER};
    border-radius: 2px;
    padding: 5px 8px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs}px;
    selection-background-color: {ACCENT};
    selection-color: {BG_DARK};
}}

QLineEdit:focus {{
    border: 1px solid {ACCENT};
}}

QLineEdit:disabled {{
    color: {TEXT_DIM};
    background-color: {BG_CARD};
}}

QTextEdit, QPlainTextEdit {{
    background-color: {BG_PANEL};
    color: {TEXT_PRIMARY};
    border: 1px solid {BORDER};
    border-radius: 2px;
    padding: 6px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs_sub}px;
    selection-background-color: {ACCENT};
    selection-color: {BG_DARK};
}}

QTextEdit:focus, QPlainTextEdit:focus {{
    border: 1px solid {ACCENT};
}}

QComboBox {{
    background-color: {BG_PANEL};
    color: {TEXT_PRIMARY};
    border: 1px solid {BORDER};
    border-radius: 2px;
    padding: 5px 8px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs}px;
    min-width: 160px;
}}

QComboBox:focus {{
    border: 1px solid {ACCENT};
}}

QComboBox::drop-down {{
    border: none;
    width: 24px;
}}

QComboBox QAbstractItemView {{
    background-color: {BG_PANEL};
    color: {TEXT_PRIMARY};
    border: 1px solid {BORDER};
    selection-background-color: {ACCENT};
    selection-color: {BG_DARK};
    outline: none;
}}

QScrollBar:vertical {{
    background-color: {BG_DARK};
    width: 8px;
    margin: 0;
}}

QScrollBar::handle:vertical {{
    background-color: {BORDER};
    border-radius: 4px;
    min-height: 20px;
}}

QScrollBar::handle:vertical:hover {{
    background-color: {ACCENT};
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}

QScrollBar:horizontal {{
    background-color: {BG_DARK};
    height: 8px;
}}

QScrollBar::handle:horizontal {{
    background-color: {BORDER};
    border-radius: 4px;
}}

QScrollBar::handle:horizontal:hover {{
    background-color: {ACCENT};
}}

QSlider::groove:horizontal {{
    background-color: {BG_CARD};
    border: 1px solid {BORDER};
    height: 4px;
    border-radius: 2px;
}}

QSlider::handle:horizontal {{
    background-color: {ACCENT};
    border: none;
    width: 12px;
    height: 12px;
    margin: -4px 0;
    border-radius: 6px;
}}

QSlider::handle:horizontal:hover {{
    background-color: {ACCENT_DIM};
}}

QSlider::sub-page:horizontal {{
    background-color: {ACCENT};
    border-radius: 2px;
}}

QFrame {{
    background-color: transparent;
}}

QFrame#card {{
    background-color: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 2px;
}}

QFrame#panel {{
    background-color: {BG_PANEL};
    border: none;
}}

QFrame#divider {{
    background-color: {BORDER};
    max-height: 1px;
    border: none;
}}

QFrame#accent_bar {{
    background-color: {ACCENT};
    max-height: 2px;
    border: none;
}}

QProgressBar {{
    background-color: {BG_CARD};
    border: 1px solid {BORDER};
    border-radius: 2px;
    height: 6px;
    text-align: center;
    color: transparent;
}}

QProgressBar::chunk {{
    background-color: {ACCENT};
    border-radius: 2px;
}}

QToolTip {{
    background-color: {BG_CARD};
    color: {TEXT_PRIMARY};
    border: 1px solid {ACCENT};
    padding: 4px 8px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs_sub}px;
}}

QCheckBox {{
    color: {TEXT_PRIMARY};
    spacing: 8px;
    font-family: "Courier New", Courier, monospace;
}}

QCheckBox::indicator {{
    width: 14px;
    height: 14px;
    border: 1px solid {BORDER};
    background-color: {BG_PANEL};
    border-radius: 2px;
}}

QCheckBox::indicator:checked {{
    background-color: {ACCENT};
    border: 1px solid {ACCENT};
}}

QStatusBar {{
    background-color: {BG_PANEL};
    color: {TEXT_DIM};
    border-top: 1px solid {BORDER};
    font-family: "Courier New", Courier, monospace;
    font-size: {fs_sub}px;
}}

QGroupBox {{
    border: 1px solid {BORDER};
    border-radius: 2px;
    margin-top: 12px;
    padding-top: 8px;
    font-family: "Courier New", Courier, monospace;
    font-size: {fs_sub}px;
    color: {TEXT_DIM};
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 6px;
    color: {TEXT_DIM};
    letter-spacing: 1px;
}}

QFormLayout QLabel {{
    color: {TEXT_DIM};
    font-size: {fs_sub}px;
    letter-spacing: 1px;
}}
"""


# Default stylesheet at 12px for import convenience
DARK_STYLESHEET = build_stylesheet(DEFAULT_FONT_SIZE)
