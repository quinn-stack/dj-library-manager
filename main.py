"""
DJ Library Manager — Entry Point
v0.4.12
"""

import sys
import os

# Ensure the project root is on the path regardless of where the script is
# launched from.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

from ui.main_window import MainWindow
from ui.theme import build_stylesheet, DEFAULT_FONT_SIZE


def main():
    # High-DPI support
    os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")

    app = QApplication(sys.argv)
    app.setApplicationName("DJ Library Manager")
    app.setOrganizationName("WadeQuinnEntertainment")
    app.setStyleSheet(build_stylesheet(DEFAULT_FONT_SIZE))

    # Raise file descriptor limits on Linux/macOS before anything else
    try:
        from engine.platform_adapter import PlatformAdapter
        status = PlatformAdapter.apply_safe_mode()
        print(f"[startup] {status}")
    except Exception as e:
        print(f"[startup] PlatformAdapter unavailable: {e}")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
