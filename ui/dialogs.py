"""
ui/dialogs.py — Shared dialog utilities for DJ Library Manager UI.

Centralises reusable dialog patterns so they stay consistent across pages
and only need updating in one place.

Usage:
    from ui.dialogs import confirm_delete

Current utilities:
    confirm_delete(parent, count, description) → bool
        Standard Yes/No for < MASS_DELETE_THRESHOLD files.
        Typed "DELETE" confirmation for >= MASS_DELETE_THRESHOLD files.
"""

from PySide6.QtWidgets import (
    QMessageBox, QDialog, QVBoxLayout, QLabel,
    QLineEdit, QDialogButtonBox
)
from PySide6.QtCore import Qt

# ── Constants ─────────────────────────────────────────────────────────────────
# Change these in one place to update behaviour across every page that uses
# confirm_delete().
MASS_DELETE_THRESHOLD = 100   # files at or above this count require typed confirmation
CONFIRM_KEYWORD       = "DELETE"


def confirm_delete(parent, count: int, description: str) -> bool:
    """Show an appropriate confirmation dialog for a permanent deletion.

    For count < MASS_DELETE_THRESHOLD: standard Yes/No QMessageBox, default No.
    For count >= MASS_DELETE_THRESHOLD: modal dialog requiring the user to type
    CONFIRM_KEYWORD exactly before the Delete button becomes active. This
    prevents accidental mass-deletion of large file sets.

    Args:
        parent:      Parent widget for the dialog.
        count:       Number of files to be permanently deleted.
        description: Short plain-English context shown in the dialog body,
                     e.g. "duplicate files" or "selected non-audio files".
                     Keep it lowercase — it follows "delete N <description>".

    Returns:
        True if the user confirmed, False if they cancelled.
    """
    if count < MASS_DELETE_THRESHOLD:
        reply = QMessageBox.question(
            parent,
            "Confirm Delete",
            f"Permanently delete {count:,} {description}?\n\n"
            "This cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        return reply == QMessageBox.Yes

    # ── Mass-delete: require typed confirmation ───────────────────────────────
    dlg = QDialog(parent)
    dlg.setWindowTitle("Confirm Mass Delete")
    dlg.setMinimumWidth(460)
    layout = QVBoxLayout(dlg)
    layout.setSpacing(14)
    layout.setContentsMargins(20, 20, 20, 20)

    warning_lbl = QLabel(
        f'<b style="color:#e05050;">⚠&nbsp; {count:,} files will be permanently deleted.</b>'
    )
    warning_lbl.setTextFormat(Qt.RichText)
    layout.addWidget(warning_lbl)

    desc_lbl = QLabel(
        f"You are about to permanently delete {count:,} {description}.\n"
        "This action cannot be undone and the files cannot be recovered.\n\n"
        f'Type <b>{CONFIRM_KEYWORD}</b> in the box below to confirm:'
    )
    desc_lbl.setTextFormat(Qt.RichText)
    desc_lbl.setWordWrap(True)
    layout.addWidget(desc_lbl)

    entry = QLineEdit()
    entry.setPlaceholderText(f"Type {CONFIRM_KEYWORD} to confirm")
    entry.setFixedHeight(34)
    layout.addWidget(entry)

    buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
    ok_btn  = buttons.button(QDialogButtonBox.Ok)
    ok_btn.setText("Delete")
    ok_btn.setEnabled(False)
    ok_btn.setStyleSheet("background-color: #7a1a1a; color: #f0f0f0;")
    buttons.rejected.connect(dlg.reject)
    buttons.accepted.connect(dlg.accept)
    layout.addWidget(buttons)

    entry.textChanged.connect(
        lambda text: ok_btn.setEnabled(text.strip() == CONFIRM_KEYWORD)
    )
    entry.returnPressed.connect(
        lambda: dlg.accept() if ok_btn.isEnabled() else None
    )

    return dlg.exec() == QDialog.Accepted
