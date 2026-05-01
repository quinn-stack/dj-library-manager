# main_window.py — Wiring Changes for Tag Finder

These are the exact changes needed to wire the new engine into main_window.py.
Everything else in main_window stays untouched.

---

## 1. Add import at the top (replace old beets imports)

REMOVE these lines:
```python
from engine.beets_engine import BeetsEngine
```

ADD this line in their place:
```python
from ui.tag_finder_page import TagFinderPage
```

---

## 2. In _build_ui() — replace page_beets with page_tag_finder

FIND (around line 92):
```python
self.page_beets = self._wrap_page_with_scroll(self._create_beets_page())
```

REPLACE WITH:
```python
self.page_tag_finder = TagFinderPage(self.settings_manager, self.profile_manager)
self.page_beets = self._wrap_page_with_scroll(self.page_tag_finder)
```

Note: keeping the variable name `page_beets` means nothing else in the sidebar
wiring needs to change — it all still maps to `page_beets` by attribute name.

---

## 3. In _build_ui() — add the page to the stack (no change needed)

The line:
```python
self.stack.addWidget(self.page_beets)
```
...stays exactly as-is.

---

## 4. In load_selected_profile() — notify the tag finder of profile changes

FIND (around line 601):
```python
def load_selected_profile(self, name):
    profile = self.profile_manager.load_profile(name)
    if not profile:
        return
    self.profile_name_input.setText(profile.get("profile_name", ""))
    self.source_path_input.setText(profile.get("source_path", ""))
    self.destination_path_input.setText(profile.get("destination_path", ""))
```

ADD this line immediately after the setText calls:
```python
    # Notify Tag Finder of profile change
    try:
        self.page_tag_finder.set_profile(name)
    except Exception:
        pass
```

---

## 5. In showEvent() — refresh tag finder info when window shows

FIND (around line 1734):
```python
def showEvent(self, event):
    """Refresh beets info every time the window shows."""
    super().showEvent(event)
    self._refresh_beets_info()
```

REPLACE WITH:
```python
def showEvent(self, event):
    super().showEvent(event)
    try:
        self.page_tag_finder._refresh_info()
    except Exception:
        pass
```

---

## 6. In closeEvent() — stop the tag finder runner on close

FIND the runner stop block in closeEvent() (around line 1742):
```python
if getattr(self, "runner", None):
    try:
        self.runner.stop()
    except Exception:
        pass
```

ADD after that block:
```python
        try:
            if getattr(self, "page_tag_finder", None):
                if self.page_tag_finder._runner and self.page_tag_finder._runner.isRunning():
                    self.page_tag_finder._runner.stop()
                    self.page_tag_finder._runner.wait(2000)
        except Exception:
            pass
```

---

## 7. Remove dead code (optional but recommended)

The following methods in main_window.py are now unused and can be deleted:
- `_create_beets_page()`
- `_refresh_beets_info()`
- `run_beets_import()`
- `stop_beets_import()`
- `beets_finished()`
- `_beets_log_append()`
- `_set_status()` (only if only used by beets page — check first)
- `run_apply_tags()`
- `apply_tags_finished()`
- `run_revert_from_report()`
- `revert_finished()`

Also remove `self._temp_config_path = None` from `__init__`.

---

## File placement

```
DJLibraryManager/
├── engine/
│   ├── acoustid_engine.py      ← NEW — drop here
│   └── ... (all existing engine files unchanged)
└── ui/
    ├── tag_finder_page.py      ← NEW — drop here
    ├── main_window.py          ← apply changes above
    └── ... (all existing ui files unchanged)
```

beets_engine.py can be deleted entirely.
