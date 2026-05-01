import json
from pathlib import Path

try:
    from .platform_adapter import PlatformAdapter as _PA
except ImportError:
    try:
        from platform_adapter import PlatformAdapter as _PA
    except ImportError:
        _PA = None

# ── ENGINE DEPENDENCY ─────────────────────────────────────────────────────────
# Depends on: engine/platform_adapter.py  →  PlatformAdapter.get_path_limit()
# Why: get_path_limit() is the single OS-aware source of truth for path length
#      defaults. Hardcoding 260 here would silently give Linux/macOS users the
#      wrong default.
# If you modify platform_adapter.py, verify this module still works correctly.
# ─────────────────────────────────────────────────────────────────────────────


def _default_path_limit():
    """Return the OS-appropriate path limit, or 260 (conservative) as fallback."""
    if _PA is not None:
        return _PA.get_path_limit()
    return 260


class SettingsManager:
    def __init__(self):
        self.base_dir      = Path.home() / ".dj_library_manager"
        self.settings_file = self.base_dir / "settings.json"
        self.base_dir.mkdir(exist_ok=True)

        self.defaults = {
            "last_profile": None,
            "acoustid_api_key": "",
            # directory where files moved to quarantine will be stored by default.
            # Do not create this directory automatically; it will be created
            # only when the user executes a move-to-quarantine action.
            "quarantine_dir": str(Path.home() / "Music" / "_QUARANTINE"),
            # whether the quarantine_dir was explicitly customized by the user
            "quarantine_dir_customized": False,
            "validation": {
                "path_length_limit":    _default_path_limit(),
                "low_confidence_cutoff": None,
                "log_retention":        20,
                # AcoustID request rate. Hard max is 3 RPS (API limit).
                # Users may reduce this when the server is under stress or they
                # are seeing a high rate of API errors.  Never write a value
                # higher than 3 into settings — the engine clamps it, but we
                # keep the stored value honest.
                "acoustid_rps":         3,
            },
            "threshold_preset": "Certainty",
            "threshold_map": {
                "Certainty": {
                    "strong": 0.95,
                    "medium": 0.90,
                    "gap": 0.35,
                    "label": "CERTAINTY — SAFE",
                    "description": "Highest accuracy. Only applies tags when the match is near-certain. Minimal manual review required. Recommended for DJ libraries.",
                    "risk": "safe"
                },
                "Close": {
                    "strong": 0.90,
                    "medium": 0.80,
                    "gap": 0.25,
                    "label": "CLOSE — MODERATE",
                    "description": "Tags may occasionally be incorrect, particularly for remixes, live versions, or obscure tracks. Review recommended after import.",
                    "risk": "warning"
                },
                "Unsure": {
                    "strong": 0.80,
                    "medium": 0.70,
                    "gap": 0.15,
                    "label": "UNSURE — RISKY",
                    "description": "It is likely that many tags will be wrong. Use only if you plan to manually audit every track after import. Not recommended for active DJ libraries.",
                    "risk": "danger"
                }
            }
        }

        if not self.settings_file.exists():
            self.save_settings(self.defaults)

    def load_settings(self):
        try:
            with open(self.settings_file, "r") as f:
                settings = json.load(f)
                for key, val in self.defaults.items():
                    if key not in settings:
                        settings[key] = val
                # Migrate: ensure validation sub-keys exist for users with older
                # settings files that predate acoustid_rps being added.
                v = settings.setdefault("validation", {})
                for k, default_val in self.defaults["validation"].items():
                    if k not in v:
                        v[k] = default_val
                return settings
        except Exception:
            self.save_settings(self.defaults)
            return self.defaults.copy()

    def save_settings(self, settings_data):
        with open(self.settings_file, "w") as f:
            json.dump(settings_data, f, indent=4)

    def get_setting(self, key):
        return self.load_settings().get(key, self.defaults.get(key))

    def update_setting(self, key, value):
        settings = self.load_settings()
        settings[key] = value
        self.save_settings(settings)

    def get_last_profile(self):
        return self.get_setting("last_profile")

    def set_last_profile(self, profile_name):
        self.update_setting("last_profile", profile_name)

    def get_active_thresholds(self):
        settings = self.load_settings()
        preset   = settings.get("threshold_preset", "Certainty")
        return settings["threshold_map"].get(preset, self.defaults["threshold_map"]["Certainty"])

    def get_validation_settings(self):
        settings = self.load_settings()
        return settings.get("validation", self.defaults.get("validation", {
            "path_length_limit": 240,
            "low_confidence_cutoff": None,
            "acoustid_rps": 3,
        }))

    def get_validation_cutoff(self):
        """Return the low-confidence cutoff to use for extraction.
        If the user configured a custom cutoff under `validation.low_confidence_cutoff`, use it.
        Otherwise fall back to the active preset's `medium` threshold.
        """
        v      = self.get_validation_settings()
        custom = v.get("low_confidence_cutoff")
        if custom is not None:
            return float(custom)
        thresh = self.get_active_thresholds()
        return float(thresh.get("medium", 0.9))

    def get_acoustid_rps(self) -> float:
        """Return the configured AcoustID request rate (RPS).

        Reads from validation.acoustid_rps. Clamped to [0.1, 3.0] here so the
        engine always receives a sane value regardless of what the settings file
        contains.
        """
        v = self.get_validation_settings()
        raw = v.get("acoustid_rps", 3)
        try:
            return max(0.1, min(float(raw), 3.0))
        except (TypeError, ValueError):
            return 3.0

    def get_quarantine_dir(self):
        settings = self.load_settings()
        return str(settings.get("quarantine_dir") or str(Path.home() / "Music" / "_QUARANTINE"))

    def get_quarantine_dir_for_source(self, source_path: str = None) -> str:
        """Return the effective quarantine directory for a given source.

        If the user has explicitly customized the quarantine_dir via
        `set_quarantine_dir()` (tracked by `quarantine_dir_customized`),
        return that. Otherwise, if `source_path` is provided, return
        `<source_path>/_QUARANTINE` (does not create it). Falls back to the
        stored `quarantine_dir`.
        """
        settings   = self.load_settings()
        customized = bool(settings.get("quarantine_dir_customized", False))
        stored     = settings.get("quarantine_dir") or str(Path.home() / "Music" / "_QUARANTINE")
        if customized:
            return str(stored)
        if source_path:
            try:
                return str(Path(source_path) / "_QUARANTINE")
            except Exception:
                return str(stored)
        return str(stored)

    def set_quarantine_dir(self, path):
        settings = self.load_settings()
        settings["quarantine_dir"]            = str(path)
        settings["quarantine_dir_customized"] = True
        self.save_settings(settings)
