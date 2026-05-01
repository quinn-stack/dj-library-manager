# DJ Library Manager

> A pre-gig safety gate and library management tool built by a working KDJ who got tired of turning up to venues with a mess of badly tagged files, duplicates, and no way to verify anything transferred correctly.

![Version](https://img.shields.io/badge/version-0.5.8-blue)
![Python](https://img.shields.io/badge/python-3.10%2B-green)
![License](https://img.shields.io/badge/license-GPL%20v2-orange)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)
![Status](https://img.shields.io/badge/status-active%20development-yellow)

---

## What Is This?

DJ Library Manager is a cross-platform desktop application for DJs and KDJs who take their library seriously.

It started as a personal tool to solve a very real problem: a large, chaotic music library accumulated over years of gigging, with inconsistent tags, creeping duplicates, and no reliable way to verify files had transferred correctly before a show. What began as a quick script became a full application when it became clear the problem deserved a proper solution.

This is not a streaming tool. It is not a player. It is the thing you run **before** the gig to make sure everything is clean, verified, and ready.

---

## Features

### Tag Finder
- Acoustic fingerprinting via AcoustID / Chromaprint
- Automatically identifies and tags untagged or incorrectly tagged files
- Configurable confidence thresholds (Certainty / Close / Unsure presets)
- Low-confidence match review workflow — nothing gets applied without your approval
- Tag-based file renaming (`Artist - Title.ext`) with full undo support

### Duplicate Finder
- Hash-based and filename-based duplicate detection
- Automatic best-candidate scoring per duplicate group
- Quarantine or delete duplicates with mass-action safety gates
- Unresolvable / ambiguous file review workflow

### Library Cleaner
- Detects and removes non-audio files from your library root
- MIME-type sniffing via `python-magic` for accurate detection
- Quarantine support — nothing is permanently deleted without confirmation
- Mass-delete protection: requires typing `DELETE` to confirm bulk removals

### Transfer Engine
- SHA256 hash verification on every transferred file
- First-transfer and incremental modes
- Collision handling (skip, rename, overwrite — your choice)
- Dry-run mode before any live transfer
- OS-aware path length validation (260 / 1024 / 4096 per platform)
- System file exclusion (`.DS_Store`, `Thumbs.db`, `$RECYCLE.BIN`, etc.)

### Validation
- Path length scanner with OS-aware limits
- Duplicate filename and hash scanning
- Structured log output with configurable retention

### Health Check *(coming v0.6.0)*
- `mp3val` integration for MP3 corruption detection
- `ffmpeg`-based general audio health scanning
- Quarantine workflow for corrupted files

---

## Design Philosophy

**Engines and UI are strictly separated.**
All business logic lives in `engine/`. The UI never runs shell commands directly. All OS-specific behaviour is centralised in `PlatformAdapter`. This makes the codebase testable, maintainable, and extensible without touching the interface layer.

**Nothing is silent.**
Every failure is logged. Every destructive action requires confirmation. Mass deletions require typed confirmation. Quarantine is always offered before permanent removal.

**Gig-night failure conditions are assumed.**
This tool is designed to be run before an event, not during one. But the architecture assumes that if something can go wrong, it will, and handles it accordingly.

**No blocking operations on the main thread.**
All scanning, hashing, fingerprinting and transfer operations run in background threads via `QThread`. The UI remains responsive throughout.

---

## Installation

### Prerequisites

Install system dependencies for your platform:

**Linux (Debian/Ubuntu)**
```bash
sudo apt install libchromaprint-tools ffmpeg mp3val
pip install -r requirements.txt
```

**macOS**
```bash
brew install chromaprint ffmpeg mp3val
pip install -r requirements.txt
```

**Windows**
- Download `fpcalc.exe` from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) and place it on your PATH
- Install [ffmpeg](https://ffmpeg.org/download.html)
- Use `python-magic-bin` instead of `python-magic` (see `requirements.txt`)
```bash
pip install -r requirements.txt
```

### Python Dependencies
```bash
pip install -r requirements.txt
```

### AcoustID API Key
Tag Finder requires a free AcoustID API key.
Register at [acoustid.org/login](https://acoustid.org/login) and enter your key in Settings.

---

## Running the App

```bash
python main.py
```

Packaged builds (AppImage for Linux, `.exe` for Windows) are planned for v0.9.0.

---

## Project Structure

```
DJ_Library_Manager/
├── engine/                  # All business logic — no UI dependencies
│   ├── acoustid_engine.py   # AcoustID fingerprinting and tag lookup
│   ├── duplicate_finder.py  # Duplicate detection engine
│   ├── hash_utils.py        # SHA256 file hashing utilities
│   ├── health_check.py      # Audio corruption detection (mp3val / ffmpeg)
│   ├── library_clean.py     # Non-audio file detection and quarantine
│   ├── low_confidence_manager.py  # LC match batch management
│   ├── platform_adapter.py  # OS detection and platform-specific logic
│   ├── profile_manager.py   # Profile persistence
│   ├── settings_manager.py  # Settings with migration support
│   ├── tag_utils.py         # Shared tag validation helpers
│   ├── tagging.py           # Tag-based file renaming
│   ├── transfer_engine.py   # File transfer with hash verification
│   └── validator.py         # Path length and library validation
├── ui/                      # PySide6 interface layer
├── requirements.txt
└── main.py
```

---

## Roadmap

| Version | Focus | Status |
|---------|-------|--------|
| v0.1.0 | Foundation — CommandRunner, engine/UI separation | ✅ Complete |
| v0.2.0 | Beets hardening, stop button, config injection | ✅ Complete |
| v0.3.0 | Validation engine, path scanner, duplicate detection | ✅ Complete |
| v0.4.0 | Transfer engine, SHA256 verification, dry-run mode | ✅ Complete |
| v0.5.0 | Library cleaner, quarantine system, duplicate finder | ✅ Complete |
| v0.6.0 | Structured logging system | 🚧 In Progress |
| v0.7.0 | Health check UI wiring (mp3val + ffmpeg) | 🔜 Planned |
| v0.8.0 | UI polish, progress indicators, iconography | 🔜 Planned |
| v0.9.0 | Packaging — AppImage, .exe | 🔜 Planned |
| v1.0.0 | Stable production release | 🎯 Target |

**Post-1.0 ideas:**
- Rekordbox / Serato XML integration
- Automatic crate export
- Genre heatmap analyser
- BPM / Key analytics dashboard
- Cloud sync awareness

---

## Contributing

Contributions are very welcome. This project is actively developed and there is plenty of ground to cover before v1.0.

If you are a DJ, KDJ, or just someone who cares about audio library management, your real-world perspective is genuinely valuable — not just code contributions but bug reports, feature suggestions, and testing on hardware and library configurations the primary developer hasn't encountered.

**To get started:**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Run the manual test plan against your changes (`DJ_Library_Manager_Test_Plan.md`)
5. Submit a pull request with a clear description of what you changed and why

Please open an issue before starting significant work so we can discuss the approach first.

---

## A Note on AI Assistance

This project was heavily assisted by AI (Claude, by Anthropic) throughout its development — from architecture decisions and code review to debugging and documentation. That assistance is acknowledged openly and without hesitation.

The domain knowledge, real-world requirements, design decisions, and direction are entirely human. The AI was a tool, and a valuable one. The project would not have reached this point without it.

This feels worth stating plainly in an era where AI assistance is sometimes hidden or considered something to be embarrassed about. It isn't.

---

## Acknowledgements

- [AcoustID](https://acoustid.org/) and [Chromaprint](https://acoustid.org/chromaprint) for audio fingerprinting
- [Mutagen](https://mutagen.readthedocs.io/) for audio tag reading and writing
- [PySide6](https://doc.qt.io/qtforpython/) for the cross-platform UI framework
- [ffmpeg](https://ffmpeg.org/) for audio health checking
- [mp3val](http://mp3val.sourceforge.net/) for MP3 integrity validation
- [Claude](https://claude.ai) by Anthropic — AI pair programming throughout development

---

## License

DJ Library Manager is released under the [GNU General Public License v3.0](LICENSE).

You are free to use, modify, and distribute this software under the terms of the GPL v2. Any derivative works must also be released under the GPL v2. This software must remain open source.

---

*Built by [@quinn-stack](https://github.com/quinn-stack) — a working KDJ who wanted cleaner files before the gig.*
