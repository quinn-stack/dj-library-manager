import platform
import subprocess
import logging

# ── USED BY OTHER ENGINES ────────────────────────────────────────────────────
# PlatformAdapter is imported by:
#   - engine/validator.py        →  PlatformAdapter.get_path_limit()
#   - engine/settings_manager.py →  PlatformAdapter.get_path_limit()
#   - engine/transfer_engine.py  →  PlatformAdapter.get_path_limit(), get_os()
#   - ui/transfer_page.py        →  PlatformAdapter.get_removable_drives()
# If you change get_path_limit() (return value, signature, or the _PATH_LIMITS
# dict), update and test all callers listed above.
# If you change get_removable_drives() return type or dict keys, update
# transfer_page.py accordingly.
# apply_safe_mode() / apply_linux_safe_mode() are called from ui/main_window.py
# — if you change that signature, update main_window.py accordingly.
# ─────────────────────────────────────────────────────────────────────────────

class PlatformAdapter:
    """Handles OS-specific logic and system-level constraints."""

    # Path length limits per OS
    _PATH_LIMITS = {
        "Windows": 260,
        "Darwin":  1024,
        "Linux":   4096,
    }

    @staticmethod
    def get_os():
        return platform.system()

    @classmethod
    def get_path_limit(cls):
        """Return the safe path length limit for the current OS.

        | OS      | Limit | Notes                                                   |
        |---------|-------|---------------------------------------------------------|
        | Windows |   260 | Hard limit unless long-path registry key is enabled     |
        | macOS   |  1024 | HFS+/APFS limit                                         |
        | Linux   |  4096 | Practical ext4/XFS filesystem limit                     |

        Returns the value for the detected OS, defaulting to the most
        conservative (Windows) limit if the OS is unrecognised.
        """
        return cls._PATH_LIMITS.get(cls.get_os(), 260)

    @classmethod
    def apply_safe_mode(cls):
        """Raise file descriptor limits on Linux and macOS.

        On both Linux and Darwin, the default fd limit is far too low for
        Beets when processing large libraries:
          - macOS default: 256  (lower than Linux — ulimit raise is critical)
          - Linux default: 1024

        On Windows this is a no-op.

        Returns a human-readable status string for display in the UI.
        """
        os_name = cls.get_os()
        if os_name in ("Linux", "Darwin"):
            try:
                import resource
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                target = min(65536, hard)
                resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
                return f"SUCCESS: ulimit raised to {target} ({os_name})"
            except Exception as e:
                return f"WARNING: Could not raise ulimit via resource module: {e}"
        elif os_name == "Windows":
            return "INFO: Windows detected — ulimit not applicable."
        else:
            return f"INFO: Unknown OS '{os_name}' — skipping ulimit."

    # Keep old name as an alias so existing call-sites don't break.
    @classmethod
    def apply_linux_safe_mode(cls):
        """Deprecated alias for apply_safe_mode(). Use apply_safe_mode() instead."""
        return cls.apply_safe_mode()

    @classmethod
    def wrap_command(cls, cmd):
        """Wrap commands with OS-specific prefixes if needed.

        On Linux and macOS we prepend a ulimit shell command as a belt-and-
        braces fallback in case the resource-module call in apply_safe_mode()
        was not sufficient (e.g. running inside a restricted shell).
        """
        if cls.get_os() in ("Linux", "Darwin"):
            return f"ulimit -n 65536 && {cmd}"
        return cmd

    @classmethod
    def get_homebrew_prefix(cls):
        """Return the Homebrew prefix on macOS, or None on other platforms.

        Homebrew on Apple Silicon installs to /opt/homebrew; on Intel Macs it
        uses /usr/local. We call `brew --prefix` at runtime so we never
        hard-code the path.
        """
        if cls.get_os() != "Darwin":
            return None
        try:
            result = subprocess.run(
                ["brew", "--prefix"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        # Common fallbacks if brew is not on PATH yet
        import os
        for fallback in ("/opt/homebrew", "/usr/local"):
            if os.path.isdir(fallback):
                return fallback
        return None

    @classmethod
    def get_removable_drives(cls) -> list:
        """Return a list of dicts describing currently mounted removable drives.

        Each dict contains:
            {
                "label":      str   — display name (volume label or basename),
                "mountpoint": str   — full mount path,
                "device":     str   — device node or empty string if unavailable,
                "fstype":     str   — filesystem type (ntfs, vfat, exfat, hfsplus…),
                "size_bytes": int   — total capacity in bytes (0 if unavailable),
                "free_bytes": int   — free space in bytes (0 if unavailable),
            }

        Results are sorted most-recently-mounted first (mtime of mountpoint
        directory descending) so the most recently plugged-in drive appears
        at the top of the list. The UI should NOT auto-select from this list —
        it should display the list and wait for the user to choose.

        Minimum size floor: volumes under 64 MB are silently excluded — they
        are almost certainly not music drives (boot partitions, swap, etc.).

        Platform behaviour:
          Linux   — scans ONLY /media/<user>/* and /run/media/<user>/* (udisks2/
                    gvfs standard automount paths). No psutil fallback — that
                    pulled in system/internal drives on multi-drive setups.
                    If a drive doesn't appear here, use Browse.
          macOS   — scans /Volumes/* excluding boot volume, hidden entries
                    (dot-prefixed), and disk images (fseventsd, .dmg mounts).
          Windows — psutil disk_partitions filtered by 'removable' in opts.

        ⚠ KNOWN LIMITATION: Large external SSDs in USB enclosures often
          identify as 'fixed' drives on all OSes and will not appear here.
          Always offer a Browse fallback in any UI using this method.
        """
        os_name = cls.get_os()

        if os_name == "Linux":
            return cls._removable_linux()
        elif os_name == "Darwin":
            return cls._removable_macos()
        elif os_name == "Windows":
            return cls._removable_windows()
        return []

    # Minimum volume size to show in the drive list (64 MB).
    # Excludes boot partitions, swap stubs, and disk image mounts.
    _MIN_DRIVE_BYTES = 64 * 1024 * 1024

    @classmethod
    def _removable_linux(cls) -> list:
        """Linux drive detection — udisks2/gvfs automount paths only.

        Scans /media/<user>/* and /run/media/<user>/* exclusively. These are
        the standard automount destinations on Ubuntu, Pop!_OS, Fedora, and
        most other desktop distros when a user plugs in a USB drive.

        No psutil fallback is used. On multi-drive systems the psutil fallback
        previously pulled in internal partitions, NAS mounts, loop devices and
        other system infrastructure — none of which are valid DJ transfer
        destinations. The Browse button handles any edge-case drive that
        isn't automounted to the standard paths.
        """
        import os, getpass
        from pathlib import Path

        drives = []
        seen   = set()

        username = getpass.getuser()
        search_roots = [
            Path("/media") / username,
            Path("/run/media") / username,
            # Some older distros mount directly under /media without a
            # username subdirectory — include as a lower-priority fallback
            Path("/media"),
        ]

        for root in search_roots:
            if not root.exists():
                continue
            try:
                for entry in sorted(root.iterdir()):
                    mp = str(entry)
                    if not entry.is_dir() or mp in seen:
                        continue
                    # Skip the username directory itself when scanning /media
                    if entry.name == username:
                        continue
                    # Skip directories that aren't actual mountpoints — on some
                    # distros /media/<user>/ exists but is empty or contains
                    # stale entries from previous sessions that are no longer mounted
                    if not os.path.ismount(mp):
                        continue
                    seen.add(mp)
                    d = cls._build_drive_dict(
                        label      = entry.name,
                        mountpoint = mp,
                        device     = "",
                        fstype     = "",
                    )
                    if d["size_bytes"] < cls._MIN_DRIVE_BYTES:
                        continue
                    drives.append(d)
            except PermissionError:
                continue

        return cls._sort_drives(drives)

    @classmethod
    def _removable_macos(cls) -> list:
        """macOS drive detection — /Volumes/* excluding system and image mounts.

        Excludes:
          - The boot volume (identified via diskutil)
          - Hidden entries (dot-prefixed names)
          - Disk image mounts — .dmg files mount under /Volumes/ and would
            otherwise appear as drives (e.g. installer images, app bundles)
          - Volumes under the minimum size floor (64 MB)
        """
        from pathlib import Path
        import subprocess

        drives = []
        volumes = Path("/Volumes")
        if not volumes.exists():
            return drives

        # Known macOS system/image volume name patterns to exclude
        _MACOS_SYSTEM_NAMES = {
            "macintosh hd", "macintosh hd - data", "recovery",
            ".timemachine", "com.apple.timemachine.localsnapshots",
        }

        # Identify the boot volume mountpoint to exclude it
        boot_volume = "/"
        try:
            result = subprocess.run(
                ["diskutil", "info", "-plist", "/"],
                capture_output=True, text=True, timeout=5
            )
            if "MountPoint" in result.stdout:
                for line in result.stdout.splitlines():
                    line = line.strip()
                    if line.startswith("<string>") and line.endswith("</string>"):
                        candidate = line[8:-9]
                        if candidate.startswith("/Volumes/") or candidate == "/":
                            boot_volume = candidate
                            break
        except Exception:
            pass

        for entry in volumes.iterdir():
            if not entry.is_dir():
                continue
            if str(entry) == boot_volume:
                continue
            # Skip hidden and system-named volumes
            if entry.name.startswith("."):
                continue
            if entry.name.lower() in _MACOS_SYSTEM_NAMES:
                continue
            d = cls._build_drive_dict(
                label      = entry.name,
                mountpoint = str(entry),
                device     = "",
                fstype     = "",
            )
            if d["size_bytes"] < cls._MIN_DRIVE_BYTES:
                continue
            drives.append(d)

        return cls._sort_drives(drives)

    @classmethod
    def _removable_windows(cls) -> list:
        drives = []
        try:
            import psutil
            for part in psutil.disk_partitions(all=False):
                opts = (part.opts or "").lower()
                if "removable" in opts:
                    label = part.device.rstrip("\\")
                    drives.append(cls._build_drive_dict(
                        label      = label,
                        mountpoint = part.mountpoint,
                        device     = part.device or "",
                        fstype     = part.fstype or "",
                    ))
        except Exception:
            pass
        return cls._sort_drives(drives)

    @staticmethod
    def _build_drive_dict(label: str, mountpoint: str,
                          device: str, fstype: str) -> dict:
        """Build a drive info dict, filling in size/free via os.statvfs or psutil."""
        size_bytes = 0
        free_bytes = 0
        try:
            import shutil
            usage      = shutil.disk_usage(mountpoint)
            size_bytes = usage.total
            free_bytes = usage.free
        except Exception:
            pass
        return {
            "label":      label,
            "mountpoint": mountpoint,
            "device":     device,
            "fstype":     fstype,
            "size_bytes": size_bytes,
            "free_bytes": free_bytes,
        }

    @staticmethod
    def _sort_drives(drives: list) -> list:
        """Sort drives by mountpoint directory mtime descending (most recent first)."""
        import os

        def _mtime(d):
            try:
                return os.path.getmtime(d["mountpoint"])
            except Exception:
                return 0.0

        return sorted(drives, key=_mtime, reverse=True)

    @classmethod
    def check_macos_dependencies(cls):
        """Check for required external tools on macOS.

        Returns a dict of {tool_name: found_bool} for tools that the app
        depends on. Only meaningful on Darwin; returns an empty dict elsewhere.

        Expected install method: Homebrew
            brew install beets ffmpeg mp3val libmagic
        """
        if cls.get_os() != "Darwin":
            return {}

        import shutil
        tools = ["beet", "ffmpeg", "mp3val", "file"]
        return {tool: shutil.which(tool) is not None for tool in tools}
