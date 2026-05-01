import json
from pathlib import Path


class ProfileManager:
    def __init__(self):
        # Create base config directory in user's home
        self.base_dir = Path.home() / ".dj_library_manager"
        self.profiles_dir = self.base_dir / "profiles"

        self.base_dir.mkdir(exist_ok=True)
        self.profiles_dir.mkdir(exist_ok=True)

    # =========================
    # Profile Listing
    # =========================
    def list_profiles(self):
        """
        Return profile names sorted alphabetically (case-insensitive).
        """
        profiles = [f.stem for f in self.profiles_dir.glob("*.json")]
        return sorted(profiles, key=lambda x: x.lower())

    # =========================
    # Load Profile
    # =========================
    def load_profile(self, name):
        path = self.profiles_dir / f"{name}.json"

        if not path.exists():
            return None

        with open(path, "r") as f:
            return json.load(f)

    # =========================
    # Save Profile
    # =========================
    def save_profile(self, profile_data):
        name = profile_data.get("profile_name")

        if not name:
            raise ValueError("Profile name cannot be empty.")

        path = self.profiles_dir / f"{name}.json"

        with open(path, "w") as f:
            json.dump(profile_data, f, indent=4)

    # =========================
    # Delete Profile
    # =========================
    def delete_profile(self, name):
        path = self.profiles_dir / f"{name}.json"

        if path.exists():
            path.unlink()
