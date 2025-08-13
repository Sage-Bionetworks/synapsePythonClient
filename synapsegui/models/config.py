"""
Configuration management for Synapse profiles
"""
import os
from typing import List

from synapseclient.api.configuration_services import get_config_file


class ConfigManager:
    """Manages Synapse configuration profiles"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.expanduser("~/.synapseConfig")

    def get_available_profiles(self) -> List[str]:
        """Get list of available authentication profiles from config file"""
        profiles = []

        try:
            config = get_config_file(self.config_path)
            sections = config.sections()

            # Look for profiles
            for section in sections:
                if section == "default":
                    profiles.append("default")
                elif section.startswith("profile "):
                    profile_name = section[8:]  # Remove "profile " prefix
                    profiles.append(profile_name)
                elif section == "authentication":
                    # Legacy authentication section
                    profiles.append("authentication (legacy)")

            # If no profiles found but config exists, add default
            if not profiles and os.path.exists(self.config_path):
                profiles.append("default")

        except Exception:
            # If config file doesn't exist or can't be read, return empty list
            pass

        return profiles

    def get_profile_info(self, profile_name: str) -> str:
        """Get username for a specific profile"""
        try:
            config = get_config_file(self.config_path)

            # Handle different profile name formats
            if profile_name == "default":
                section_name = "default"
            elif profile_name == "authentication (legacy)":
                section_name = "authentication"
            else:
                section_name = f"profile {profile_name}"

            if config.has_section(section_name):
                username = config.get(section_name, "username", fallback="")
                return username

        except Exception:
            pass

        return ""

    def has_config_file(self) -> bool:
        """Check if config file exists"""
        return (
            os.path.exists(self.config_path) and len(self.get_available_profiles()) > 0
        )
