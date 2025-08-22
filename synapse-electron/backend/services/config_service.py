"""
Configuration service for Synapse Desktop Client.

This module provides the ConfigManager service class for managing
Synapse configuration profiles and authentication settings.
"""
import os
from typing import List

from synapseclient.api.configuration_services import get_config_file


class ConfigManager:
    """
    Manages Synapse configuration profiles.

    Handles reading and parsing Synapse configuration files to extract
    available authentication profiles and their associated information.
    """

    def __init__(self, config_path: str = None) -> None:
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the Synapse configuration file.
                        Defaults to ~/.synapseConfig if not provided.
        """
        self.config_path = config_path or os.path.expanduser("~/.synapseConfig")

    def get_available_profiles(self) -> List[str]:
        """
        Get list of available authentication profiles from configuration file.

        Returns:
            List of profile names found in the configuration file
        """
        profiles = []

        try:
            config = get_config_file(self.config_path)
            sections = config.sections()

            for section in sections:
                if section == "default":
                    profiles.append("default")
                elif section.startswith("profile "):
                    profile_name = section[8:]
                    profiles.append(profile_name)
                elif section == "authentication":
                    profiles.append("authentication (legacy)")

            if not profiles and os.path.exists(self.config_path):
                profiles.append("default")

        except Exception:
            pass

        return profiles

    def get_profile_info(self, profile_name: str) -> str:
        """
        Get username for a specific profile.

        Args:
            profile_name: Name of the profile to get information for

        Returns:
            Username associated with the profile, or empty string if not found
        """
        try:
            config = get_config_file(self.config_path)

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
        """
        Check if configuration file exists and contains profiles.

        Returns:
            True if config file exists and has profiles, False otherwise
        """
        return (
            os.path.exists(self.config_path) and len(self.get_available_profiles()) > 0
        )
