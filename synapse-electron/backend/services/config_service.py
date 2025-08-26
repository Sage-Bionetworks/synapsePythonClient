"""
Configuration service for Synapse Desktop Client.

This module provides the ConfigManager service class for managing
Synapse configuration profiles and authentication settings.
"""
import os
from typing import List, Optional

from synapseclient.api.configuration_services import get_config_file


class ConfigManager:
    """
    Manages Synapse configuration profiles.

    Handles reading and parsing Synapse configuration files to extract
    available authentication profiles and their associated information.
    Provides methods to access profile data and validate configuration files.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        """
        Initialize the configuration manager.

        Sets up the configuration manager with the specified or default
        Synapse configuration file path.

        Arguments:
            config_path: Path to the Synapse configuration file.
                        Defaults to ~/.synapseConfig if not provided.

        Returns:
            None

        Raises:
            None: Initialization does not perform file operations that could fail.
        """
        self.config_path = config_path or os.path.expanduser("~/.synapseConfig")

    def get_available_profiles(self) -> List[str]:
        """
        Get list of available authentication profiles from configuration file.

        Parses the Synapse configuration file to extract all available
        authentication profiles including default, named profiles, and legacy.

        Arguments:
            None

        Returns:
            List[str]: List of profile names found in the configuration file.
                      Returns empty list if no profiles found or file cannot be read.

        Raises:
            Exception: Catches and handles configuration file parsing errors,
                      returning empty list on failure.
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

        Retrieves the username associated with the specified profile from
        the configuration file.

        Arguments:
            profile_name: Name of the profile to get information for

        Returns:
            str: Username associated with the profile, or empty string if not found

        Raises:
            Exception: Catches and handles configuration file access errors,
                      returning empty string on failure.
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

        Validates that the configuration file exists on the filesystem and
        contains at least one authentication profile.

        Arguments:
            None

        Returns:
            bool: True if config file exists and has profiles, False otherwise

        Raises:
            None: This method handles all exceptions internally.
        """
        return (
            os.path.exists(self.config_path) and len(self.get_available_profiles()) > 0
        )
