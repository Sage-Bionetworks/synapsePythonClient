"""This module is responsible for exposing access to any configuration either through
file, environment variables, or other means.
"""

import configparser
import functools
import urllib.parse
from typing import Dict

from synapseclient.core.constants import config_file_constants
from synapseclient.core.pool_provider import DEFAULT_NUM_THREADS


@functools.lru_cache()
def get_config_file(config_path: str) -> configparser.RawConfigParser:
    """
    Retrieves the client configuration information.

    Arguments:
        config_path:  Path to configuration file on local file system

    Returns:
        A RawConfigParser populated with properties from the user's configuration file.
    """

    try:
        config = configparser.RawConfigParser()
        config.read(config_path)  # Does not fail if the file does not exist
        return config
    except configparser.Error as ex:
        raise ValueError(f"Error parsing Synapse config file: {config_path}") from ex


def get_config_section_dict(
    section_name: str,
    config_path: str,
) -> Dict[str, str]:
    """
    Get a profile section in the configuration file with the section name.

    Arguments:
        section_name: The name of the profile section in the configuration file
        config_path:  Path to configuration file on local file system

    Returns:
        A dictionary containing the configuration profile section content. If the
        section does not exist, an empty dictionary is returned.
    """
    config = get_config_file(config_path)
    try:
        return dict(config.items(section_name))
    except configparser.NoSectionError:
        # section not present
        return {}


def get_client_authenticated_s3_profile(
    endpoint: str,
    bucket: str,
    config_path: str,
) -> Dict[str, str]:
    """
    Get the authenticated S3 profile from the configuration file.

    Arguments:
        endpoint: The location of the target service
        bucket:   AWS S3 bucket name
        config_path:  Path to configuration file on local file system

    Returns:
        The authenticated S3 profile
    """

    config_section = urllib.parse.urljoin(base=endpoint, url=bucket)
    return get_config_section_dict(
        section_name=config_section, config_path=config_path
    ).get("profile_name", "default")


def get_config_authentication(
    config_path: str,
) -> Dict[str, str]:
    """
    Get the authentication section of the configuration file.

    Arguments:
        config_path:  Path to configuration file on local file system

    Returns:
        The authentication section of the configuration file
    """
    return get_config_section_dict(
        section_name=config_file_constants.AUTHENTICATION_SECTION_NAME,
        config_path=config_path,
    )


def get_transfer_config(
    config_path: str,
) -> Dict[str, str]:
    """
    Get the transfer profile from the configuration file.

    Arguments:
        config_path:  Path to configuration file on local file system

    Raises:
        ValueError: Invalid max_threads value. Should be equal or less than 16.
        ValueError: Invalid use_boto_sts value. Should be true or false.

    Returns:
        The transfer profile
    """
    # defaults
    transfer_config = {"max_threads": DEFAULT_NUM_THREADS, "use_boto_sts": False}

    for k, v in get_config_section_dict(
        section_name="transfer", config_path=config_path
    ).items():
        if v:
            if k == "max_threads":
                try:
                    transfer_config["max_threads"] = int(v)
                except ValueError as cause:
                    raise ValueError(
                        f"Invalid transfer.max_threads config setting {v}"
                    ) from cause

            elif k == "use_boto_sts":
                lower_v = v.lower()
                if lower_v not in ("true", "false"):
                    raise ValueError(
                        f"Invalid transfer.use_boto_sts config setting {v}"
                    )

                transfer_config["use_boto_sts"] = "true" == lower_v

    return transfer_config
