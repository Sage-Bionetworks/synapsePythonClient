"""
System utilities for the Synapse Desktop Client backend.

This module provides utilities for environment setup, directory operations,
and system-specific functionality.
"""

import logging
import mimetypes
import os
import sys
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def setup_electron_environment() -> None:
    """
    Setup environment variables and directories when running from Electron.

    This fixes issues with temporary directories and cache paths in packaged apps.
    Detects if running in an Electron context and configures appropriate paths.
    """
    try:
        is_electron_context = _detect_electron_environment()

        if not is_electron_context:
            logger.info("Not in Electron environment, using default settings")
            return

        logger.info("Detected Electron environment, setting up proper directories")

        app_cache_dir = _get_app_cache_directory()
        _setup_cache_directories(app_cache_dir)
        _setup_temp_directories(app_cache_dir)
        _change_working_directory()

    except (OSError, IOError) as e:
        logger.warning("Failed to setup Electron environment: %s", e)


def _detect_electron_environment() -> bool:
    """
    Check if we're running from within Electron's environment.

    Returns:
        True if running in Electron context, False otherwise
    """
    return (
        "electron" in sys.executable.lower()
        or "resources" in os.getcwd()
        or os.path.exists(os.path.join(os.getcwd(), "..", "..", "app.asar"))
    )


def _get_app_cache_directory() -> str:
    """
    Get the application cache directory for the current platform.

    Returns:
        Path to the application cache directory
    """
    if os.name == "nt":  # Windows
        cache_base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
        if cache_base:
            return os.path.join(cache_base, "SynapseDesktopClient")
        else:
            return os.path.join(
                os.path.expanduser("~"),
                "AppData",
                "Local",
                "SynapseDesktopClient",
            )
    else:  # Unix-like systems
        cache_base = os.getenv("XDG_CACHE_HOME")
        if cache_base:
            return os.path.join(cache_base, "SynapseDesktopClient")
        else:
            return os.path.join(
                os.path.expanduser("~"), ".cache", "SynapseDesktopClient"
            )


def _setup_cache_directories(app_cache_dir: str) -> None:
    """
    Create and configure cache directories.

    Args:
        app_cache_dir: Base application cache directory
    """
    os.makedirs(app_cache_dir, exist_ok=True)

    synapse_cache = os.path.join(app_cache_dir, "synapse")
    os.makedirs(synapse_cache, exist_ok=True)
    os.environ["SYNAPSE_CACHE"] = synapse_cache

    logger.info("Set SYNAPSE_CACHE to: %s", synapse_cache)


def _setup_temp_directories(app_cache_dir: str) -> None:
    """
    Create and configure temporary directories.

    Args:
        app_cache_dir: Base application cache directory
    """
    temp_dir = os.path.join(app_cache_dir, "temp")
    os.makedirs(temp_dir, exist_ok=True)

    os.environ["TMPDIR"] = temp_dir
    os.environ["TMP"] = temp_dir
    os.environ["TEMP"] = temp_dir

    logger.info("Set temp directories to: %s", temp_dir)


def _change_working_directory() -> None:
    """Change working directory to user's home to avoid permission issues."""
    user_home = os.path.expanduser("~")
    os.chdir(user_home)
    logger.info("Changed working directory to: %s", user_home)


def get_home_and_downloads_directories() -> Dict[str, str]:
    """
    Get the user's home and downloads directory paths.

    Returns:
        Dictionary with 'home_directory' and 'downloads_directory' keys

    Raises:
        Exception: If directories cannot be accessed or created
    """
    home_dir = os.path.expanduser("~")
    downloads_dir = os.path.join(home_dir, "Downloads")

    # Verify the Downloads directory exists, create if it doesn't
    if not os.path.exists(downloads_dir):
        try:
            os.makedirs(downloads_dir, exist_ok=True)
            logger.info("Created Downloads directory: %s", downloads_dir)
        except (OSError, IOError) as e:
            logger.warning("Could not create Downloads directory: %s", e)
            downloads_dir = home_dir

    return {"home_directory": home_dir, "downloads_directory": downloads_dir}


def scan_directory_for_files(
    directory_path: str, recursive: bool = True
) -> Dict[str, Any]:
    """
    Scan a directory for files and folders with metadata.

    Args:
        directory_path: The directory path to scan
        recursive: Whether to scan subdirectories recursively

    Returns:
        Dictionary containing file list and summary information

    Raises:
        ValueError: If directory doesn't exist or isn't a directory
    """
    if not os.path.exists(directory_path):
        raise ValueError("Directory does not exist")

    if not os.path.isdir(directory_path):
        raise ValueError("Path is not a directory")

    logger.info("Scanning directory: %s", directory_path)

    files = []
    total_size = 0

    def scan_recursive(current_path: str, base_path: str) -> List[Dict[str, Any]]:
        """Recursively scan a directory."""
        nonlocal total_size
        items = []

        try:
            for item in os.listdir(current_path):
                item_path = os.path.join(current_path, item)
                relative_path = os.path.relpath(item_path, base_path)

                if os.path.isfile(item_path):
                    file_info = _get_file_info(
                        item, item_path, relative_path, current_path
                    )
                    if file_info:
                        items.append(file_info)
                        total_size += file_info["size"]

                elif os.path.isdir(item_path) and recursive:
                    folder_info = _get_folder_info(
                        item, item_path, relative_path, current_path
                    )
                    items.append(folder_info)

                    # Recursively scan subdirectories
                    sub_items = scan_recursive(item_path, base_path)
                    items.extend(sub_items)

        except (PermissionError, OSError) as e:
            logger.warning("Could not access directory %s: %s", current_path, e)

        return items

    files = scan_recursive(directory_path, directory_path)

    # Count files vs folders
    file_count = sum(1 for f in files if f["type"] == "file")
    folder_count = sum(1 for f in files if f["type"] == "folder")

    logger.info("Found %d files and %d folders", file_count, folder_count)

    return {
        "success": True,
        "files": files,
        "summary": {
            "total_items": len(files),
            "file_count": file_count,
            "folder_count": folder_count,
            "total_size": total_size,
        },
    }


def _get_file_info(
    item_name: str, item_path: str, relative_path: str, current_path: str
) -> Optional[Dict[str, Any]]:
    """
    Get file information for a single file.

    Args:
        item_name: Name of the file
        item_path: Full path to the file
        relative_path: Relative path from scan root
        current_path: Parent directory path

    Returns:
        File information dictionary or None if file cannot be accessed
    """
    try:
        file_size = os.path.getsize(item_path)
        mime_type, _ = mimetypes.guess_type(item_path)

        return {
            "id": item_path,
            "name": item_name,
            "type": "file",
            "size": file_size,
            "path": item_path,
            "relative_path": relative_path,
            "parent_path": current_path,
            "mime_type": mime_type or "application/octet-stream",
        }
    except (OSError, IOError) as e:
        logger.warning("Could not access file %s: %s", item_path, e)
        return None


def _get_folder_info(
    item_name: str, item_path: str, relative_path: str, current_path: str
) -> Dict[str, Any]:
    """
    Get folder information for a single folder.

    Args:
        item_name: Name of the folder
        item_path: Full path to the folder
        relative_path: Relative path from scan root
        current_path: Parent directory path

    Returns:
        Folder information dictionary
    """
    return {
        "id": item_path,
        "name": item_name,
        "type": "folder",
        "size": 0,
        "path": item_path,
        "relative_path": relative_path,
        "parent_path": current_path,
    }
