"""
Synapse client operations and management.

This module provides the SynapseClientManager class and supporting utilities
for handling all Synapse client operations, including authentication, file
uploads/downloads, and bulk operations, separated from UI logic.
"""
import asyncio
import io
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import synapseclient
from synapseclient.core import utils
from synapseclient.models import File

DESKTOP_CLIENT_VERSION = "0.1.0"

# Set up logger for this module
logger = logging.getLogger(__name__)


def _safe_stderr_redirect(new_stderr: Any) -> tuple[Any, Any]:
    """
    Safely redirect stderr, handling the case where original stderr might be None.

    Args:
        new_stderr: New stderr object to redirect to

    Returns:
        Tuple of (original_stderr, safe_original_stderr) where safe_original_stderr
        is guaranteed to be a valid file-like object
    """
    original_stderr = sys.stderr
    safe_original_stderr = (
        original_stderr if original_stderr is not None else io.StringIO()
    )
    sys.stderr = new_stderr
    return original_stderr, safe_original_stderr


def _safe_stderr_restore(original_stderr: Any, safe_original_stderr: Any) -> None:
    """
    Safely restore stderr, ensuring we never set it to None.

    Args:
        original_stderr: The original stderr object
        safe_original_stderr: Fallback stderr object if original was None
    """
    if original_stderr is not None:
        sys.stderr = original_stderr
    else:
        sys.stderr = safe_original_stderr


class TQDMProgressCapture:
    """
    Capture TQDM progress updates for GUI display.

    Intercepts TQDM progress output and extracts progress information
    to provide callbacks for GUI progress bars and status updates.
    """

    def __init__(
        self,
        progress_callback: Optional[Callable[[int, str], None]],
        detail_callback: Optional[Callable[[str], None]],
    ) -> None:
        """
        Initialize the TQDM progress capture.

        Args:
            progress_callback: Function to call with progress updates (progress%, message) or None
            detail_callback: Function to call with detailed progress messages or None
        """
        self.progress_callback = progress_callback
        self.detail_callback = detail_callback
        self.last_progress = 0

    def write(self, s: str) -> None:
        """
        Capture TQDM output and extract progress information.

        Args:
            s: String output from TQDM to parse for progress information
        """
        if s and "\r" in s:
            # TQDM typically uses \r for progress updates
            progress_line = s.strip().replace("\r", "")
            if "%" in progress_line and (
                "B/s" in progress_line or "it/s" in progress_line
            ):
                try:
                    # Look for percentage in the format "XX%"
                    match = re.search(r"(\d+)%", progress_line)
                    if match:
                        progress = int(match.group(1))
                        if progress != self.last_progress:
                            self.last_progress = progress
                            if self.progress_callback:
                                self.progress_callback(
                                    progress, f"Progress: {progress}%"
                                )
                            if self.detail_callback:
                                self.detail_callback(progress_line)
                except Exception:
                    pass

    def flush(self) -> None:
        """Required for file-like object interface."""
        pass


class SynapseClientManager:
    """
    Handles all Synapse client operations.

    Manages authentication, file operations, and bulk operations for the
    Synapse platform, providing a clean interface between the GUI and
    the underlying synapseclient library.
    """

    def __init__(self) -> None:
        """Initialize the Synapse client manager with default state."""
        self.client: Optional[synapseclient.Synapse] = None
        self.is_logged_in = False
        self.username = ""

    async def login_manual(self, username: str, token: str) -> Dict[str, Any]:
        """
        Login with username and authentication token.

        Args:
            username: Synapse username or email address
            token: Personal access token for authentication

        Returns:
            Dictionary with 'success' boolean and either 'username' or 'error' key
        """
        try:
            self.client = synapseclient.Synapse(
                skip_checks=True,
                debug=True,
                user_agent=[f"synapsedesktopclient/{DESKTOP_CLIENT_VERSION}"],
                silent_progress_bars=True,
            )

            if username:
                self.client.login(email=username, authToken=token, silent=False)
            else:
                self.client.login(authToken=token, silent=False)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            logger = logging.getLogger("synapseclient")
            logger.info(f"Successfully logged in as {self.username}")

            return {"success": True, "username": self.username}
        except Exception as e:
            logger = logging.getLogger("synapseclient")
            logger.error(f"Login failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def login_with_profile(self, profile_name: str) -> Dict[str, Any]:
        """
        Login with configuration file profile.

        Args:
            profile_name: Name of the profile from Synapse configuration file

        Returns:
            Dictionary with 'success' boolean and either 'username' or 'error' key
        """
        try:
            self.client = synapseclient.Synapse(
                skip_checks=True,
                debug=True,
                user_agent=[f"synapsedesktopclient/{DESKTOP_CLIENT_VERSION}"],
                silent_progress_bars=True,
            )

            if profile_name == "authentication (legacy)":
                self.client.login(silent=False)
            else:
                self.client.login(profile=profile_name, silent=False)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            logger = logging.getLogger("synapseclient")
            logger.info(
                f"Successfully logged in as {self.username} using profile {profile_name}"
            )

            return {"success": True, "username": self.username}
        except Exception as e:
            logger = logging.getLogger("synapseclient")
            logger.error(f"Login with profile '{profile_name}' failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def logout(self) -> None:
        """Logout from Synapse and clear authentication state."""
        if self.client:
            self.client.logout()
            logger = logging.getLogger("synapseclient")
            logger.info(f"User {self.username} logged out")
        self.client = None
        self.is_logged_in = False
        self.username = ""

    async def download_file(
        self,
        synapse_id: str,
        version: Optional[int],
        download_path: str,
        progress_callback: Optional[Callable[[int, str], None]],
        detail_callback: Optional[Callable[[str], None]],
    ) -> Dict[str, Any]:
        """
        Download a file from Synapse.

        Args:
            synapse_id: Synapse entity ID to download
            version: Specific version to download (None for latest)
            download_path: Local directory path for download
            progress_callback: Function for progress updates (progress%, message)
            detail_callback: Function for detailed progress messages

        Returns:
            Dictionary with 'success' boolean and either 'path' or 'error' key
        """
        try:
            if not self.is_logged_in:
                return {"success": False, "error": "Not logged in"}

            logger = logging.getLogger("synapseclient")
            version_info = f" version {version}" if version else ""
            logger.info(
                f"Starting download of {synapse_id}{version_info} to {download_path}"
            )

            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)
            original_stderr, safe_original_stderr = _safe_stderr_redirect(
                progress_capture
            )

            try:
                file_obj = File(
                    id=synapse_id,
                    version_number=version,
                    path=download_path,
                    download_file=True,
                )

                file_obj = file_obj.get(synapse_client=self.client)

                if file_obj.path and os.path.exists(file_obj.path):
                    logger.info(
                        f"Successfully downloaded {synapse_id} to {file_obj.path}"
                    )
                    return {"success": True, "path": file_obj.path}
                else:
                    error_msg = f"No files associated with entity {synapse_id}"
                    logger.error(error_msg)
                    return {"success": False, "error": error_msg}
            finally:
                _safe_stderr_restore(original_stderr, safe_original_stderr)

        except Exception as e:
            logger = logging.getLogger("synapseclient")
            logger.error(f"Download failed for {synapse_id}: {str(e)}")
            return {"success": False, "error": str(e)}

    async def upload_file(
        self,
        file_path: str,
        parent_id: Optional[str],
        entity_id: Optional[str],
        name: Optional[str],
        progress_callback: Optional[Callable[[int, str], None]],
        detail_callback: Optional[Callable[[str], None]],
    ) -> Dict[str, Any]:
        """
        Upload a file to Synapse.

        Args:
            file_path: Local path to the file to upload
            parent_id: Parent entity ID for new files (required for new files)
            entity_id: Entity ID to update (for updating existing files)
            name: Name for the entity (optional, uses filename if not provided)
            progress_callback: Function for progress updates (progress%, message) or None
            detail_callback: Function for detailed progress messages or None

        Returns:
            Dictionary with 'success' boolean and either entity info or 'error' key
        """
        try:
            if not self.is_logged_in:
                return {"success": False, "error": "Not logged in"}

            if not os.path.exists(file_path):
                return {"success": False, "error": f"File does not exist: {file_path}"}

            logger = logging.getLogger("synapseclient")
            if entity_id:
                logger.info(
                    f"Starting upload of {file_path} to update entity {entity_id}"
                )
            else:
                logger.info(
                    f"Starting upload of {file_path} to create new entity in {parent_id}"
                )

            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)
            original_stderr, safe_original_stderr = _safe_stderr_redirect(
                progress_capture
            )

            try:
                if entity_id:
                    file_obj = File(
                        id=entity_id, path=file_path, name=name, download_file=False
                    )
                    file_obj = file_obj.get(synapse_client=self.client)
                    file_obj.path = file_path
                    if name:
                        file_obj.name = name
                else:
                    if not parent_id:
                        return {
                            "success": False,
                            "error": "Parent ID is required for new files",
                        }

                    file_obj = File(
                        path=file_path,
                        name=name or utils.guess_file_name(file_path),
                        parent_id=parent_id,
                    )

                file_obj = file_obj.store(synapse_client=self.client)

                logger.info(
                    f"Successfully uploaded {file_path} as entity {file_obj.id}: {file_obj.name}"
                )

                return {
                    "success": True,
                    "entity_id": file_obj.id,
                    "name": file_obj.name,
                }
            finally:
                _safe_stderr_restore(original_stderr, safe_original_stderr)

        except Exception as e:
            logger = logging.getLogger("synapseclient")
            logger.error(f"Upload failed for {file_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    async def enumerate_container(
        self, container_id: str, recursive: bool
    ) -> Dict[str, Any]:
        """
        Enumerate contents of a Synapse container (Project or Folder).

        Args:
            container_id: Synapse ID of the container to enumerate
            recursive: Whether to enumerate recursively

        Returns:
            Dictionary with success status and list of BulkItem objects
        """
        try:
            if not self.client:
                return {"success": False, "error": "Not logged in"}

            # Log enumeration start
            logger = logging.getLogger("synapseclient")
            recursive_info = " (recursive)" if recursive else ""
            logger.info(
                f"Starting enumeration of container {container_id}{recursive_info}"
            )

            # Debug: Log environment info
            logger.info(f"DEBUG: Current working directory: {os.getcwd()}")
            logger.info(f"DEBUG: SYNAPSE_CACHE env var: {os.environ.get('SYNAPSE_CACHE', 'Not set')}")
            logger.info(f"DEBUG: TMP env var: {os.environ.get('TMP', 'Not set')}")
            logger.info(f"DEBUG: TEMP env var: {os.environ.get('TEMP', 'Not set')}")

            # Import here to avoid circular imports
            from synapseclient.models import Folder

            logger.info("DEBUG: About to create Folder object")
            container = Folder(id=container_id)
            logger.info(f"DEBUG: Created Folder object with ID: {container_id}")

            # Debug: Check container attributes before sync
            logger.info(f"DEBUG: Container path attribute: {getattr(container, 'path', 'Not set')}")
            logger.info(f"DEBUG: Container cache dir: {getattr(container, 'cache_dir', 'Not set')}")

            logger.info("DEBUG: About to call sync_from_synapse_async")

            # Sync metadata only (download_file=False)
            # Note: Progress bars are automatically disabled in packaged environments
            await container.sync_from_synapse_async(
                download_file=False,
                recursive=recursive,
                include_types=["file", "folder"],
                synapse_client=self.client,
            )
            logger.info("DEBUG: sync_from_synapse_async completed successfully")

            logger.info("DEBUG: About to convert container to bulk items")
            items = self._convert_to_bulk_items(
                container=container, recursive=recursive
            )
            logger.info(f"DEBUG: Converted to {len(items)} bulk items")

            # Build hierarchical paths for all items
            logger.info("DEBUG: Building hierarchical paths for downloaded items")
            try:
                path_mapping = self._build_hierarchical_paths(items, container_id)
                logger.info(f"DEBUG: Built path mapping for {len(path_mapping)} items")
            except Exception as path_error:
                logger.error(f"DEBUG: Error building hierarchical paths: {path_error}")
                raise

            # Update items with correct hierarchical paths
            logger.info("DEBUG: About to update items with hierarchical paths")
            for i, item in enumerate(items):
                if item.synapse_id in path_mapping:
                    item.path = path_mapping[item.synapse_id]
                    logger.info(f"DEBUG: Set path for item {i+1}/{len(items)} - {item.name}: '{item.path}'")
                else:
                    logger.info(
                        f"DEBUG: No path mapping for item {i+1}/{len(items)} - "
                        f"{item.name} (ID: {item.synapse_id})"
                    )

            logger.info(
                f"Successfully enumerated {len(items)} items from container {container_id}"
            )

            return {"success": True, "items": items}

        except Exception as e:
            logger = logging.getLogger("synapseclient")
            logger.error(f"DEBUG: Exception type: {type(e).__name__}")
            logger.error(f"DEBUG: Exception args: {e.args}")
            logger.error(f"DEBUG: Exception details: {repr(e)}")
            logger.error(f"Enumeration failed for container {container_id}: {str(e)}")

            # Add stack trace for better debugging
            import traceback
            logger.error(f"DEBUG: Full traceback:\n{traceback.format_exc()}")

            return {"success": False, "error": str(e)}

    def _build_hierarchical_paths(self, items: list, root_container_id: str) -> dict:
        """
        Build hierarchical paths for items based on parent-child relationships.

        Args:
            items: List of BulkItem objects
            root_container_id: ID of the root container being enumerated

        Returns:
            Dictionary mapping synapse_id to hierarchical path
        """
        # Create a mapping of ID to item for quick lookups
        id_to_item = {item.synapse_id: item for item in items}

        # Add the root container to avoid issues
        id_to_item[root_container_id] = None

        # Function to recursively build path for an item
        def get_item_path(item_id: str, visited: set = None) -> str:
            if visited is None:
                visited = set()

            if item_id in visited:
                # Circular reference protection
                return ""

            if item_id == root_container_id or item_id not in id_to_item:
                return ""

            visited.add(item_id)
            item = id_to_item[item_id]

            if (
                item is None
                or item.parent_id is None
                or item.parent_id == root_container_id
            ):
                visited.remove(item_id)
                return item.name if item else ""

            parent_path = get_item_path(item.parent_id, visited)
            visited.remove(item_id)

            if parent_path:
                return f"{parent_path}/{item.name}"
            else:
                return item.name

        # Build paths for all items
        path_mapping = {}
        for item in items:
            if item.item_type.lower() == "file":
                # For files, we want the directory path (excluding the filename)
                parent_path = ""
                if item.parent_id and item.parent_id != root_container_id:
                    parent_path = get_item_path(item.parent_id)
                path_mapping[item.synapse_id] = parent_path
            else:
                # For folders, include the folder name in the path
                path_mapping[item.synapse_id] = get_item_path(item.synapse_id)

        return path_mapping

    def _convert_to_bulk_items(self, container: Any, recursive: bool) -> list:
        """
        Convert container contents to BulkItem objects.

        Args:
            container: Container object with populated files/folders
            recursive: Whether enumeration was recursive

        Returns:
            List of BulkItem objects
        """
        from ..models.bulk_item import BulkItem

        items = []

        if hasattr(container, "files"):
            for file in container.files:
                file_path = file.path if hasattr(file, "path") else None
                # Log file path information for debugging
                logger = logging.getLogger("synapseclient")
                logger.info(
                    f"DEBUG: Converting file {file.name} (ID: {file.id}) - path attribute: '{file_path}'"
                )

                items.append(
                    BulkItem(
                        synapse_id=file.id,
                        name=file.name,
                        item_type="File",
                        size=file.file_handle.content_size if file.file_handle else 0,
                        parent_id=file.parent_id,
                        path=file_path,
                    )
                )

        if hasattr(container, "folders"):
            for folder in container.folders:
                folder_path = folder.path if hasattr(folder, "path") else None
                # Log folder path information for debugging
                logger = logging.getLogger("synapseclient")
                logger.info(
                    f"DEBUG: Converting folder {folder.name} (ID: {folder.id}) - "
                    f"path attribute: '{folder_path}'"
                )

                items.append(
                    BulkItem(
                        synapse_id=folder.id,
                        name=folder.name,
                        item_type="Folder",
                        size=None,
                        parent_id=folder.parent_id,
                        path=folder_path,
                    )
                )

                if recursive:
                    items.extend(self._convert_to_bulk_items(folder, recursive))

        return items

    async def _safe_callback(self, callback, *args):
        """Safely call a callback function, handling both sync and async callbacks."""
        if callback is None:
            return
        try:
            # Check if the callback is a coroutine function (async)
            if asyncio.iscoroutinefunction(callback):
                await callback(*args)
            else:
                # Call synchronous callback
                callback(*args)
        except Exception as e:
            logger.warning(f"Callback error: {e}")

    async def bulk_download(
        self,
        items: list,
        download_path: str,
        recursive: bool,
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ) -> Dict[str, Any]:
        """
        Download multiple items from Synapse.

        Args:
            items: List of BulkItem objects to download
            download_path: Base directory for downloads
            recursive: Whether to download folders recursively
            progress_callback: Callback for progress updates
            detail_callback: Callback for detailed progress messages

        Returns:
            Dictionary with success status and results
        """
        try:
            if not self.client:
                return {"success": False, "error": "Not logged in"}

            results = []
            total_items = len(items)

            for i, item in enumerate(items):
                overall_progress = int((i / total_items) * 100)
                await self._safe_callback(
                    progress_callback,
                    overall_progress,
                    f"Processing item {i + 1} of {total_items}",
                )
                await self._safe_callback(
                    detail_callback, f"Downloading {item.name} ({item.synapse_id})"
                )

                try:
                    if item.item_type.lower() == "file":
                        # Log the item details for debugging
                        await self._safe_callback(
                            detail_callback,
                            f"DEBUG: Processing file {item.name} with path: '{item.path}', parent_id: {item.parent_id}",
                        )

                        # Determine the download path, considering the item's path within the container
                        item_download_path = download_path
                        if recursive and item.path and item.path.strip():
                            # Create subdirectory structure based on the item's hierarchical path
                            await self._safe_callback(
                                detail_callback,
                                f"DEBUG: Creating directory structure for path: '{item.path}'",
                            )
                            item_download_path = os.path.join(download_path, item.path)
                            # Ensure the directory exists
                            os.makedirs(item_download_path, exist_ok=True)

                        await self._safe_callback(
                            detail_callback,
                            f"DEBUG: Final download path for {item.name}: {item_download_path}",
                        )

                        result = await self.download_file(
                            synapse_id=item.synapse_id,
                            version=None,
                            download_path=item_download_path,
                            progress_callback=progress_callback,
                            detail_callback=detail_callback,
                        )
                        results.append({"item": item, "result": result})
                    elif item.item_type.lower() == "folder":
                        if True:
                            raise NotImplementedError(
                                "Folder download not implemented yet"
                            )
                        from synapseclient.models import Folder

                        folder = Folder(id=item.synapse_id)
                        await folder.sync_from_synapse_async(
                            path=os.path.join(download_path, item.name),
                            download_file=True,
                            recursive=recursive,
                            include_types=["file", "folder"],
                            synapse_client=self.client,
                        )

                        results.append(
                            {
                                "item": item,
                                "result": {
                                    "success": True,
                                    "message": "Folder downloaded",
                                },
                            }
                        )

                except Exception as e:
                    results.append(
                        {"item": item, "result": {"success": False, "error": str(e)}}
                    )

            await self._safe_callback(progress_callback, 100, "Bulk download complete")

            successes = sum(1 for r in results if r["result"].get("success", False))
            failures = total_items - successes

            return {
                "success": True,
                "results": results,
                "summary": f"Downloaded {successes} items, {failures} failed",
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    async def bulk_upload(
        self,
        items: list,
        parent_id: str,
        preserve_structure: bool,
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ) -> Dict[str, Any]:
        """
        Upload multiple items to Synapse.

        Args:
            items: List of BulkItem objects to upload
            parent_id: Parent folder ID in Synapse
            preserve_structure: Whether to preserve directory structure
            progress_callback: Callback for progress updates
            detail_callback: Callback for detailed progress messages

        Returns:
            Dictionary with success status and results
        """
        try:
            if not self.client:
                return {"success": False, "error": "Not logged in"}

            results = []

            # If preserving structure, we need to create folders first
            folder_mapping = {}
            if preserve_structure:
                folder_mapping = await self._create_folder_structure(
                    items=items,
                    base_parent_id=parent_id,
                    progress_callback=progress_callback,
                    detail_callback=detail_callback,
                )

            file_items = [item for item in items if item.item_type == "File"]

            for i, item in enumerate(file_items):
                overall_progress = int((i / len(file_items)) * 100)
                await self._safe_callback(
                    progress_callback,
                    overall_progress,
                    f"Uploading file {i + 1} of {len(file_items)}",
                )
                await self._safe_callback(detail_callback, f"Uploading {item.name}")

                try:
                    target_parent = parent_id
                    if preserve_structure and item.path:
                        # Find the appropriate parent folder for this file
                        item_dir = os.path.dirname(item.path)

                        normalized_item_dir = item_dir.replace("\\", "/")

                        if normalized_item_dir in folder_mapping:
                            target_parent = folder_mapping[normalized_item_dir]
                        else:
                            # Check if this file belongs to any created folder
                            # Find the deepest (most specific) folder that contains this file
                            best_match = ""
                            for folder_path, folder_id in folder_mapping.items():
                                if self._is_subpath(item.path, folder_path):
                                    if len(folder_path) > len(best_match):
                                        best_match = folder_path
                                        target_parent = folder_id

                    result = await self.upload_file(
                        file_path=item.path,
                        parent_id=target_parent,
                        entity_id=None,
                        name=item.name,
                        progress_callback=progress_callback,
                        detail_callback=detail_callback,
                    )

                    results.append({"item": item, "result": result})

                except Exception as e:
                    results.append(
                        {"item": item, "result": {"success": False, "error": str(e)}}
                    )

            await self._safe_callback(progress_callback, 100, "Bulk upload complete")

            successes = sum(1 for r in results if r["result"].get("success", False))
            failures = len(file_items) - successes

            return {
                "success": True,
                "results": results,
                "summary": f"Uploaded {successes} files, {failures} failed",
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _create_folder_structure(
        self,
        items: list,
        base_parent_id: str,
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ) -> Dict[str, str]:
        """
        Create folder structure in Synapse for bulk upload.

        Args:
            items: List of BulkItem objects
            base_parent_id: Base parent folder ID
            progress_callback: Callback for progress updates
            detail_callback: Callback for detailed progress messages

        Returns:
            Dictionary mapping local paths to Synapse folder IDs
        """
        from synapseclient.models import Folder

        folder_mapping = {}

        root_folders = []
        for item in items:
            if item.item_type == "Folder":
                is_root = True
                for other_item in items:
                    if (
                        other_item.item_type == "Folder"
                        and other_item.path != item.path
                        and self._is_subpath(item.path, other_item.path)
                    ):
                        is_root = False
                        break
                if is_root:
                    root_folders.append(item)

        dir_paths = set()

        # First, add the explicitly selected folders
        for root_folder in root_folders:
            dir_paths.add(root_folder.path)

        # Then, add subdirectories within selected folders only
        for item in items:
            if item.path and item.item_type == "File":
                dir_path = os.path.dirname(item.path)
                if dir_path:
                    # Only add directories that are within a selected root folder
                    for root_folder in root_folders:
                        if self._is_subpath(dir_path, root_folder.path):
                            # Add all directories between root folder and file's directory
                            current_path = Path(dir_path)
                            root_path = Path(root_folder.path)

                            # Build list of directories from root folder to file's directory
                            relative_parts = current_path.relative_to(root_path).parts
                            temp_path = root_path

                            for part in relative_parts:
                                temp_path = temp_path / part
                                dir_paths.add(str(temp_path))
                            break

        sorted_dirs = sorted(dir_paths, key=lambda x: len(Path(x).parts))

        await detail_callback("Creating folder structure...")

        for i, dir_path in enumerate(sorted_dirs):
            if i % 5 == 0:
                progress = int((i / len(sorted_dirs)) * 50)
                await progress_callback(
                    progress, f"Creating folders ({i}/{len(sorted_dirs)})"
                )

            path_obj = Path(dir_path)
            folder_name = path_obj.name

            parent_folder_id = base_parent_id

            # For root folders, parent is the base parent
            is_root_folder = any(
                root_folder.path == dir_path for root_folder in root_folders
            )

            if not is_root_folder:
                # Find the parent directory that should already be created
                parent_path = str(path_obj.parent)
                normalized_parent_path = parent_path.replace("\\", "/")
                if normalized_parent_path in folder_mapping:
                    parent_folder_id = folder_mapping[normalized_parent_path]
                else:
                    # Parent might be a root folder
                    for root_folder in root_folders:
                        if root_folder.path == parent_path:
                            parent_folder_id = folder_mapping[normalized_parent_path]
                            break

            try:
                folder = Folder(name=folder_name, parent_id=parent_folder_id)
                folder = folder.store(synapse_client=self.client)
                normalized_dir_path = dir_path.replace("\\", "/")
                folder_mapping[normalized_dir_path] = folder.id
                await detail_callback(
                    f"Created folder: {folder_name} ({folder.id}) from path {dir_path}"
                )
            except Exception as e:
                await detail_callback(f"Error creating folder {folder_name}: {str(e)}")

        return folder_mapping

    def _is_subpath(self, child_path: str, parent_path: str) -> bool:
        """
        Check if child_path is a subpath of parent_path.

        Args:
            child_path: The potential child path to check
            parent_path: The potential parent path

        Returns:
            True if child_path is a subpath of parent_path, False otherwise
        """
        try:
            Path(child_path).relative_to(Path(parent_path))
            return True
        except ValueError:
            return False
