"""
Synapse client operations - separated from UI logic
"""
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


def _safe_stderr_redirect(new_stderr):
    """
    Safely redirect stderr, handling the case where original stderr might be None.
    
    Returns:
        Tuple of (original_stderr, safe_original_stderr) where safe_original_stderr
        is guaranteed to be a valid file-like object.
    """
    original_stderr = sys.stderr
    safe_original_stderr = original_stderr if original_stderr is not None else io.StringIO()
    sys.stderr = new_stderr
    return original_stderr, safe_original_stderr


def _safe_stderr_restore(original_stderr, safe_original_stderr):
    """
    Safely restore stderr, ensuring we never set it to None.
    """
    if original_stderr is not None:
        sys.stderr = original_stderr
    else:
        # If original was None, use a StringIO to avoid None issues
        sys.stderr = safe_original_stderr


class TQDMProgressCapture:
    """Capture TQDM progress updates for GUI display"""

    def __init__(
        self,
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ):
        self.progress_callback = progress_callback
        self.detail_callback = detail_callback
        self.last_progress = 0

    def write(self, s: str) -> None:
        """Capture TQDM output and extract progress information"""
        if s and "\r" in s:
            # TQDM typically uses \r for progress updates
            progress_line = s.strip().replace("\r", "")
            if "%" in progress_line and (
                "B/s" in progress_line or "it/s" in progress_line
            ):
                # Parse progress percentage
                try:
                    # Look for percentage in the format "XX%"
                    match = re.search(r"(\d+)%", progress_line)
                    if match:
                        progress = int(match.group(1))
                        if progress != self.last_progress:
                            self.last_progress = progress
                            # Send progress update for progress bar
                            self.progress_callback(progress, f"Progress: {progress}%")
                            # Send detailed progress line for output logging
                            self.detail_callback(progress_line)
                except Exception:
                    pass

    def flush(self) -> None:
        """Required for file-like object interface"""
        pass


class SynapseClientManager:
    """Handles all Synapse client operations"""

    def __init__(self):
        self.client: Optional[synapseclient.Synapse] = None
        self.is_logged_in = False
        self.username = ""

    async def login_manual(self, username: str, token: str) -> Dict[str, Any]:
        """Login with username and token"""
        try:
            # Create client with debug logging to capture detailed messages
            self.client = synapseclient.Synapse(skip_checks=True, debug=False)

            if username:
                self.client.login(email=username, authToken=token, silent=False)
            else:
                self.client.login(authToken=token, silent=False)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            # Log successful login
            logger = logging.getLogger('synapseclient')
            logger.info(f"Successfully logged in as {self.username}")

            return {"success": True, "username": self.username}
        except Exception as e:
            # Log login error
            logger = logging.getLogger('synapseclient')
            logger.error(f"Login failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def login_with_profile(self, profile_name: str) -> Dict[str, Any]:
        """Login with config file profile"""
        try:
            # Create client with debug logging to capture detailed messages
            self.client = synapseclient.Synapse(skip_checks=True, debug=False)

            if profile_name == "authentication (legacy)":
                self.client.login(silent=False)
            else:
                self.client.login(profile=profile_name, silent=False)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            # Log successful login
            logger = logging.getLogger('synapseclient')
            logger.info(f"Successfully logged in as {self.username} using profile {profile_name}")

            return {"success": True, "username": self.username}
        except Exception as e:
            # Log login error
            logger = logging.getLogger('synapseclient')
            logger.error(f"Login with profile '{profile_name}' failed: {str(e)}")
            return {"success": False, "error": str(e)}

    def logout(self) -> None:
        """Logout from Synapse"""
        if self.client:
            self.client.logout()
            # Log logout
            logger = logging.getLogger('synapseclient')
            logger.info(f"User {self.username} logged out")
        self.client = None
        self.is_logged_in = False
        self.username = ""

    async def download_file(
        self,
        synapse_id: str,
        version: Optional[int],
        download_path: str,
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ) -> Dict[str, Any]:
        """Download file from Synapse"""
        try:
            if not self.is_logged_in:
                return {"success": False, "error": "Not logged in"}

            # Log download start
            logger = logging.getLogger('synapseclient')
            version_info = f" version {version}" if version else ""
            logger.info(f"Starting download of {synapse_id}{version_info} to {download_path}")

            # Create progress capture for TQDM output
            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)

            # Safely redirect stderr to capture TQDM output
            original_stderr, safe_original_stderr = _safe_stderr_redirect(progress_capture)

            try:
                file_obj = File(
                    id=synapse_id,
                    version_number=version,
                    path=download_path,
                    download_file=True,
                )

                file_obj = file_obj.get(synapse_client=self.client)

                if file_obj.path and os.path.exists(file_obj.path):
                    logger.info(f"Successfully downloaded {synapse_id} to {file_obj.path}")
                    return {"success": True, "path": file_obj.path}
                else:
                    error_msg = f"No files associated with entity {synapse_id}"
                    logger.error(error_msg)
                    return {"success": False, "error": error_msg}
            finally:
                # Safely restore original stderr
                _safe_stderr_restore(original_stderr, safe_original_stderr)

        except Exception as e:
            logger = logging.getLogger('synapseclient')
            logger.error(f"Download failed for {synapse_id}: {str(e)}")
            return {"success": False, "error": str(e)}

    async def upload_file(
        self,
        file_path: str,
        parent_id: Optional[str],
        entity_id: Optional[str],
        name: Optional[str],
        progress_callback: Callable[[int, str], None],
        detail_callback: Callable[[str], None],
    ) -> Dict[str, Any]:
        """Upload file to Synapse"""
        try:
            if not self.is_logged_in:
                return {"success": False, "error": "Not logged in"}

            if not os.path.exists(file_path):
                return {"success": False, "error": f"File does not exist: {file_path}"}

            # Log upload start
            logger = logging.getLogger('synapseclient')
            if entity_id:
                logger.info(f"Starting upload of {file_path} to update entity {entity_id}")
            else:
                logger.info(f"Starting upload of {file_path} to create new entity in {parent_id}")

            # Create progress capture for TQDM output
            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)

            # Safely redirect stderr to capture TQDM output
            original_stderr, safe_original_stderr = _safe_stderr_redirect(progress_capture)

            try:
                if entity_id:  # Update existing
                    file_obj = File(
                        id=entity_id, path=file_path, name=name, download_file=False
                    )
                    file_obj = file_obj.get(synapse_client=self.client)
                    file_obj.path = file_path
                    if name:
                        file_obj.name = name
                else:  # Create new
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
                
                # Log successful upload
                logger.info(f"Successfully uploaded {file_path} as entity {file_obj.id}: {file_obj.name}")
                
                return {
                    "success": True,
                    "entity_id": file_obj.id,
                    "name": file_obj.name,
                }
            finally:
                # Safely restore original stderr
                _safe_stderr_restore(original_stderr, safe_original_stderr)

        except Exception as e:
            logger = logging.getLogger('synapseclient')
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
            logger = logging.getLogger('synapseclient')
            recursive_info = " (recursive)" if recursive else ""
            logger.info(f"Starting enumeration of container {container_id}{recursive_info}")

            # Import here to avoid circular imports
            from synapseclient.models import Folder, Project

            # Determine container type and create appropriate object
            # TODO: This needs to be fixed as this is not correct
            if container_id.startswith("project"):
                container = Project(id=container_id)
            else:
                container = Folder(id=container_id)

            # Sync metadata only (download_file=False)
            await container.sync_from_synapse_async(
                download_file=False, recursive=recursive, synapse_client=self.client
            )

            # Convert to BulkItem objects
            items = self._convert_to_bulk_items(container, recursive)

            # Log successful enumeration
            logger.info(f"Successfully enumerated {len(items)} items from container {container_id}")

            return {"success": True, "items": items}

        except Exception as e:
            logger = logging.getLogger('synapseclient')
            logger.error(f"Enumeration failed for container {container_id}: {str(e)}")
            return {"success": False, "error": str(e)}

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

        # Add files
        if hasattr(container, "files"):
            for file in container.files:
                items.append(
                    BulkItem(
                        synapse_id=file.id,
                        name=file.name,
                        item_type="File",
                        size=file.file_handle.content_size if file.file_handle else 0,
                        parent_id=file.parent_id,
                        path=file.path if hasattr(file, "path") else None,
                    )
                )

        # Add folders
        if hasattr(container, "folders"):
            for folder in container.folders:
                items.append(
                    BulkItem(
                        synapse_id=folder.id,
                        name=folder.name,
                        item_type="Folder",
                        size=None,
                        parent_id=folder.parent_id,
                        path=folder.path if hasattr(folder, "path") else None,
                    )
                )

                # Recursively add folder contents if recursive
                if recursive:
                    items.extend(self._convert_to_bulk_items(folder, recursive))

        return items

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
                # Update overall progress
                overall_progress = int((i / total_items) * 100)
                progress_callback(
                    overall_progress, f"Processing item {i + 1} of {total_items}"
                )
                detail_callback(f"Downloading {item.name} ({item.synapse_id})")

                try:
                    if item.item_type == "File":
                        # Download individual file
                        result = await self.download_file(
                            synapse_id=item.synapse_id,
                            version=None,
                            download_path=download_path,
                            progress_callback=progress_callback,
                            detail_callback=detail_callback,
                        )
                        results.append({"item": item, "result": result})
                    elif item.item_type == "Folder":
                        # Use sync_from_synapse_async for folders
                        from synapseclient.models import Folder

                        folder = Folder(id=item.synapse_id)
                        await folder.sync_from_synapse_async(
                            path=os.path.join(download_path, item.name),
                            download_file=True,
                            recursive=recursive,
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
                    else:
                        # Skip non-downloadable items
                        results.append(
                            {
                                "item": item,
                                "result": {
                                    "success": False,
                                    "error": f"Cannot download {item.item_type}",
                                },
                            }
                        )

                except Exception as e:
                    results.append(
                        {"item": item, "result": {"success": False, "error": str(e)}}
                    )

            # Final progress
            progress_callback(100, "Bulk download complete")

            # Count successes and failures
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
                    items, parent_id, progress_callback, detail_callback
                )

            # Upload files
            file_items = [item for item in items if item.item_type == "File"]

            for i, item in enumerate(file_items):
                # Update overall progress
                overall_progress = int((i / len(file_items)) * 100)
                progress_callback(
                    overall_progress, f"Uploading file {i + 1} of {len(file_items)}"
                )
                detail_callback(f"Uploading {item.name}")

                try:
                    # Determine target parent
                    target_parent = parent_id
                    if preserve_structure and item.path:
                        # Find the appropriate parent folder for this file
                        item_dir = os.path.dirname(item.path)
                        
                        # Normalize path separators to match folder_mapping keys
                        normalized_item_dir = item_dir.replace('\\', '/')
                        
                        # First check for exact match
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

            # Final progress
            progress_callback(100, "Bulk upload complete")

            # Count successes and failures
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

        # Find root folders (explicitly selected folders)
        root_folders = []
        for item in items:
            if item.item_type == "Folder":
                # Check if this folder is not a subfolder of another folder in the list
                is_root = True
                for other_item in items:
                    if (other_item.item_type == "Folder" and 
                        other_item.path != item.path and 
                        self._is_subpath(item.path, other_item.path)):
                        is_root = False
                        break
                if is_root:
                    root_folders.append(item)

        # Get all unique directory paths that need to be created
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

        # Sort by depth (create parents first)
        sorted_dirs = sorted(dir_paths, key=lambda x: len(Path(x).parts))

        detail_callback("Creating folder structure...")

        for i, dir_path in enumerate(sorted_dirs):
            if i % 5 == 0:  # Update progress periodically
                progress = int(
                    (i / len(sorted_dirs)) * 50
                )  # Use 50% for folder creation
                progress_callback(
                    progress, f"Creating folders ({i}/{len(sorted_dirs)})"
                )

            path_obj = Path(dir_path)
            folder_name = path_obj.name

            # Determine parent folder ID
            parent_folder_id = base_parent_id
            
            # For root folders, parent is the base parent
            is_root_folder = any(root_folder.path == dir_path for root_folder in root_folders)
            
            if not is_root_folder:
                # Find the parent directory that should already be created
                parent_path = str(path_obj.parent)
                normalized_parent_path = parent_path.replace('\\', '/')
                if normalized_parent_path in folder_mapping:
                    parent_folder_id = folder_mapping[normalized_parent_path]
                else:
                    # Parent might be a root folder
                    for root_folder in root_folders:
                        if root_folder.path == parent_path:
                            parent_folder_id = folder_mapping[normalized_parent_path]
                            break

            # Create folder
            try:
                folder = Folder(name=folder_name, parent_id=parent_folder_id)
                folder = folder.store(synapse_client=self.client)
                # Normalize path separators for consistent lookup
                normalized_dir_path = dir_path.replace('\\', '/')
                folder_mapping[normalized_dir_path] = folder.id
                detail_callback(f"Created folder: {folder_name} ({folder.id}) from path {dir_path}")
            except Exception as e:
                detail_callback(f"Error creating folder {folder_name}: {str(e)}")

        return folder_mapping

    def _is_subpath(self, child_path: str, parent_path: str) -> bool:
        """Check if child_path is a subpath of parent_path."""
        try:
            Path(child_path).relative_to(Path(parent_path))
            return True
        except ValueError:
            return False
