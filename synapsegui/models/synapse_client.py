"""
Synapse client operations - separated from UI logic
"""
import os
import re
import sys
from typing import Any, Callable, Dict, Optional

import synapseclient
from synapseclient.core import utils
from synapseclient.models import File


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
            self.client = synapseclient.Synapse(skip_checks=True)

            if username:
                self.client.login(email=username, authToken=token, silent=True)
            else:
                self.client.login(authToken=token, silent=True)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            return {"success": True, "username": self.username}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def login_with_profile(self, profile_name: str) -> Dict[str, Any]:
        """Login with config file profile"""
        try:
            self.client = synapseclient.Synapse(skip_checks=True)

            if profile_name == "authentication (legacy)":
                self.client.login(silent=True)
            else:
                self.client.login(profile=profile_name, silent=True)

            self.is_logged_in = True
            self.username = getattr(self.client, "username", None) or getattr(
                self.client, "email", "Unknown User"
            )

            return {"success": True, "username": self.username}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def logout(self) -> None:
        """Logout from Synapse"""
        if self.client:
            self.client.logout()
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

            # Create progress capture for TQDM output
            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)

            # Redirect stderr to capture TQDM output
            original_stderr = sys.stderr
            sys.stderr = progress_capture

            try:
                file_obj = File(
                    id=synapse_id,
                    version_number=version,
                    path=download_path,
                    download_file=True,
                )

                file_obj = file_obj.get(synapse_client=self.client)

                if file_obj.path and os.path.exists(file_obj.path):
                    return {"success": True, "path": file_obj.path}
                else:
                    return {
                        "success": False,
                        "error": f"No files associated with entity {synapse_id}",
                    }
            finally:
                # Restore original stderr
                sys.stderr = original_stderr

        except Exception as e:
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

            # Create progress capture for TQDM output
            progress_capture = TQDMProgressCapture(progress_callback, detail_callback)

            # Redirect stderr to capture TQDM output
            original_stderr = sys.stderr
            sys.stderr = progress_capture

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
                return {
                    "success": True,
                    "entity_id": file_obj.id,
                    "name": file_obj.name,
                }
            finally:
                # Restore original stderr
                sys.stderr = original_stderr

        except Exception as e:
            return {"success": False, "error": str(e)}

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
                        # Find the appropriate parent folder
                        item_dir = os.path.dirname(item.path)
                        if item_dir in folder_mapping:
                            target_parent = folder_mapping[item_dir]

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
        from pathlib import Path

        from synapseclient.models import Folder

        folder_mapping = {}

        # Get all unique directory paths
        dir_paths = set()
        for item in items:
            if item.path and item.item_type == "File":
                dir_path = os.path.dirname(item.path)
                if dir_path:
                    # Add all parent directories
                    path_obj = Path(dir_path)
                    for parent in [path_obj] + list(path_obj.parents):
                        if str(parent) != ".":
                            dir_paths.add(str(parent))

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
            parent_path = str(path_obj.parent)
            if parent_path != "." and parent_path in folder_mapping:
                parent_folder_id = folder_mapping[parent_path]

            # Create folder
            try:
                folder = Folder(name=folder_name, parent_id=parent_folder_id)
                folder = folder.store(synapse_client=self.client)
                folder_mapping[dir_path] = folder.id
                detail_callback(f"Created folder: {folder_name} ({folder.id})")
            except Exception as e:
                detail_callback(f"Error creating folder {folder_name}: {str(e)}")

        return folder_mapping
