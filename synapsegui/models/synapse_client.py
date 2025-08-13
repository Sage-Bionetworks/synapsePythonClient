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
