"""
Main application controller - orchestrates components
"""
import asyncio
import threading
from typing import Any, Dict

from ..models.config import ConfigManager
from ..models.synapse_client import SynapseClientManager
from ..utils.progress import ProgressManager


class ApplicationController:
    """Main application controller that coordinates between UI and business logic"""

    def __init__(self) -> None:
        self.synapse_client = SynapseClientManager()
        self.config_manager = ConfigManager()
        self.progress_manager = ProgressManager()

        # UI components will be set by the main window
        self.login_component = None
        self.download_component = None
        self.upload_component = None
        self.output_component = None

    def set_ui_components(
        self,
        login_component,
        download_component,
        upload_component,
        output_component,
        bulk_download_component=None,
        bulk_upload_component=None,
    ) -> None:
        """Set references to UI components"""
        self.login_component = login_component
        self.download_component = download_component
        self.upload_component = upload_component
        self.output_component = output_component
        self.bulk_download_component = bulk_download_component
        self.bulk_upload_component = bulk_upload_component

    def handle_login(self, mode: str, credentials: Dict[str, Any]) -> None:
        """Handle login request from UI"""

        def login_worker():
            if mode == "manual":
                result = asyncio.run(
                    self.synapse_client.login_manual(
                        credentials.get("username", ""), credentials.get("token", "")
                    )
                )
            else:  # config
                result = asyncio.run(
                    self.synapse_client.login_with_profile(
                        credentials.get("profile", "")
                    )
                )

            # Update UI based on result
            if result["success"]:
                self.login_component.update_login_state(
                    True, result.get("username", "")
                )
                self._enable_operations(True)
                self.output_component.log_message(
                    f"Login successful! Logged in as: {result.get('username', '')}"
                )
            else:
                self.login_component.update_login_state(False, error=result["error"])
                self.output_component.log_message(
                    f"Login failed: {result['error']}", error=True
                )
            
            # Re-enable login/logout button after login attempt completes
            self.login_component.set_login_button_state(True)

        # Disable login button during attempt
        self.login_component.set_login_button_state(False)

        # Run in background thread
        threading.Thread(target=login_worker, daemon=True).start()

    def handle_logout(self) -> None:
        """Handle logout request"""
        self.synapse_client.logout()
        self.login_component.update_login_state(False)
        self._enable_operations(False)
        self.output_component.log_message("Logged out successfully")
        # Ensure login button remains enabled after logout
        self.login_component.set_login_button_state(True)

    def handle_download(
        self, synapse_id: str, version: str, download_path: str
    ) -> None:
        """Handle file download request"""

        def download_worker():
            version_num = None
            if version:
                try:
                    version_num = int(version)
                except ValueError:
                    self.output_component.log_message(
                        "Version must be a number", error=True
                    )
                    return

            def progress_callback(progress: int, message: str):
                self.download_component.update_progress(progress, message)

            def detail_callback(detail_message: str):
                self.output_component.log_message(detail_message)

            result = asyncio.run(
                self.synapse_client.download_file(
                    synapse_id,
                    version_num,
                    download_path,
                    progress_callback,
                    detail_callback,
                )
            )

            if result["success"]:
                self.output_component.log_message(f"Downloaded: {result['path']}")
                self.download_component.show_success(f"Downloaded: {result['path']}")
            else:
                self.output_component.log_message(
                    f"Download failed: {result['error']}", error=True
                )
                self.download_component.show_error(result["error"])

        self.download_component.start_operation()
        threading.Thread(target=download_worker, daemon=True).start()

    def handle_upload(
        self, file_path: str, parent_id: str, entity_id: str, name: str
    ) -> None:
        """Handle file upload request"""

        def upload_worker():
            def progress_callback(progress: int, message: str):
                self.upload_component.update_progress(progress, message)

            def detail_callback(detail_message: str):
                self.output_component.log_message(detail_message)

            result = asyncio.run(
                self.synapse_client.upload_file(
                    file_path,
                    parent_id or None,
                    entity_id or None,
                    name or None,
                    progress_callback,
                    detail_callback,
                )
            )

            if result["success"]:
                message = (
                    f"Created/Updated entity: {result['entity_id']} - {result['name']}"
                )
                self.output_component.log_message(message)
                self.upload_component.show_success(message)
            else:
                self.output_component.log_message(
                    f"Upload failed: {result['error']}", error=True
                )
                self.upload_component.show_error(result["error"])

        self.upload_component.start_operation()
        threading.Thread(target=upload_worker, daemon=True).start()

    def handle_enumerate(self, container_id: str, recursive: bool) -> None:
        """Handle container enumeration request from UI"""

        def enumerate_worker():
            result = asyncio.run(
                self.synapse_client.enumerate_container(container_id, recursive)
            )

            # Schedule UI updates on the main thread using after()
            if self.bulk_download_component:
                if result["success"]:
                    self.bulk_download_component.parent.after(
                        0, 
                        lambda: self.bulk_download_component.handle_enumeration_result(result["items"])
                    )
                else:
                    self.bulk_download_component.parent.after(
                        0,
                        lambda: self.bulk_download_component.handle_enumeration_result([], result["error"])
                    )

        threading.Thread(target=enumerate_worker, daemon=True).start()

    def _enable_operations(self, enabled: bool) -> None:
        """Enable/disable operation buttons based on login state"""
        if self.download_component:
            self.download_component.set_enabled(enabled)
        if self.upload_component:
            self.upload_component.set_enabled(enabled)
        if self.bulk_download_component:
            pass
        if self.bulk_upload_component:
            pass

    def handle_bulk_download(
        self, items: list, download_path: str, recursive: bool
    ) -> None:
        """
        Handle bulk download request from UI.

        Args:
            items: List of BulkItem objects to download
            download_path: Local directory path where files will be downloaded
            recursive: Whether to download folder contents recursively.
                      If True, all subfolders and files within folders will be downloaded.
                      If False, only the immediate contents of folders will be downloaded.
        """

        def bulk_download_worker():
            # Start the operation
            if self.bulk_download_component:
                self.output_component.get_frame().after(0, lambda: self.bulk_download_component.start_bulk_operation())
                
            result = asyncio.run(
                self.synapse_client.bulk_download(
                    items,
                    download_path,
                    recursive,
                    self._on_bulk_download_progress_update,
                    self._on_detail_message,
                )
            )

            if result["success"]:
                self.output_component.log_message(
                    f"Bulk download complete: {result['summary']}"
                )
                if self.bulk_download_component:
                    self.output_component.get_frame().after(0, lambda: self.bulk_download_component.complete_bulk_operation(True, result['summary']))
            else:
                self.output_component.log_message(
                    f"Bulk download failed: {result['error']}", error=True
                )
                if self.bulk_download_component:
                    self.output_component.get_frame().after(0, lambda: self.bulk_download_component.complete_bulk_operation(False, result['error']))

        self.output_component.log_message(
            f"Starting bulk download of {len(items)} items..."
        )
        threading.Thread(target=bulk_download_worker, daemon=True).start()

    def handle_bulk_upload(
        self, items: list, parent_id: str, preserve_structure: bool
    ) -> None:
        """Handle bulk upload request from UI"""

        def bulk_upload_worker():
            # Start the operation
            if self.bulk_upload_component:
                self.output_component.get_frame().after(0, lambda: self.bulk_upload_component.start_bulk_operation())
                
            result = asyncio.run(
                self.synapse_client.bulk_upload(
                    items,
                    parent_id,
                    preserve_structure,
                    self._on_bulk_upload_progress_update,
                    self._on_detail_message,
                )
            )

            if result["success"]:
                self.output_component.log_message(
                    f"Bulk upload complete: {result['summary']}"
                )
                if self.bulk_upload_component:
                    self.output_component.get_frame().after(0, lambda: self.bulk_upload_component.complete_bulk_operation(True, result['summary']))
            else:
                self.output_component.log_message(
                    f"Bulk upload failed: {result['error']}", error=True
                )
                if self.bulk_upload_component:
                    self.output_component.get_frame().after(0, lambda: self.bulk_upload_component.complete_bulk_operation(False, result['error']))

        self.output_component.log_message(
            f"Starting bulk upload of {len(items)} items..."
        )
        threading.Thread(target=bulk_upload_worker, daemon=True).start()

    def _on_bulk_download_progress_update(self, progress: int, message: str) -> None:
        """Handle progress updates from bulk download operations"""
        # Schedule UI updates on the main thread
        if self.bulk_download_component:
            self.output_component.get_frame().after(0, lambda: self.bulk_download_component.update_progress(progress, message))
        if self.output_component:
            self.output_component.log_message(f"Download Progress: {progress}% - {message}")

    def _on_bulk_upload_progress_update(self, progress: int, message: str) -> None:
        """Handle progress updates from bulk upload operations"""
        # Schedule UI updates on the main thread
        if self.bulk_upload_component:
            self.output_component.get_frame().after(0, lambda: self.bulk_upload_component.update_progress(progress, message))
        if self.output_component:
            self.output_component.log_message(f"Upload Progress: {progress}% - {message}")

    def _on_progress_update(self, progress: int, message: str) -> None:
        """Handle progress updates from single file operations"""
        if self.output_component:
            self.output_component.log_message(f"Progress: {progress}% - {message}")

    def _on_detail_message(self, message: str) -> None:
        """Handle detailed progress messages from bulk operations"""
        if self.output_component:
            self.output_component.log_message(message)
