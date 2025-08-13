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
        self, login_component, download_component, upload_component, output_component
    ) -> None:
        """Set references to UI components"""
        self.login_component = login_component
        self.download_component = download_component
        self.upload_component = upload_component
        self.output_component = output_component

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

    def _enable_operations(self, enabled: bool) -> None:
        """Enable/disable operation buttons based on login state"""
        if self.download_component:
            self.download_component.set_enabled(enabled)
        if self.upload_component:
            self.upload_component.set_enabled(enabled)
