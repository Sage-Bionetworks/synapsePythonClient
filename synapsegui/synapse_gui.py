"""
Example of how the refactored main GUI would look with bulk operations
"""
import tkinter as tk
from tkinter import ttk
from typing import TYPE_CHECKING

from .components.bulk_download_component import BulkDownloadComponent
from .components.bulk_upload_component import BulkUploadComponent
from .components.download_component import DownloadComponent
from .components.login_component import LoginComponent
from .components.output_component import OutputComponent
from .components.upload_component import UploadComponent
from .controllers.app_controller import ApplicationController
from .utils.logging_integration import LoggingIntegration

if TYPE_CHECKING:
    pass


class SynapseGUI:
    """Main GUI window"""

    def __init__(self, root: tk.Tk) -> None:
        """
        Initialize the Synapse GUI application.

        Args:
            root: The main tkinter window instance
        """
        self.root = root
        self.root.title("Synapse Desktop Client")
        self.root.geometry("800x700")

        self.controller = ApplicationController()
        self.create_widgets()

        self.controller.set_ui_components(
            login_component=self.login_component,
            download_component=self.download_component,
            upload_component=self.upload_component,
            output_component=self.output_component,
            root=self.root,
            bulk_download_component=self.bulk_download_component,
            bulk_upload_component=self.bulk_upload_component,
        )

        self.logging_integration = LoggingIntegration(self.output_component.log_message)
        self.logging_integration.setup_logging_integration(self.root)

    def create_widgets(self) -> None:
        """Create and configure the main window layout and components."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)

        title_label = ttk.Label(
            main_frame, text="Synapse Desktop Client", font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, pady=(0, 20))

        self.login_component = LoginComponent(
            main_frame,
            self.controller.config_manager,
            self.controller.handle_login,
            self.controller.handle_logout,
        )

        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(
            row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )

        self.download_component = DownloadComponent(
            self.notebook, self.controller.handle_download
        )
        self.notebook.add(self.download_component.get_frame(), text="Download File")

        self.upload_component = UploadComponent(
            self.notebook, self.controller.handle_upload
        )
        self.notebook.add(self.upload_component.get_frame(), text="Upload File")

        self.output_component = OutputComponent(main_frame)
        self.output_component.get_frame().grid(
            row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )

        self.bulk_download_component = BulkDownloadComponent(
            self.notebook,
            self.controller.handle_bulk_download,
            self.controller.handle_enumerate,
            self.output_component.log_message,
            self._on_progress_update,
        )
        self.notebook.add(
            self.bulk_download_component.create_ui(), text="Bulk Download"
        )

        self.bulk_upload_component = BulkUploadComponent(
            self.notebook,
            self.controller.handle_bulk_upload,
            self.output_component.log_message,
            self._on_progress_update,
        )
        self.notebook.add(self.bulk_upload_component.create_ui(), text="Bulk Upload")

        self.output_component.get_status_bar().grid(
            row=4, column=0, sticky=(tk.W, tk.E)
        )

    def _on_progress_update(self, message: str, progress: int) -> None:
        """
        Handle progress updates from bulk operations.

        Args:
            message: Progress message to display
            progress: Progress percentage (0-100)
        """

        def update_ui():
            if self.output_component:
                self.output_component.log_message(f"Progress: {progress}% - {message}")

        self.root.after(0.001, update_ui)

    def cleanup(self) -> None:
        """Clean up resources when closing the application."""
        if hasattr(self, "logging_integration"):
            self.logging_integration.cleanup_logging_integration()


def main() -> None:
    """Main entry point for the Synapse Desktop Client application."""
    root = tk.Tk()
    app = SynapseGUI(root)

    def on_closing() -> None:
        """Handle application closing event."""
        app.cleanup()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")

    try:
        root.mainloop()
    except KeyboardInterrupt:
        app.cleanup()


if __name__ == "__main__":
    main()
