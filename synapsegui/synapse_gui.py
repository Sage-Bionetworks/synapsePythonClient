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
        self.root = root
        self.root.title("Synapse Desktop Client")
        self.root.geometry("800x700")

        # Initialize controller
        self.controller = ApplicationController()

        # Create UI components
        self.create_widgets()

        # Connect components to controller
        self.controller.set_ui_components(
            self.login_component,
            self.download_component,
            self.upload_component,
            self.output_component,
            self.bulk_download_component,
            self.bulk_upload_component,
        )

        # Setup logging integration to forward console logs to GUI
        self.logging_integration = LoggingIntegration(self.output_component.log_message)
        self.logging_integration.setup_logging_integration(self.root)

    def create_widgets(self) -> None:
        """Create main window layout and components"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)

        # Title
        title_label = ttk.Label(
            main_frame, text="Synapse Desktop Client", font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, pady=(0, 20))

        # Login component
        self.login_component = LoginComponent(
            main_frame,
            self.controller.config_manager,
            self.controller.handle_login,
            self.controller.handle_logout,
        )

        # Operation tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(
            row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )

        # Download component
        self.download_component = DownloadComponent(
            self.notebook, self.controller.handle_download
        )
        self.notebook.add(self.download_component.get_frame(), text="Download File")

        # Upload component
        self.upload_component = UploadComponent(
            self.notebook, self.controller.handle_upload
        )
        self.notebook.add(self.upload_component.get_frame(), text="Upload File")

        # Output component
        self.output_component = OutputComponent(main_frame)
        self.output_component.get_frame().grid(
            row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )

        # Bulk Download component
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

        # Bulk Upload component
        self.bulk_upload_component = BulkUploadComponent(
            self.notebook,
            self.controller.handle_bulk_upload,
            self.output_component.log_message,
            self._on_progress_update,
        )
        self.notebook.add(self.bulk_upload_component.create_ui(), text="Bulk Upload")

        # Status bar
        self.output_component.get_status_bar().grid(
            row=4, column=0, sticky=(tk.W, tk.E)
        )

    def _on_progress_update(self, message: str, progress: int) -> None:
        """Handle progress updates from bulk operations"""
        # Note: parameters are swapped compared to controller callback
        # to match the component's expected signature
        if self.output_component:
            self.output_component.log_message(f"Progress: {progress}% - {message}")

    def cleanup(self) -> None:
        """Clean up resources when closing the application"""
        if hasattr(self, 'logging_integration'):
            self.logging_integration.cleanup_logging_integration()


def main() -> None:
    """Main function - much cleaner now"""
    root = tk.Tk()
    app = SynapseGUI(root)

    # Setup cleanup on window close
    def on_closing():
        app.cleanup()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    # Center the window
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
