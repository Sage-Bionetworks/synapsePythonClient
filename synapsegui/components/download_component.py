"""
Download component UI - separated from main window
"""
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional


class DownloadComponent:
    """
    Download file UI component.

    Provides functionality for downloading individual files from Synapse,
    including optional version specification and custom download location.
    """

    def __init__(
        self, parent: tk.Widget, on_download_callback: Callable[[str, str, str], None]
    ) -> None:
        """
        Initialize the download component.

        Args:
            parent: Parent widget to contain this component
            on_download_callback: Callback function for download operations
        """
        self.parent = parent
        self.on_download = on_download_callback

        self.download_id_var = tk.StringVar()
        self.download_version_var = tk.StringVar()
        self.download_location_var = tk.StringVar(value=str(Path.home() / "Downloads"))
        self.download_progress_var = tk.StringVar(value="")

        self.download_button: Optional[ttk.Button] = None
        self.download_progress_bar: Optional[ttk.Progressbar] = None
        self.frame: Optional[ttk.Frame] = None

        self.create_ui()

    def create_ui(self) -> None:
        """Create and configure the download UI components."""
        self.frame = ttk.Frame(self.parent, padding="10")
        self.frame.columnconfigure(1, weight=1)

        ttk.Label(self.frame, text="Synapse ID:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )
        download_id_entry = ttk.Entry(
            self.frame, textvariable=self.download_id_var, width=40
        )
        download_id_entry.grid(
            row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        ttk.Label(self.frame, text="Version (optional):").grid(
            row=1, column=0, sticky=tk.W, pady=(0, 5)
        )
        download_version_entry = ttk.Entry(
            self.frame, textvariable=self.download_version_var, width=40
        )
        download_version_entry.grid(
            row=1, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        ttk.Label(self.frame, text="Download Location:").grid(
            row=2, column=0, sticky=tk.W, pady=(0, 5)
        )

        location_frame = ttk.Frame(self.frame)
        location_frame.grid(
            row=2, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )
        location_frame.columnconfigure(0, weight=1)

        download_location_entry = ttk.Entry(
            location_frame, textvariable=self.download_location_var
        )
        download_location_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        browse_button = ttk.Button(
            location_frame, text="Browse", command=self.browse_download_location
        )
        browse_button.grid(row=0, column=1)

        self.download_button = ttk.Button(
            self.frame,
            text="Download File",
            command=self._handle_download,
            state="disabled",
        )
        self.download_button.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        download_progress_label = ttk.Label(
            self.frame,
            textvariable=self.download_progress_var,
            foreground="blue",
            font=("Arial", 8),
        )
        download_progress_label.grid(row=4, column=0, columnspan=2, pady=(5, 0))

        self.download_progress_bar = ttk.Progressbar(self.frame, mode="determinate")
        self.download_progress_bar.grid(
            row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0)
        )

    def browse_download_location(self) -> None:
        """Open directory browser to select download location."""
        directory = filedialog.askdirectory(initialdir=self.download_location_var.get())
        if directory:
            self.download_location_var.set(directory)

    def _handle_download(self) -> None:
        """Handle download button click and validate input parameters."""
        synapse_id = self.download_id_var.get().strip()
        version = self.download_version_var.get().strip()
        download_path = self.download_location_var.get().strip()

        if not synapse_id:
            messagebox.showerror("Error", "Synapse ID is required")
            return

        self.on_download(synapse_id, version, download_path)

    def set_enabled(self, enabled: bool) -> None:
        """
        Enable or disable download functionality.

        Args:
            enabled: True to enable download operations, False to disable
        """
        state = "normal" if enabled else "disabled"
        if self.download_button:
            self.download_button.config(state=state)

    def start_operation(self) -> None:
        """Initialize UI state when download operation begins."""
        self.download_progress_var.set("Preparing download...")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 0

    def update_progress(self, progress: int, message: str) -> None:
        """
        Update progress bar and status message.

        Args:
            progress: Progress percentage (0-100)
            message: Status message to display
        """
        if self.download_progress_bar:
            self.download_progress_bar["value"] = progress
        self.download_progress_var.set(message)

    def show_success(self, message: str) -> None:
        """
        Display success state and message.

        Args:
            message: Success message to display
        """
        self.download_progress_var.set("Download completed")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 100
        messagebox.showinfo("Success", message)

    def show_error(self, error_message: str) -> None:
        """
        Display error state and message.

        Args:
            error_message: Error message to display
        """
        self.download_progress_var.set("Download failed")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 0
        messagebox.showerror("Download Error", error_message)

    def get_frame(self) -> ttk.Frame:
        """
        Get the main frame for this component.

        Returns:
            The main frame widget containing all download UI elements
        """
        return self.frame
