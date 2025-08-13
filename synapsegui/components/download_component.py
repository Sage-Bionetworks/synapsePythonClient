"""
Download component UI - separated from main window
"""
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional


class DownloadComponent:
    """Download file UI component"""

    def __init__(
        self, parent: tk.Widget, on_download_callback: Callable[[str, str, str], None]
    ):
        self.parent = parent
        self.on_download = on_download_callback

        # State variables
        self.download_id_var = tk.StringVar()
        self.download_version_var = tk.StringVar()
        self.download_location_var = tk.StringVar(value=str(Path.home() / "Downloads"))
        self.download_progress_var = tk.StringVar(value="")

        # UI references
        self.download_button: Optional[ttk.Button] = None
        self.download_progress_bar: Optional[ttk.Progressbar] = None
        self.frame: Optional[ttk.Frame] = None

        self.create_ui()

    def create_ui(self) -> None:
        """Create download UI components"""
        self.frame = ttk.Frame(self.parent, padding="10")
        self.frame.columnconfigure(1, weight=1)

        # Synapse ID
        ttk.Label(self.frame, text="Synapse ID:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )
        download_id_entry = ttk.Entry(
            self.frame, textvariable=self.download_id_var, width=40
        )
        download_id_entry.grid(
            row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        # Version (optional)
        ttk.Label(self.frame, text="Version (optional):").grid(
            row=1, column=0, sticky=tk.W, pady=(0, 5)
        )
        download_version_entry = ttk.Entry(
            self.frame, textvariable=self.download_version_var, width=40
        )
        download_version_entry.grid(
            row=1, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        # Download location
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

        # Download button
        self.download_button = ttk.Button(
            self.frame,
            text="Download File",
            command=self._handle_download,
            state="disabled",
        )
        self.download_button.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        # Progress bar for downloads
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
        """Browse for download directory"""
        directory = filedialog.askdirectory(initialdir=self.download_location_var.get())
        if directory:
            self.download_location_var.set(directory)

    def _handle_download(self) -> None:
        """Handle download button click"""
        synapse_id = self.download_id_var.get().strip()
        version = self.download_version_var.get().strip()
        download_path = self.download_location_var.get().strip()

        if not synapse_id:
            messagebox.showerror("Error", "Synapse ID is required")
            return

        self.on_download(synapse_id, version, download_path)

    def set_enabled(self, enabled: bool) -> None:
        """Enable/disable download functionality"""
        state = "normal" if enabled else "disabled"
        if self.download_button:
            self.download_button.config(state=state)

    def start_operation(self) -> None:
        """Called when download operation starts"""
        self.download_progress_var.set("Preparing download...")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 0

    def update_progress(self, progress: int, message: str) -> None:
        """Update progress bar and message"""
        if self.download_progress_bar:
            self.download_progress_bar["value"] = progress
        self.download_progress_var.set(message)

    def show_success(self, message: str) -> None:
        """Show success state"""
        self.download_progress_var.set("Download completed")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 100
        messagebox.showinfo("Success", message)

    def show_error(self, error_message: str) -> None:
        """Show error state"""
        self.download_progress_var.set("Download failed")
        if self.download_progress_bar:
            self.download_progress_bar["value"] = 0
        messagebox.showerror("Download Error", error_message)

    def get_frame(self) -> ttk.Frame:
        """Get the main frame for this component"""
        return self.frame
