"""
Upload component UI - separated from main window
"""
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional


class UploadComponent:
    """
    Upload file UI component.

    Provides functionality for uploading individual files to Synapse,
    supporting both new file creation and updating existing entities.
    """

    def __init__(
        self,
        parent: tk.Widget,
        on_upload_callback: Callable[[str, str, str, str], None],
    ) -> None:
        """
        Initialize the upload component.

        Args:
            parent: Parent widget to contain this component
            on_upload_callback: Callback function for upload operations
        """
        self.parent = parent
        self.on_upload = on_upload_callback

        self.upload_file_var = tk.StringVar()
        self.upload_mode_var = tk.StringVar(value="new")
        self.parent_id_var = tk.StringVar()
        self.entity_id_var = tk.StringVar()
        self.upload_name_var = tk.StringVar()
        self.upload_progress_var = tk.StringVar(value="")

        self.upload_button: Optional[ttk.Button] = None
        self.upload_progress_bar: Optional[ttk.Progressbar] = None
        self.parent_id_entry: Optional[ttk.Entry] = None
        self.entity_id_entry: Optional[ttk.Entry] = None
        self.frame: Optional[ttk.Frame] = None

        self.create_ui()

    def create_ui(self) -> None:
        """Create and configure the upload UI components."""
        self.frame = ttk.Frame(self.parent, padding="10")
        self.frame.columnconfigure(1, weight=1)

        ttk.Label(self.frame, text="File to Upload:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )

        file_frame = ttk.Frame(self.frame)
        file_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0))
        file_frame.columnconfigure(0, weight=1)

        upload_file_entry = ttk.Entry(file_frame, textvariable=self.upload_file_var)
        upload_file_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        browse_file_button = ttk.Button(
            file_frame, text="Browse", command=self.browse_upload_file
        )
        browse_file_button.grid(row=0, column=1)

        mode_frame = ttk.LabelFrame(self.frame, text="Upload Mode", padding="10")
        mode_frame.grid(
            row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0)
        )
        mode_frame.columnconfigure(2, weight=1)

        new_radio = ttk.Radiobutton(
            mode_frame,
            text="Create New File",
            variable=self.upload_mode_var,
            value="new",
            command=self.on_upload_mode_change,
        )
        new_radio.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        update_radio = ttk.Radiobutton(
            mode_frame,
            text="Update Existing File",
            variable=self.upload_mode_var,
            value="update",
            command=self.on_upload_mode_change,
        )
        update_radio.grid(row=1, column=0, sticky=tk.W)

        parent_label = ttk.Label(mode_frame, text="Parent ID (Project/Folder):")
        parent_label.grid(row=0, column=1, sticky=tk.W, padx=(20, 5), pady=(0, 5))

        self.parent_id_entry = ttk.Entry(
            mode_frame, textvariable=self.parent_id_var, width=30
        )
        self.parent_id_entry.grid(row=0, column=2, sticky=(tk.W, tk.E), pady=(0, 5))

        entity_label = ttk.Label(mode_frame, text="Entity ID to Update:")
        entity_label.grid(row=1, column=1, sticky=tk.W, padx=(20, 5))

        self.entity_id_entry = ttk.Entry(
            mode_frame, textvariable=self.entity_id_var, width=30, state="disabled"
        )
        self.entity_id_entry.grid(row=1, column=2, sticky=(tk.W, tk.E))

        ttk.Label(self.frame, text="Entity Name (optional):").grid(
            row=2, column=0, sticky=tk.W, pady=(10, 5)
        )
        upload_name_entry = ttk.Entry(
            self.frame, textvariable=self.upload_name_var, width=40
        )
        upload_name_entry.grid(
            row=2, column=1, sticky=(tk.W, tk.E), pady=(10, 5), padx=(10, 0)
        )

        self.upload_button = ttk.Button(
            self.frame,
            text="Upload File",
            command=self._handle_upload,
            state="disabled",
        )
        self.upload_button.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        upload_progress_label = ttk.Label(
            self.frame,
            textvariable=self.upload_progress_var,
            foreground="blue",
            font=("Arial", 8),
        )
        upload_progress_label.grid(row=4, column=0, columnspan=2, pady=(5, 0))

        self.upload_progress_bar = ttk.Progressbar(self.frame, mode="determinate")
        self.upload_progress_bar.grid(
            row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0)
        )

    def browse_upload_file(self) -> None:
        """Open file browser to select a file for upload."""
        file_path = filedialog.askopenfilename(
            title="Select file to upload", initialdir=str(Path.home())
        )
        if file_path:
            self.upload_file_var.set(file_path)
            if not self.upload_name_var.get():
                self.upload_name_var.set(Path(file_path).name)

    def on_upload_mode_change(self) -> None:
        """Handle upload mode radio button changes to update field availability."""
        mode = self.upload_mode_var.get()
        if mode == "new":
            if self.parent_id_entry:
                self.parent_id_entry.config(state="normal")
            if self.entity_id_entry:
                self.entity_id_entry.config(state="disabled")
            self.entity_id_var.set("")
        else:
            if self.parent_id_entry:
                self.parent_id_entry.config(state="disabled")
            if self.entity_id_entry:
                self.entity_id_entry.config(state="normal")
            self.parent_id_var.set("")

    def _handle_upload(self) -> None:
        """Handle upload button click and validate input parameters."""
        file_path = self.upload_file_var.get().strip()
        parent_id = self.parent_id_var.get().strip()
        entity_id = self.entity_id_var.get().strip()
        name = self.upload_name_var.get().strip()

        if not file_path:
            messagebox.showerror("Error", "File path is required")
            return

        mode = self.upload_mode_var.get()
        if mode == "new" and not parent_id:
            messagebox.showerror("Error", "Parent ID is required for new files")
            return
        elif mode == "update" and not entity_id:
            messagebox.showerror("Error", "Entity ID is required for updates")
            return

        self.on_upload(file_path, parent_id, entity_id, name)

    def set_enabled(self, enabled: bool) -> None:
        """
        Enable or disable upload functionality.

        Args:
            enabled: True to enable upload operations, False to disable
        """
        state = "normal" if enabled else "disabled"
        if self.upload_button:
            self.upload_button.config(state=state)

    def start_operation(self) -> None:
        """Initialize UI state when upload operation begins."""
        self.upload_progress_var.set("Preparing upload...")
        if self.upload_progress_bar:
            self.upload_progress_bar["value"] = 0

    def update_progress(self, progress: int, message: str) -> None:
        """
        Update progress bar and status message.

        Args:
            progress: Progress percentage (0-100)
            message: Status message to display
        """
        if self.upload_progress_bar:
            self.upload_progress_bar["value"] = progress
        self.upload_progress_var.set(message)

    def show_success(self, message: str) -> None:
        """
        Display success state and message.

        Args:
            message: Success message to display
        """
        self.upload_progress_var.set("Upload completed")
        if self.upload_progress_bar:
            self.upload_progress_bar["value"] = 100
        messagebox.showinfo("Success", message)

    def show_error(self, error_message: str) -> None:
        """
        Display error state and message.

        Args:
            error_message: Error message to display
        """
        self.upload_progress_var.set("Upload failed")
        if self.upload_progress_bar:
            self.upload_progress_bar["value"] = 0
        messagebox.showerror("Upload Error", error_message)

    def get_frame(self) -> ttk.Frame:
        """
        Get the main frame for this component.

        Returns:
            The main frame widget containing all upload UI elements
        """
        return self.frame
