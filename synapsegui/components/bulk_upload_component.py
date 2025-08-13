"""
Bulk Upload Component for Synapse Desktop Client.

This component provides functionality to upload multiple files and folders
to Synapse containers with directory structure preservation.
"""

import os
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Dict, List, Optional

from ..models.bulk_item import BulkItem


class BulkUploadComponent:
    """Component for bulk upload operations with file/folder selection."""

    def __init__(
        self,
        parent: tk.Widget,
        on_bulk_upload: Callable[[List[BulkItem], str, bool], None],
        on_log_message: Callable[[str, bool], None],
        on_progress_update: Callable[[str, int], None],
    ) -> None:
        """
        Initialize the bulk upload component.

        Args:
            parent: Parent widget
            on_bulk_upload: Callback for bulk upload operation
            on_log_message: Callback for logging messages
            on_progress_update: Callback for progress updates
        """
        self.parent = parent
        self.on_bulk_upload = on_bulk_upload
        self.on_log_message = on_log_message
        self.on_progress_update = on_progress_update

        # UI elements
        self.frame: Optional[ttk.Frame] = None
        self.parent_id_var = tk.StringVar()
        self.preserve_structure_var = tk.BooleanVar(value=True)
        self.tree: Optional[ttk.Treeview] = None
        self.status_var = tk.StringVar()

        # Data
        self.upload_items: List[BulkItem] = []
        self.selected_items: Dict[str, BulkItem] = {}

    def create_ui(self) -> ttk.Frame:
        """Create and return the bulk upload UI."""
        self.frame = ttk.Frame(self.parent)

        # Parent container section
        parent_frame = ttk.LabelFrame(self.frame, text="Upload Destination")
        parent_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(parent_frame, text="Parent Folder ID:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        ttk.Entry(parent_frame, textvariable=self.parent_id_var, width=20).grid(
            row=0, column=1, padx=5, pady=5
        )

        ttk.Checkbutton(
            parent_frame,
            text="Preserve directory structure",
            variable=self.preserve_structure_var,
        ).grid(row=0, column=2, padx=10, pady=5)

        # File selection section
        selection_frame = ttk.LabelFrame(self.frame, text="File Selection")
        selection_frame.pack(fill="x", padx=10, pady=5)

        buttons_frame = ttk.Frame(selection_frame)
        buttons_frame.pack(fill="x", padx=5, pady=5)

        ttk.Button(buttons_frame, text="Add Files", command=self._add_files).pack(
            side="left", padx=5
        )

        ttk.Button(buttons_frame, text="Add Folder", command=self._add_folder).pack(
            side="left", padx=5
        )

        ttk.Button(
            buttons_frame, text="Remove Selected", command=self._remove_selected
        ).pack(side="left", padx=5)

        ttk.Button(buttons_frame, text="Clear All", command=self._clear_all).pack(
            side="left", padx=5
        )

        # Status section
        status_frame = ttk.Frame(self.frame)
        status_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(status_frame, text="Status:").pack(side="left")
        ttk.Label(status_frame, textvariable=self.status_var).pack(
            side="left", padx=(5, 0)
        )

        # File list tree section
        tree_frame = ttk.LabelFrame(self.frame, text="Files to Upload")
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Tree with scrollbars
        tree_container = ttk.Frame(tree_frame)
        tree_container.pack(fill="both", expand=True, padx=5, pady=5)

        self.tree = ttk.Treeview(
            tree_container,
            columns=("Type", "Size", "Local Path"),
            show="tree headings",
            selectmode="extended",
        )

        # Configure columns
        self.tree.heading("#0", text="Name")
        self.tree.heading("Type", text="Type")
        self.tree.heading("Size", text="Size")
        self.tree.heading("Local Path", text="Local Path")

        self.tree.column("#0", width=250)
        self.tree.column("Type", width=100)
        self.tree.column("Size", width=100)
        self.tree.column("Local Path", width=400)

        # Scrollbars for tree
        v_scrollbar = ttk.Scrollbar(
            tree_container, orient="vertical", command=self.tree.yview
        )
        h_scrollbar = ttk.Scrollbar(
            tree_container, orient="horizontal", command=self.tree.xview
        )
        self.tree.configure(
            yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set
        )

        self.tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # Upload section
        upload_frame = ttk.LabelFrame(self.frame, text="Upload Options")
        upload_frame.pack(fill="x", padx=10, pady=5)

        ttk.Button(
            upload_frame, text="Start Bulk Upload", command=self._on_upload_clicked
        ).pack(pady=10)

        # Bind tree selection
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        return self.frame

    def _add_files(self) -> None:
        """Add individual files to upload list."""
        files = filedialog.askopenfilenames(
            title="Select Files to Upload", filetypes=[("All Files", "*.*")]
        )

        if files:
            added_count = 0
            for file_path in files:
                if self._add_file_item(file_path):
                    added_count += 1

            self._refresh_tree()
            self._update_status()
            self.on_log_message(f"Added {added_count} files", False)

    def _add_folder(self) -> None:
        """Add a folder and its contents to upload list."""
        folder_path = filedialog.askdirectory(title="Select Folder to Upload")

        if folder_path:
            added_count = self._add_folder_recursive(folder_path)
            self._refresh_tree()
            self._update_status()
            self.on_log_message(f"Added folder with {added_count} items", False)

    def _add_file_item(self, file_path: str) -> bool:
        """
        Add a single file to the upload list.

        Args:
            file_path: Path to the file

        Returns:
            True if file was added, False if already exists
        """
        # Check if already added
        for item in self.upload_items:
            if item.path == file_path:
                return False

        # Get file info
        path_obj = Path(file_path)
        if not path_obj.exists():
            return False

        stat = path_obj.stat()

        # Create BulkItem
        item = BulkItem(
            synapse_id="",  # Will be set after upload
            name=path_obj.name,
            item_type="File",
            size=stat.st_size,
            modified=None,  # Will be set from file timestamp if needed
            parent_id=None,  # Will be determined during upload
            path=file_path,
        )

        self.upload_items.append(item)
        return True

    def _add_folder_recursive(self, folder_path: str) -> int:
        """
        Add a folder and all its contents recursively.

        Args:
            folder_path: Path to the folder

        Returns:
            Number of items added
        """
        added_count = 0
        folder_obj = Path(folder_path)

        if not folder_obj.exists() or not folder_obj.is_dir():
            return 0

        # Add the folder itself
        folder_item = BulkItem(
            synapse_id="",
            name=folder_obj.name,
            item_type="Folder",
            size=None,
            modified=None,
            parent_id=None,
            path=folder_path,
        )

        # Check if folder already added
        folder_exists = any(item.path == folder_path for item in self.upload_items)
        if not folder_exists:
            self.upload_items.append(folder_item)
            added_count += 1

        # Add all files and subfolders
        try:
            for item_path in folder_obj.rglob("*"):
                if item_path.is_file():
                    if self._add_file_item(str(item_path)):
                        added_count += 1
                elif item_path.is_dir() and item_path != folder_obj:
                    # Add subdirectory if not already added
                    if not any(
                        item.path == str(item_path) for item in self.upload_items
                    ):
                        subfolder_item = BulkItem(
                            synapse_id="",
                            name=item_path.name,
                            item_type="Folder",
                            size=None,
                            modified=None,
                            parent_id=None,
                            path=str(item_path),
                        )
                        self.upload_items.append(subfolder_item)
                        added_count += 1
        except PermissionError as e:
            self.on_log_message(f"Permission error accessing {folder_path}: {e}", True)

        return added_count

    def _remove_selected(self) -> None:
        """Remove selected items from upload list."""
        if not self.tree or not self.tree.selection():
            return

        selected_paths = []
        for item_id in self.tree.selection():
            item_values = self.tree.item(item_id, "values")
            if len(item_values) >= 3:
                selected_paths.append(item_values[2])  # Local Path column

        # Remove items
        original_count = len(self.upload_items)
        self.upload_items = [
            item for item in self.upload_items if item.path not in selected_paths
        ]
        removed_count = original_count - len(self.upload_items)

        if removed_count > 0:
            self._refresh_tree()
            self._update_status()
            self.on_log_message(f"Removed {removed_count} items", False)

    def _clear_all(self) -> None:
        """Clear all items from upload list."""
        if self.upload_items:
            count = len(self.upload_items)
            self.upload_items.clear()
            self._refresh_tree()
            self._update_status()
            self.on_log_message(f"Cleared {count} items", False)

    def _refresh_tree(self) -> None:
        """Refresh the tree view with current upload items."""
        # Clear tree
        if self.tree:
            for item in self.tree.get_children():
                self.tree.delete(item)

        # Group items by directory structure if preserving structure
        if self.preserve_structure_var.get():
            self._populate_tree_structured()
        else:
            self._populate_tree_flat()

    def _populate_tree_structured(self) -> None:
        """Populate tree with directory structure preserved."""
        # Build directory tree
        dirs_added = set()

        # Sort items by path depth to ensure parents are added first
        sorted_items = sorted(self.upload_items, key=lambda x: len(Path(x.path).parts))

        for item in sorted_items:
            path_obj = Path(item.path)

            # Ensure parent directories are in tree
            parent_iid = ""
            current_path = ""

            for part in path_obj.parts[:-1]:  # Exclude the file/folder name itself
                current_path = (
                    os.path.join(current_path, part) if current_path else part
                )
                dir_key = (parent_iid, part)

                if dir_key not in dirs_added:
                    parent_iid = self.tree.insert(
                        parent_iid,
                        "end",
                        text=part,
                        values=("Directory", "", current_path),
                        tags=("directory",),
                    )
                    dirs_added.add(dir_key)
                else:
                    # Find existing directory item
                    for child in self.tree.get_children(parent_iid):
                        if self.tree.item(child, "text") == part:
                            parent_iid = child
                            break

            # Add the actual item
            size_str = item.get_display_size()
            self.tree.insert(
                parent_iid,
                "end",
                text=item.name,
                values=(item.item_type, size_str, item.path),
                tags=(item.path,),
            )

    def _populate_tree_flat(self) -> None:
        """Populate tree with flat structure (no directories)."""
        for item in self.upload_items:
            size_str = item.get_display_size()
            self.tree.insert(
                "",
                "end",
                text=item.name,
                values=(item.item_type, size_str, item.path),
                tags=(item.path,),
            )

    def _update_status(self) -> None:
        """Update status display."""
        file_count = sum(1 for item in self.upload_items if item.item_type == "File")
        folder_count = sum(
            1 for item in self.upload_items if item.item_type == "Folder"
        )

        total_size = sum(item.size or 0 for item in self.upload_items if item.size)
        size_str = self._format_size(total_size) if total_size > 0 else "0 B"

        status_text = f"{file_count} files, {folder_count} folders, {size_str}"
        self.status_var.set(status_text)

    def _format_size(self, size: int) -> str:
        """Format file size for display."""
        size = float(size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    def _on_tree_selection_changed(self, event: tk.Event) -> None:
        """Handle tree selection change."""
        # Update selected items for potential removal
        pass

    def _on_upload_clicked(self) -> None:
        """Handle upload button click."""
        if not self.upload_items:
            messagebox.showwarning("Warning", "Please add files or folders to upload")
            return

        parent_id = self.parent_id_var.get().strip()
        if not parent_id:
            messagebox.showerror("Error", "Please enter a parent folder ID")
            return

        if not parent_id.startswith("syn"):
            messagebox.showerror("Error", "Parent ID must start with 'syn'")
            return

        # Confirm upload
        file_count = sum(1 for item in self.upload_items if item.item_type == "File")
        folder_count = sum(
            1 for item in self.upload_items if item.item_type == "Folder"
        )

        message = (
            f"Upload {file_count} files and {folder_count} folders to {parent_id}?"
        )
        if not messagebox.askyesno("Confirm Upload", message):
            return

        self.on_log_message(
            f"Starting bulk upload of {len(self.upload_items)} items", False
        )

        # Call the bulk upload callback
        self.on_bulk_upload(
            self.upload_items.copy(), parent_id, self.preserve_structure_var.get()
        )
