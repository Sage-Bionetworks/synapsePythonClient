"""
Bulk Upload Component for Synapse Desktop Client.

This component provides functionality to upload multiple files and folders
to Synapse containers with directory structure preservation.
"""

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

        self.frame: Optional[ttk.Frame] = None
        self.parent_id_var = tk.StringVar()
        self.preserve_structure_var = tk.BooleanVar(value=True)
        self.tree: Optional[ttk.Treeview] = None
        self.status_var = tk.StringVar()
        self.bulk_progress_bar: Optional[ttk.Progressbar] = None

        self.upload_items: List[BulkItem] = []
        self.selected_items: Dict[str, BulkItem] = {}

    def create_ui(self) -> ttk.Frame:
        """Create and return the bulk upload UI."""
        self.frame = ttk.Frame(self.parent)

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

        status_frame = ttk.Frame(self.frame)
        status_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(status_frame, text="Status:").pack(side="left")
        ttk.Label(status_frame, textvariable=self.status_var).pack(
            side="left", padx=(5, 0)
        )

        self.bulk_progress_bar = ttk.Progressbar(
            status_frame, mode="determinate", length=300
        )
        self.bulk_progress_bar.pack(side="right", padx=(10, 0))

        tree_frame = ttk.LabelFrame(self.frame, text="Files to Upload")
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree_container = ttk.Frame(tree_frame)
        tree_container.pack(fill="both", expand=True, padx=5, pady=5)

        self.tree = ttk.Treeview(
            tree_container,
            columns=("Type", "Size", "Local Path"),
            show="tree headings",
            selectmode="extended",
        )

        self.tree.heading("#0", text="Name")
        self.tree.heading("Type", text="Type")
        self.tree.heading("Size", text="Size")
        self.tree.heading("Local Path", text="Local Path")

        self.tree.column("#0", width=250)
        self.tree.column("Type", width=100)
        self.tree.column("Size", width=100)
        self.tree.column("Local Path", width=400)

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

        upload_frame = ttk.LabelFrame(self.frame, text="Upload Options")
        upload_frame.pack(fill="x", padx=10, pady=5)

        ttk.Button(
            upload_frame, text="Start Bulk Upload", command=self._on_upload_clicked
        ).pack(pady=10)

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        return self.frame

    def _add_files(self) -> None:
        """Open file dialog to add individual files to the upload list."""
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
        """Open directory dialog to add a folder and its contents to the upload list."""
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
        for item in self.upload_items:
            if item.path == file_path:
                return False

        path_obj = Path(file_path)
        if not path_obj.exists():
            return False

        stat = path_obj.stat()

        item = BulkItem(
            synapse_id="",  # Will be set after upload
            name=path_obj.name,
            item_type="File",
            size=stat.st_size,
            parent_id=None,  # Will be determined during upload
            path=file_path,
        )

        self.upload_items.append(item)
        return True

    def _add_folder_recursive(self, folder_path: str) -> int:
        """
        Add a folder and all its contents recursively to the upload list.

        Args:
            folder_path: Path to the folder to add

        Returns:
            Number of items added to the upload list
        """
        added_count = 0
        folder_obj = Path(folder_path)

        if not folder_obj.exists() or not folder_obj.is_dir():
            return 0

        folder_item = BulkItem(
            synapse_id="",
            name=folder_obj.name,
            item_type="Folder",
            size=None,
            parent_id=None,
            path=folder_path,
        )

        folder_exists = any(item.path == folder_path for item in self.upload_items)
        if not folder_exists:
            self.upload_items.append(folder_item)
            added_count += 1

        try:
            for item_path in folder_obj.rglob("*"):
                if item_path.is_file():
                    if self._add_file_item(str(item_path)):
                        added_count += 1
        except PermissionError as e:
            self.on_log_message(f"Permission error accessing {folder_path}: {e}", True)

        return added_count

    def _remove_selected(self) -> None:
        """Remove selected items from the upload list."""
        if not self.tree or not self.tree.selection():
            return

        selected_paths = []
        for item_id in self.tree.selection():
            item_tags = self.tree.item(item_id, "tags")
            if "visual_directory" in item_tags:
                continue

            item_values = self.tree.item(item_id, "values")
            if len(item_values) >= 3:
                selected_paths.append(item_values[2])

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
        """Clear all items from the upload list."""
        if self.upload_items:
            count = len(self.upload_items)
            self.upload_items.clear()
            self._refresh_tree()
            self._update_status()
            self.on_log_message(f"Cleared {count} items", False)

    def _refresh_tree(self) -> None:
        """Refresh the tree view with current upload items."""
        if self.tree:
            for item in self.tree.get_children():
                self.tree.delete(item)

        if self.preserve_structure_var.get():
            self._populate_tree_structured()
        else:
            self._populate_tree_flat()

    def _populate_tree_structured(self) -> None:
        """Populate tree view with hierarchical directory structure preserved."""
        root_folders = []
        for item in self.upload_items:
            if item.item_type == "Folder":
                is_root = True
                for other_item in self.upload_items:
                    if (
                        other_item.item_type == "Folder"
                        and other_item.path != item.path
                        and self._is_subpath(item.path, other_item.path)
                    ):
                        is_root = False
                        break
                if is_root:
                    root_folders.append(item)

        tree_nodes = {}

        for root_folder in root_folders:
            root_id = self.tree.insert(
                "",
                "end",
                text=root_folder.name,
                values=(
                    root_folder.item_type,
                    root_folder.get_display_size(),
                    root_folder.path,
                ),
                tags=(root_folder.path,),
            )
            tree_nodes[root_folder.path] = root_id

        remaining_items = [
            item for item in self.upload_items if item not in root_folders
        ]

        for item in sorted(remaining_items, key=lambda x: len(Path(x.path).parts)):
            parent_id = ""
            best_parent_path = ""

            for root_folder in root_folders:
                if self._is_subpath(item.path, root_folder.path):
                    parent_id = tree_nodes[root_folder.path]
                    best_parent_path = root_folder.path
                    break

            if parent_id and best_parent_path:
                relative_parts = self._get_relative_path_parts(
                    item.path, best_parent_path
                )

                current_parent_id = parent_id
                current_path = Path(best_parent_path)

                for part in relative_parts[:-1]:
                    current_path = current_path / part
                    current_path_str = str(current_path)

                    if current_path_str not in tree_nodes:
                        current_parent_id = self.tree.insert(
                            current_parent_id,
                            "end",
                            text=part,
                            values=("Directory", "", current_path_str),
                            tags=("visual_directory",),
                        )
                        tree_nodes[current_path_str] = current_parent_id
                    else:
                        current_parent_id = tree_nodes[current_path_str]

                self.tree.insert(
                    current_parent_id,
                    "end",
                    text=item.name,
                    values=(item.item_type, item.get_display_size(), item.path),
                    tags=(item.path,),
                )
            else:
                self.tree.insert(
                    "",
                    "end",
                    text=item.name,
                    values=(item.item_type, item.get_display_size(), item.path),
                    tags=(item.path,),
                )

    def _is_subpath(self, child_path: str, parent_path: str) -> bool:
        """
        Check if child_path is a subpath of parent_path.

        Args:
            child_path: Path to check if it's a child
            parent_path: Path to check if it's a parent

        Returns:
            True if child_path is within parent_path
        """
        try:
            Path(child_path).relative_to(Path(parent_path))
            return True
        except ValueError:
            return False

    def _get_relative_path_parts(self, child_path: str, parent_path: str) -> List[str]:
        """
        Get the relative path parts from parent to child.

        Args:
            child_path: Full path to the child
            parent_path: Full path to the parent

        Returns:
            List of path components from parent to child
        """
        try:
            relative_path = Path(child_path).relative_to(Path(parent_path))
            return list(relative_path.parts)
        except ValueError:
            return []

    def _get_path_for_node(self, node_id: str) -> str:
        """
        Get the file path for a tree node.

        Args:
            node_id: Tree node identifier

        Returns:
            File path associated with the node
        """
        values = self.tree.item(node_id, "values")
        return values[2] if len(values) >= 3 else ""

    def _populate_tree_flat(self) -> None:
        """Populate tree view with flat structure without directory hierarchy."""
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
        """Update the status display with current file and folder counts."""
        file_count = sum(1 for item in self.upload_items if item.item_type == "File")
        folder_count = sum(
            1 for item in self.upload_items if item.item_type == "Folder"
        )

        total_size = sum(item.size or 0 for item in self.upload_items if item.size)
        size_str = self._format_size(total_size) if total_size > 0 else "0 B"

        status_text = f"{file_count} files, {folder_count} folders, {size_str}"
        self.status_var.set(status_text)

    def _format_size(self, size: int) -> str:
        """
        Format file size for human-readable display.

        Args:
            size: File size in bytes

        Returns:
            Formatted size string with appropriate units
        """
        size = float(size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    def _on_tree_selection_changed(self, event: tk.Event) -> None:
        """
        Handle tree view selection changes.

        Args:
            event: Tkinter event object
        """
        pass

    def _on_upload_clicked(self) -> None:
        """Handle upload button click to initiate bulk upload."""
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

        self.on_bulk_upload(
            self.upload_items.copy(), parent_id, self.preserve_structure_var.get()
        )

    def update_progress(self, progress: int, message: str) -> None:
        """Update the progress bar and status message.

        Args:
            progress: Progress percentage (0-100)
            message: Status message to display
        """
        if self.bulk_progress_bar:
            self.bulk_progress_bar["value"] = progress
        self.status_var.set(message)

    def start_bulk_operation(self) -> None:
        """Called when a bulk operation starts"""
        self.status_var.set("Starting bulk upload...")
        if self.bulk_progress_bar:
            self.bulk_progress_bar["value"] = 0

    def complete_bulk_operation(self, success: bool, message: str) -> None:
        """Called when a bulk operation completes

        Args:
            success: Whether the operation was successful
            message: Completion message
        """
        if self.bulk_progress_bar:
            self.bulk_progress_bar["value"] = 100 if success else 0
        self.status_var.set(message)

        if success:
            self.on_log_message(f"Bulk upload completed: {message}", False)
        else:
            self.on_log_message(f"Bulk upload failed: {message}", True)
