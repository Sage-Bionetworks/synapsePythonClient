"""
Bulk Download Component for Synapse Desktop Client.

This component provides functionality to enumerate and selectively download
multiple items from Synapse containers (Projects/Folders).
"""

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, List, Optional

from ..models.bulk_item import BulkItem


class BulkDownloadComponent:
    """Component for bulk download operations with enumeration and selection."""

    def __init__(
        self,
        parent: tk.Widget,
        on_bulk_download: Callable[[List[BulkItem], str, bool], None],
        on_enumerate: Callable[[str, bool], None],
        on_log_message: Callable[[str, bool], None],
        on_progress_update: Callable[[str, int], None],
    ) -> None:
        """
        Initialize the bulk download component.

        Args:
            parent: Parent widget
            on_bulk_download: Callback for bulk download operation
            on_enumerate: Callback for enumeration operation
            on_log_message: Callback for logging messages
            on_progress_update: Callback for progress updates
        """
        self.parent = parent
        self.on_bulk_download = on_bulk_download
        self.on_enumerate = on_enumerate
        self.on_log_message = on_log_message
        self.on_progress_update = on_progress_update

        # UI elements
        self.frame: Optional[ttk.Frame] = None
        self.container_id_var = tk.StringVar()
        self.download_path_var = tk.StringVar(value=str(Path.home() / "Downloads"))
        self.recursive_var = tk.BooleanVar(value=True)
        self.tree: Optional[ttk.Treeview] = None
        self.progress_var = tk.StringVar()
        self.bulk_progress_bar: Optional[ttk.Progressbar] = None

        # Data
        self.container_items: List[BulkItem] = []
        self.selected_items: Dict[str, BulkItem] = {}

    def create_ui(self) -> ttk.Frame:
        """Create and return the bulk download UI."""
        self.frame = ttk.Frame(self.parent)

        # Container ID section
        container_frame = ttk.LabelFrame(self.frame, text="Container Information")
        container_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(container_frame, text="Project/Folder ID:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        ttk.Entry(container_frame, textvariable=self.container_id_var, width=20).grid(
            row=0, column=1, padx=5, pady=5
        )

        ttk.Checkbutton(
            container_frame,
            text="Recursive (include subfolders)",
            variable=self.recursive_var,
        ).grid(row=0, column=2, padx=10, pady=5)

        ttk.Button(
            container_frame,
            text="Enumerate Contents",
            command=self._on_enumerate_clicked,
        ).grid(row=0, column=3, padx=5, pady=5)

        # Progress section
        progress_frame = ttk.Frame(self.frame)
        progress_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(progress_frame, text="Status:").pack(side="left")
        ttk.Label(progress_frame, textvariable=self.progress_var).pack(
            side="left", padx=(5, 0)
        )

        # Progress bar for bulk operations
        self.bulk_progress_bar = ttk.Progressbar(
            progress_frame, mode="determinate", length=300
        )
        self.bulk_progress_bar.pack(side="right", padx=(10, 0))

        # Selection tree section
        tree_frame = ttk.LabelFrame(self.frame, text="Contents Selection")
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Tree with scrollbars
        tree_container = ttk.Frame(tree_frame)
        tree_container.pack(fill="both", expand=True, padx=5, pady=5)

        self.tree = ttk.Treeview(
            tree_container,
            columns=("ID", "Type", "Size"),
            show="tree headings",
            selectmode="extended",
        )

        # Configure columns
        self.tree.heading("#0", text="Name")
        self.tree.heading("ID", text="Synapse ID")
        self.tree.heading("Type", text="Type")
        self.tree.heading("Size", text="Size")

        self.tree.column("#0", width=250)
        self.tree.column("ID", width=120)
        self.tree.column("Type", width=80)
        self.tree.column("Size", width=100)

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

        # Selection buttons
        selection_frame = ttk.Frame(tree_frame)
        selection_frame.pack(fill="x", padx=5, pady=5)

        ttk.Button(selection_frame, text="Select All", command=self._select_all).pack(
            side="left", padx=5
        )
        ttk.Button(selection_frame, text="Select None", command=self._select_none).pack(
            side="left", padx=5
        )
        ttk.Button(
            selection_frame, text="Select Files Only", command=self._select_files_only
        ).pack(side="left", padx=5)
        ttk.Button(
            selection_frame,
            text="Select Folders Only",
            command=self._select_folders_only,
        ).pack(side="left", padx=5)

        # Download section
        download_frame = ttk.LabelFrame(self.frame, text="Download Options")
        download_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(download_frame, text="Download Path:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        ttk.Entry(download_frame, textvariable=self.download_path_var, width=50).grid(
            row=0, column=1, padx=5, pady=5
        )
        ttk.Button(
            download_frame, text="Browse", command=self._browse_download_path
        ).grid(row=0, column=2, padx=5, pady=5)

        ttk.Button(
            download_frame,
            text="Download Selected Items",
            command=self._on_download_clicked,
        ).grid(row=1, column=1, pady=10)

        # Bind tree selection
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        return self.frame

    def _on_enumerate_clicked(self) -> None:
        """Handle enumerate button click."""
        container_id = self.container_id_var.get().strip()
        if not container_id:
            messagebox.showerror("Error", "Please enter a Project or Folder ID")
            return

        if not container_id.startswith(("syn", "project")):
            messagebox.showerror("Error", "ID must start with 'syn' or 'project'")
            return

        # Clear previous results
        self._clear_tree()
        self.container_items.clear()
        self.selected_items.clear()

        # Update status
        self.progress_var.set("Enumerating contents...")
        self.on_log_message(
            f"Starting enumeration of {container_id} (recursive={self.recursive_var.get()})",
            False,
        )

        # Delegate to controller using the same pattern as other operations
        self.on_enumerate(container_id, self.recursive_var.get())

    def handle_enumeration_result(
        self, items: List[BulkItem], error: str = None
    ) -> None:
        """
        Handle enumeration results from the controller.

        Args:
            items: List of enumerated BulkItem objects, or empty list if error
            error: Error message if enumeration failed, None if successful
        """
        if error:
            self.progress_var.set("Enumeration failed")
            self.on_log_message(f"Enumeration failed: {error}", True)
            return

        # Store items and update UI
        self.container_items = items
        self._populate_tree()
        count = len(self.container_items)
        self.progress_var.set(f"Found {count} items")
        self.on_log_message(f"Enumeration complete: {count} items found", False)

    def _populate_tree(self) -> None:
        """Populate the tree view with enumerated items."""
        self._clear_tree()

        # Group items by parent to build tree structure
        parent_map = {}
        root_items = []
        all_parent_ids = set()

        for item in self.container_items:
            if item.parent_id:
                if item.parent_id not in parent_map:
                    parent_map[item.parent_id] = []
                parent_map[item.parent_id].append(item)
                all_parent_ids.add(item.parent_id)
            else:
                root_items.append(item)

        # If we have no root items but have items with parent_ids,
        # find items whose parent_id doesn't exist in our item list
        # These are effectively the "root" items for our view
        if not root_items and self.container_items:
            item_ids = {item.synapse_id for item in self.container_items}
            for item in self.container_items:
                if item.parent_id and item.parent_id not in item_ids:
                    # This item's parent is not in our enumerated list,
                    # so treat it as a root item for display purposes
                    root_items.append(item)

        # Insert root items
        for item in root_items:
            self._insert_item_with_children(item, "", parent_map)

    def _insert_item_with_children(
        self, item: BulkItem, parent_iid: str, parent_map: Dict[str, List[BulkItem]]
    ) -> str:
        """
        Insert an item and its children into the tree.

        Args:
            item: Item to insert
            parent_iid: Parent item ID in tree
            parent_map: Map of parent IDs to child items

        Returns:
            Tree item ID
        """
        # Format size
        size_str = self._format_size(item.size) if item.size else ""

        # Insert item with ID, Type, and Size columns
        iid = self.tree.insert(
            parent_iid,
            "end",
            text=item.name,
            values=(item.synapse_id, item.item_type, size_str),
            tags=(item.synapse_id,),
        )

        # Insert children if any
        if item.synapse_id in parent_map:
            for child in parent_map[item.synapse_id]:
                self._insert_item_with_children(child, iid, parent_map)

        return iid

    def _format_size(self, size: Any) -> str:
        """Format file size for display."""
        if not isinstance(size, (int, float)):
            return ""

        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    def _clear_tree(self) -> None:
        """Clear all items from the tree."""
        if self.tree:
            for item in self.tree.get_children():
                self.tree.delete(item)

    def _select_all(self) -> None:
        """Select all items in the tree."""
        if self.tree:
            self.tree.selection_set(self.tree.get_children())
            self._update_selected_items()

    def _select_none(self) -> None:
        """Deselect all items in the tree."""
        if self.tree:
            self.tree.selection_remove(self.tree.selection())
            self._update_selected_items()

    def _select_files_only(self) -> None:
        """Select only file items."""
        if self.tree:
            self.tree.selection_remove(self.tree.selection())
            file_items = []
            self._collect_items_by_type(self.tree.get_children(), "File", file_items)
            if file_items:
                self.tree.selection_set(file_items)
            self._update_selected_items()

    def _select_folders_only(self) -> None:
        """Select only folder items."""
        if self.tree:
            self.tree.selection_remove(self.tree.selection())
            folder_items = []
            self._collect_items_by_type(
                self.tree.get_children(), "Folder", folder_items
            )
            if folder_items:
                self.tree.selection_set(folder_items)
            self._update_selected_items()

    def _collect_items_by_type(
        self, items: List[str], item_type: str, result: List[str]
    ) -> None:
        """Recursively collect items of specific type."""
        for item_id in items:
            item_values = self.tree.item(item_id, "values")
            # Type is now at index 1 (ID, Type, Size)
            if item_values and len(item_values) > 1 and item_values[1] == item_type:
                result.append(item_id)

            # Check children
            children = self.tree.get_children(item_id)
            if children:
                self._collect_items_by_type(children, item_type, result)

    def _on_tree_selection_changed(self, event: tk.Event) -> None:
        """Handle tree selection change."""
        self._update_selected_items()

    def _update_selected_items(self) -> None:
        """Update the selected items dictionary."""
        self.selected_items.clear()

        if not self.tree:
            return

        selection = self.tree.selection()
        for item_id in selection:
            # Get synapse ID from tags
            tags = self.tree.item(item_id, "tags")
            if tags:
                synapse_id = tags[0]
                # Find the corresponding BulkItem
                for bulk_item in self.container_items:
                    if bulk_item.synapse_id == synapse_id:
                        self.selected_items[synapse_id] = bulk_item
                        break

    def _browse_download_path(self) -> None:
        """Browse for download directory."""
        path = filedialog.askdirectory(title="Select Download Directory")
        if path:
            self.download_path_var.set(path)

    def _on_download_clicked(self) -> None:
        """Handle download button click."""
        if not self.selected_items:
            messagebox.showwarning("Warning", "Please select items to download")
            return

        download_path = self.download_path_var.get().strip()
        if not download_path:
            messagebox.showerror("Error", "Please select a download directory")
            return

        # Get list of selected items
        selected_list = list(self.selected_items.values())

        self.on_log_message(
            f"Starting bulk download of {len(selected_list)} items", False
        )

        # Call the bulk download callback
        self.on_bulk_download(selected_list, download_path, self.recursive_var.get())

    def update_progress(self, progress: int, message: str) -> None:
        """Update the progress bar and status message.

        Args:
            progress: Progress percentage (0-100)
            message: Status message to display
        """
        if self.bulk_progress_bar:
            self.bulk_progress_bar["value"] = progress
        self.progress_var.set(message)

    def start_bulk_operation(self) -> None:
        """Called when a bulk operation starts"""
        self.progress_var.set("Starting bulk download...")
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
        self.progress_var.set(message)

        if success:
            self.on_log_message(f"Bulk download completed: {message}", False)
        else:
            self.on_log_message(f"Bulk download failed: {message}", True)
