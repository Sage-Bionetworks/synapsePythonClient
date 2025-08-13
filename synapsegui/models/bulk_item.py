"""
Bulk Item Model for Synapse Desktop Client.

This module contains the data model for items in bulk operations.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class BulkItem:
    """
    Represents an item in bulk download/upload operations.

    This class holds metadata about a Synapse entity that can be
    selected for bulk operations.
    """

    synapse_id: str
    name: str
    item_type: str  # "File" or "Folder" only
    size: Optional[int] = None
    parent_id: Optional[str] = None
    path: Optional[str] = None

    def __str__(self) -> str:
        """Return string representation of the item."""
        return f"{self.item_type}: {self.name} ({self.synapse_id})"

    def is_downloadable(self) -> bool:
        """
        Check if this item type can be downloaded.

        Returns:
            True if the item can be downloaded, False otherwise
        """
        return self.item_type in ["File", "Folder"]

    def get_display_size(self) -> str:
        """
        Get formatted size for display.

        Returns:
            Formatted size string or empty if no size
        """
        if not self.size or not isinstance(self.size, (int, float)):
            return ""

        size = float(self.size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    def get_display_modified(self) -> str:
        """
        Get formatted modification date for display.

        Returns:
            Formatted date string or empty if no date
        """
        if not self.modified:
            return ""
        return self.modified.strftime("%Y-%m-%d %H:%M")
