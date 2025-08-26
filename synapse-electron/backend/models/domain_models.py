"""
Domain Models for Synapse Desktop Client.

This module contains business domain models that represent core entities
in the application with associated business logic methods.
"""

from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class BulkItem:
    """
    Represents an item in bulk download/upload operations.

    This class holds metadata about a Synapse entity that can be
    selected for bulk operations such as downloads or uploads.

    Attributes:
        synapse_id: Unique identifier for the Synapse entity
        name: Display name of the item
        item_type: Type of item ("File" or "Folder" only)
        size: Size in bytes for files, None for folders
        parent_id: ID of the parent container
        path: Local or Synapse path information
    """

    synapse_id: str
    name: str
    item_type: str  # "File" or "Folder" only
    size: Optional[Union[int, float]] = None
    parent_id: Optional[str] = None
    path: Optional[str] = None

    def __str__(self) -> str:
        """
        Return string representation of the item.

        Arguments:
            None

        Returns:
            str: Formatted string showing item type, name, and Synapse ID

        Raises:
            None: This method does not raise exceptions.
        """
        return f"{self.item_type}: {self.name} ({self.synapse_id})"

    def is_downloadable(self) -> bool:
        """
        Check if this item type can be downloaded.

        Determines whether the item is of a type that supports download
        operations based on its item_type.

        Arguments:
            None

        Returns:
            bool: True if the item can be downloaded, False otherwise

        Raises:
            None: This method does not raise exceptions.
        """
        return self.item_type in ["File", "Folder"]

    def get_display_size(self) -> str:
        """
        Get formatted size for display.

        Converts the raw byte size into a human-readable format with
        appropriate units (B, KB, MB, GB, TB, PB).

        Arguments:
            None

        Returns:
            str: Formatted size string with units, or empty string if no size available

        Raises:
            None: This method handles invalid sizes gracefully.
        """
        if not self.size or not isinstance(self.size, (int, float)):
            return ""

        size = float(self.size)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
