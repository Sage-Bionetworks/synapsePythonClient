"""
Progress handling utilities
"""
from typing import Callable


class ProgressManager:
    """
    Manages progress updates for different operations.
    
    Provides a centralized way to register, update, and manage 
    progress callbacks for various application operations.
    """

    def __init__(self) -> None:
        """Initialize the progress manager with an empty callback registry."""
        self.callbacks = {}

    def register_callback(self, operation: str, callback: Callable[[int, str], None]) -> None:
        """
        Register a progress callback for a specific operation.

        Args:
            operation: Name of the operation to register callback for
            callback: Function to call with progress updates (progress, message)
        """
        self.callbacks[operation] = callback

    def update_progress(self, operation: str, progress: int, message: str) -> None:
        """
        Update progress for a specific operation.

        Args:
            operation: Name of the operation to update
            progress: Progress percentage (0-100)
            message: Progress message to display
        """
        if operation in self.callbacks:
            self.callbacks[operation](progress, message)

    def unregister_callback(self, operation: str) -> None:
        """
        Remove callback for a specific operation.

        Args:
            operation: Name of the operation to unregister
        """
        if operation in self.callbacks:
            del self.callbacks[operation]
