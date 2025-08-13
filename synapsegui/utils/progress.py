"""
Progress handling utilities
"""
from typing import Callable


class ProgressManager:
    """Manages progress updates for different operations"""

    def __init__(self):
        self.callbacks = {}

    def register_callback(self, operation: str, callback: Callable[[int, str], None]):
        """Register a progress callback for an operation"""
        self.callbacks[operation] = callback

    def update_progress(self, operation: str, progress: int, message: str):
        """Update progress for a specific operation"""
        if operation in self.callbacks:
            self.callbacks[operation](progress, message)

    def unregister_callback(self, operation: str):
        """Remove callback for an operation"""
        if operation in self.callbacks:
            del self.callbacks[operation]
