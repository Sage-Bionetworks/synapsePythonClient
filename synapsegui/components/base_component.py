"""
Base component class for UI components
"""
import tkinter as tk
from tkinter import ttk
from typing import Optional


class BaseComponent:
    """Base class for UI components"""

    def __init__(self, parent: tk.Widget, **kwargs):
        self.parent = parent
        self.frame: Optional[ttk.Frame] = None

    def create_ui(self) -> None:
        """Create the UI elements - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement create_ui()")

    def get_frame(self) -> tk.Widget:
        """Get the main frame for this component"""
        if self.frame is None:
            raise ValueError("Frame not initialized - call create_ui() first")
        return self.frame

    def set_enabled(self, enabled: bool) -> None:
        """Enable/disable component - default implementation"""
        state = "normal" if enabled else "disabled"
        if self.frame:
            for child in self.frame.winfo_children():
                if hasattr(child, "config"):
                    try:
                        child.config(state=state)
                    except tk.TclError:
                        pass  # Some widgets don't support state changes

    def destroy(self) -> None:
        """Clean up component resources"""
        if self.frame:
            self.frame.destroy()
            self.frame = None
