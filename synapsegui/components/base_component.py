"""
Base component class for UI components
"""
import tkinter as tk
from tkinter import ttk
from typing import Optional


class BaseComponent:
    """
    Base class for UI components.
    
    Provides common functionality for UI component management including
    frame creation, enabling/disabling, and cleanup operations.
    """

    def __init__(self, parent: tk.Widget, **kwargs) -> None:
        """
        Initialize the base component.

        Args:
            parent: Parent widget to contain this component
            **kwargs: Additional keyword arguments
        """
        self.parent = parent
        self.frame: Optional[ttk.Frame] = None

    def create_ui(self) -> None:
        """
        Create the UI elements for this component.
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement create_ui()")

    def get_frame(self) -> tk.Widget:
        """
        Get the main frame for this component.
        
        Returns:
            The main frame widget
            
        Raises:
            ValueError: If frame not initialized
        """
        if self.frame is None:
            raise ValueError("Frame not initialized - call create_ui() first")
        return self.frame

    def set_enabled(self, enabled: bool) -> None:
        """
        Enable or disable the component and all its child widgets.
        
        Args:
            enabled: True to enable, False to disable
        """
        state = "normal" if enabled else "disabled"
        if self.frame:
            for child in self.frame.winfo_children():
                if hasattr(child, "config"):
                    try:
                        child.config(state=state)
                    except tk.TclError:
                        pass

    def destroy(self) -> None:
        """Clean up component resources and destroy the frame."""
        if self.frame:
            self.frame.destroy()
            self.frame = None
