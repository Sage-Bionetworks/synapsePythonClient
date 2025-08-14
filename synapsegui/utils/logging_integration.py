"""
Logging utilities for GUI integration
"""
import logging
import tkinter as tk
from typing import Callable, Optional


class GUILogHandler(logging.Handler):
    """
    Custom logging handler that forwards log messages to the GUI.
    
    Captures Python logging messages and forwards them to a GUI callback
    function for display in the application interface.
    """

    def __init__(self, log_callback: Callable[[str, bool], None]) -> None:
        """
        Initialize the GUI log handler.

        Args:
            log_callback: Function to call with log messages (message, is_error)
        """
        super().__init__()
        self.log_callback = log_callback
        self.root = None

    def set_root(self, root: tk.Tk) -> None:
        """
        Set the tkinter root for thread-safe GUI updates.

        Args:
            root: The main tkinter window
        """
        self.root = root

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to the GUI.

        Args:
            record: The log record to emit
        """
        try:
            message = self.format(record)
            is_error = record.levelno >= logging.ERROR

            if self.root:
                self.root.after(0, lambda: self.log_callback(message, is_error))
            else:
                self.log_callback(message, is_error)
        except Exception:
            pass


class LoggingIntegration:
    """
    Manages integration between Python logging and GUI logging.
    
    Handles setup and cleanup of logging handlers to forward Python
    log messages to the GUI output component.
    """

    def __init__(self, log_callback: Callable[[str, bool], None]) -> None:
        """
        Initialize the logging integration.

        Args:
            log_callback: Function to call with log messages (message, is_error)
        """
        self.log_callback = log_callback
        self.gui_handler: Optional[GUILogHandler] = None
        self.original_handlers = []

    def setup_logging_integration(self, root: tk.Tk) -> None:
        """
        Setup logging to forward messages to GUI.

        Args:
            root: The main tkinter window for thread-safe updates
        """
        self.gui_handler = GUILogHandler(self.log_callback)
        self.gui_handler.set_root(root)

        formatter = logging.Formatter("%(name)s: %(message)s")
        self.gui_handler.setFormatter(formatter)

        synapse_loggers = [
            "synapseclient_default",
            "synapseclient_debug", 
            "synapseclient",
            "synapseclient.client",
            "synapseclient.core",
            "synapseclient.models",
        ]

        for logger_name in synapse_loggers:
            logger = logging.getLogger(logger_name)
            self.original_handlers.extend(logger.handlers[:])
            logger.addHandler(self.gui_handler)

    def cleanup_logging_integration(self) -> None:
        """Clean up logging integration by removing GUI handlers."""
        if self.gui_handler:
            synapse_loggers = [
                "synapseclient_default",
                "synapseclient_debug",
                "synapseclient",
                "synapseclient.client", 
                "synapseclient.core",
                "synapseclient.models",
            ]

            for logger_name in synapse_loggers:
                logger = logging.getLogger(logger_name)
                if self.gui_handler in logger.handlers:
                    logger.removeHandler(self.gui_handler)

            self.gui_handler = None
