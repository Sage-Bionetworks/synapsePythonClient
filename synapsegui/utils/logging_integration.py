"""
Logging utilities for GUI integration
"""
import logging
import tkinter as tk
from typing import Callable, Optional


class GUILogHandler(logging.Handler):
    """Custom logging handler that forwards log messages to the GUI"""

    def __init__(self, log_callback: Callable[[str, bool], None]):
        super().__init__()
        self.log_callback = log_callback
        self.root = None

    def set_root(self, root: tk.Tk) -> None:
        """Set the tkinter root for thread-safe GUI updates"""
        self.root = root

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to the GUI"""
        try:
            message = self.format(record)
            is_error = record.levelno >= logging.ERROR

            # If we have a root widget, schedule the GUI update on the main thread
            if self.root:
                self.root.after(0, lambda: self.log_callback(message, is_error))
            else:
                # Fallback: call directly (may not be thread-safe)
                self.log_callback(message, is_error)
        except Exception:
            # Avoid recursion errors by not logging the exception
            pass


class LoggingIntegration:
    """Manages integration between Python logging and GUI logging"""

    def __init__(self, log_callback: Callable[[str, bool], None]):
        self.log_callback = log_callback
        self.gui_handler: Optional[GUILogHandler] = None
        self.original_handlers = []

    def setup_logging_integration(self, root: tk.Tk) -> None:
        """Setup logging to forward messages to GUI"""
        # Create GUI log handler
        self.gui_handler = GUILogHandler(self.log_callback)
        self.gui_handler.set_root(root)

        # Set formatter to match GUI expectations
        formatter = logging.Formatter("%(name)s: %(message)s")
        self.gui_handler.setFormatter(formatter)

        # Add the handler to the synapseclient loggers
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
            # Store original handlers for cleanup
            self.original_handlers.extend(logger.handlers[:])
            # Add our GUI handler
            logger.addHandler(self.gui_handler)

    def cleanup_logging_integration(self) -> None:
        """Clean up logging integration"""
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
