"""
Utilities package for Synapse Desktop Client backend.

This package contains utility functions and helper classes used
throughout the application.
"""

from .async_utils import create_async_operation_wrapper, run_async_task_in_background
from .logging_utils import get_queued_messages, initialize_logging, setup_logging
from .system_utils import (
    get_home_and_downloads_directories,
    scan_directory_for_files,
    setup_electron_environment,
)
from .websocket_utils import (
    broadcast_message,
    handle_websocket_client,
    start_websocket_server,
)

__all__ = [
    # Logging utilities
    "setup_logging",
    "get_queued_messages",
    "initialize_logging",
    # WebSocket utilities
    "handle_websocket_client",
    "broadcast_message",
    "start_websocket_server",
    # System utilities
    "setup_electron_environment",
    "get_home_and_downloads_directories",
    "scan_directory_for_files",
    # Async utilities
    "run_async_task_in_background",
    "create_async_operation_wrapper",
]
