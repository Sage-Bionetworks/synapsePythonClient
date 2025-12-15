"""
Logging utilities for the Synapse Desktop Client backend.

This module provides centralized logging configuration and handlers
for forwarding logs to the Electron frontend, with support for
dynamic log level control.
"""

import logging
from typing import List

from models.api_models import LogMessage

# Global message queue for logs to avoid asyncio issues
log_message_queue: List[LogMessage] = []


class ElectronLogHandler(logging.Handler):
    """
    Custom logging handler that forwards logs to Electron via message queue.

    This handler captures log messages and stores them in a queue that can be
    polled by the Electron frontend, avoiding the complexity of real-time
    WebSocket communication for log messages.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """
        Process a log record and add it to the message queue.

        Processes incoming log records, formats them appropriately,
        and adds them to the queue for frontend consumption.

        Arguments:
            record: The log record to process

        Returns:
            None

        Raises:
            Exception: Record processing errors are caught and printed to avoid recursion.
        """
        try:
            if self._should_skip_record(record):
                return

            log_data = self._create_log_message(record)
            self._add_to_queue(log_data)

        except Exception as e:
            print(f"Error in ElectronLogHandler: {e}")

    def _should_skip_record(self, record: logging.LogRecord) -> bool:
        """
        Check if a log record should be skipped.

        Determines whether certain log records should be filtered out
        to reduce noise in the frontend log viewer.

        Arguments:
            record: The log record to check

        Returns:
            bool: True if the record should be skipped

        Raises:
            None: This method does not raise exceptions.
        """
        return record.name.startswith("websockets")

    def _create_log_message(self, record: logging.LogRecord) -> LogMessage:
        """
        Create a LogMessage from a log record.

        Converts a Python logging record into a LogMessage object
        suitable for frontend consumption.

        Arguments:
            record: The log record to convert

        Returns:
            LogMessage: LogMessage object ready for the frontend

        Raises:
            Exception: Message creation errors are allowed to propagate.
        """
        message = self.format(record)

        level_mapping = {
            logging.DEBUG: "debug",
            logging.INFO: "info",
            logging.WARNING: "warning",
            logging.ERROR: "error",
            logging.CRITICAL: "critical",
        }
        level = level_mapping.get(record.levelno, "info")

        return LogMessage(
            type="log",
            message=message,
            level=level,
            logger_name=record.name,
            timestamp=record.created,
            source="python-logger",
            auto_scroll=True,
            raw_message=record.getMessage(),
            filename=getattr(record, "filename", ""),
            line_number=getattr(record, "lineno", 0),
        )

    def _add_to_queue(self, log_data: LogMessage) -> None:
        """
        Add log message to the queue with size management.

        Adds the log message to the global queue and manages queue
        size to prevent memory issues.

        Arguments:
            log_data: The log message to add

        Returns:
            None

        Raises:
            Exception: Queue management errors are caught and printed.
        """
        try:
            log_message_queue.append(log_data)

            # Keep queue size reasonable - remove old messages if it gets too large
            if len(log_message_queue) > 1000:
                log_message_queue.pop(0)

        except Exception as e:
            print(f"Error queuing log message: {e}")


def setup_logging(log_level: str = "info") -> None:
    """
    Configure logging for the application with specified log level.

    Sets up the custom ElectronLogHandler to capture all log messages
    and forward them to the frontend via the message queue.

    Arguments:
        log_level: Log level to set. Options: "debug", "info", "warning", "error"

    Returns:
        None

    Raises:
        None: Configuration errors are handled gracefully.
    """
    # Map string levels to logging constants
    level_mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }

    numeric_level = level_mapping.get(log_level.lower(), logging.INFO)

    # Create and configure the custom handler
    electron_handler = ElectronLogHandler()
    electron_handler.setLevel(
        logging.DEBUG
    )  # Handler captures all, filtering happens at logger level

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    electron_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.addHandler(electron_handler)
    root_logger.setLevel(numeric_level)

    # Set specific logger levels
    _configure_specific_loggers(numeric_level)


def _configure_specific_loggers(log_level: int) -> None:
    """
    Configure specific logger levels to reduce noise.

    Sets appropriate log levels for various third-party and internal
    loggers to maintain readability in the log output.

    Arguments:
        log_level: The numeric logging level to apply

    Returns:
        None

    Raises:
        None: This function does not raise exceptions.
    """
    # WebSocket loggers - always keep at WARNING unless debug mode
    websockets_level = logging.WARNING if log_level > logging.DEBUG else log_level
    logging.getLogger("websockets").setLevel(websockets_level)
    logging.getLogger("websockets.protocol").setLevel(websockets_level)

    # Synapse client loggers - respect the main log level
    logging.getLogger("synapseclient").setLevel(log_level)
    logging.getLogger("synapseclient.core").setLevel(log_level)

    # Third-party loggers that can be noisy
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def get_queued_messages() -> list[LogMessage]:
    """
    Get all queued log messages and clear the queue.

    Retrieves all pending log messages from the global queue and
    clears the queue for the next polling cycle.

    Arguments:
        None

    Returns:
        list[LogMessage]: List of LogMessage objects from the queue

    Raises:
        None: This function does not raise exceptions.
    """
    messages = log_message_queue.copy()
    log_message_queue.clear()
    return messages


async def initialize_logging() -> None:
    """
    Initialize logging and send startup messages.

    This function should be called during application startup to
    emit initial log messages confirming the logging system is working.

    Arguments:
        None

    Returns:
        None

    Raises:
        Exception: Initialization errors are caught and printed.
    """
    try:
        logger = logging.getLogger(__name__)
        logger.info("Synapse Backend Server logging system initialized")
    except Exception as e:
        print(f"Failed to initialize logging: {e}")
