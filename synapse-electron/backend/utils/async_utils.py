"""
Asynchronous utilities for the Synapse Desktop Client backend.

This module provides utilities for managing background tasks and
async operations without blocking the main event loop.
"""

import asyncio
import logging
import threading
from typing import Any, Callable

logger = logging.getLogger(__name__)


def run_async_task_in_background(
    async_task: Callable[[], Any], task_name: str = "background_task"
) -> None:
    """
    Run an async task in a background thread with its own event loop.

    This utility helps avoid blocking the main FastAPI event loop when
    running long-running tasks like file uploads/downloads.

    Arguments:
        async_task: The async function to run in the background
        task_name: Name for logging purposes

    Returns:
        None

    Raises:
        Exception: Background task errors are logged but not propagated to caller.
    """

    def run_task_in_thread() -> None:
        """
        Run the async task in a new event loop.

        Creates a new event loop for the background thread and executes
        the async task within it.

        Arguments:
            None

        Returns:
            None

        Raises:
            Exception: Task execution errors are logged and handled gracefully.
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(async_task())
        except Exception as e:
            logger.error("Background task %s error: %s", task_name, e)
        finally:
            loop.close()

    thread = threading.Thread(target=run_task_in_thread, name=task_name)
    thread.daemon = True
    thread.start()

    logger.info("Started background task: %s", task_name)
