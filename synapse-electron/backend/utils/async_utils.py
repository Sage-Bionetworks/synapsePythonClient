"""
Asynchronous utilities for the Synapse Desktop Client backend.

This module provides utilities for managing background tasks and
async operations without blocking the main event loop.
"""

import asyncio
import logging
import threading
from typing import Callable, Any

logger = logging.getLogger(__name__)


def run_async_task_in_background(
    async_task: Callable[[], Any],
    task_name: str = "background_task"
) -> None:
    """
    Run an async task in a background thread with its own event loop.

    This utility helps avoid blocking the main FastAPI event loop when
    running long-running tasks like file uploads/downloads.

    Args:
        async_task: The async function to run in the background
        task_name: Name for logging purposes
    """
    def run_task_in_thread() -> None:
        """Run the async task in a new event loop."""
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


async def create_async_operation_wrapper(
    operation_func: Callable[..., Any],
    *args,
    **kwargs
) -> Any:
    """
    Wrap a potentially blocking operation to run asynchronously.

    Args:
        operation_func: The function to wrap
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        The result of the operation
    """
    loop = asyncio.get_event_loop()

    # Run the operation in a thread pool to avoid blocking
    return await loop.run_in_executor(None, operation_func, *args, **kwargs)
