"""Shared test helpers for integration tests."""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def wait_for_condition(
    condition_fn: Callable[[], Union[Awaitable[T], T]],
    timeout_seconds: float = 60,
    poll_interval_seconds: float = 2,
    backoff_factor: float = 1.5,
    max_interval_seconds: float = 15,
    description: str = "condition",
) -> T:
    """Poll until condition_fn returns a truthy value, with exponential backoff.

    Args:
        condition_fn: A callable (sync or async) that returns a truthy value when
            the condition is met, or a falsy value to keep polling. If it raises
            an exception, polling continues until timeout.
        timeout_seconds: Maximum time to wait before raising TimeoutError.
        poll_interval_seconds: Initial interval between polls.
        backoff_factor: Multiplier applied to the interval after each poll.
        max_interval_seconds: Cap on the poll interval.
        description: Human-readable description for error messages.

    Returns:
        The truthy value returned by condition_fn.

    Raises:
        TimeoutError: If the condition is not met within timeout_seconds.
    """
    elapsed = 0.0
    interval = poll_interval_seconds
    last_exception: Optional[Exception] = None

    while elapsed < timeout_seconds:
        try:
            result = condition_fn()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                result = await result
            if result:
                return result
        except Exception as ex:
            last_exception = ex
            logger.debug(f"Polling for {description}: caught {type(ex).__name__}: {ex}")

        wait_time = min(interval, timeout_seconds - elapsed)
        if wait_time <= 0:
            break
        await asyncio.sleep(wait_time)
        elapsed += wait_time
        interval = min(interval * backoff_factor, max_interval_seconds)

    msg = f"Timed out waiting for {description} after {timeout_seconds}s"
    if last_exception:
        msg += f" (last error: {type(last_exception).__name__}: {last_exception})"
    raise TimeoutError(msg)
