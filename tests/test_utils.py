"""Utility functions for unit/ingration tests"""

from typing import Any, Callable, Coroutine


def spy_for_async_function(
    original_func: Callable[..., Any]
) -> Callable[..., Coroutine[Any, Any, Any]]:
    """This function is used to create a spy for async functions."""

    async def wrapper(*args, **kwargs):
        return await original_func(*args, **kwargs)  # Call the original function

    return wrapper


def spy_for_function(original_func: Callable[..., Any]) -> Callable[..., Any]:
    """This function is used to create a spy for functions."""

    def wrapper(*args, **kwargs):
        return original_func(*args, **kwargs)  # Call the original function

    return wrapper
