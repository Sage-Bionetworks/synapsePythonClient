"""This utility class is to hold any utilities that are needed for async operations."""

import asyncio
import functools
from typing import Any, Callable, Coroutine, Union

import nest_asyncio
from opentelemetry import trace

tracer = trace.get_tracer("synapseclient")


def otel_trace_method(method_to_trace_name: Union[Callable[..., str], None] = None):
    """
    Decorator to trace a method with OpenTelemetry in an async environment. This function
    is specifically written to be used on a method within a class.

    This will pass the class instance as the first argument to the method. This allows
    you to modify the name of the trace to include information about the class instance.

    Example: Decorating a method within a class that will be traced with OpenTelemetry.
        Setting the trace name:

            @otel_trace_method(method_to_trace_name=lambda self, **kwargs: f"Project_Store: {self.name}")
            async def store(self):

    Arguments:
        method_to_trace_name: A callable that takes the class instance as the first argument
            and returns a string to be used as the trace name. If this is not provided,
            the trace name will be set to the method name.

    Returns:
        A callable decorator that will trace the method with OpenTelemetry.
    """

    def decorator(func):
        """Function decorator."""

        async def otel_trace_method_wrapper(self, *arg, **kwargs) -> None:
            """Wrapper for the function to be traced."""
            trace_name = (
                method_to_trace_name(self, *arg, **kwargs)
                if method_to_trace_name
                else None
            )
            current_span = trace.get_current_span()
            if current_span.is_recording():
                with tracer.start_as_current_span(
                    trace_name or f"Synaspse::{func.__name__}"
                ):
                    return await func(self, *arg, **kwargs)
            else:
                return await func(self, *arg, **kwargs)

        return otel_trace_method_wrapper

    return decorator


class ClassOrInstance:
    """Helper class to allow a method to be called as a class method or instance method."""

    def __init__(self, fn):
        self.fn = fn

    def __get__(self, obj, cls):
        def f(*args, **kwds):
            if obj is not None:
                return self.fn(obj, *args, **kwds)
            else:
                return self.fn(cls, *args, **kwds)

        functools.update_wrapper(f, self.fn)
        return f


def wrap_async_to_sync(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """Wrap an async function to be called in a sync context."""
    loop = None

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        pass

    if loop:
        nest_asyncio.apply(loop=loop)
        return loop.run_until_complete(coroutine)
    else:
        return asyncio.run(coroutine)


def wrap_async_generator_to_sync_generator(async_gen_func: Callable, *args, **kwargs):
    """
    Wrap an async generator function to be called in a sync context, returning a sync generator.

    This function takes an async generator function and its arguments, then yields items
    synchronously by running the async generator in the appropriate event loop.

    Arguments:
        async_gen_func: The async generator function to wrap
        *args: Positional arguments to pass to the async generator function
        **kwargs: Keyword arguments to pass to the async generator function

    Yields:
        Items from the async generator, yielded synchronously
    """
    loop = None

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        pass

    if loop:
        nest_asyncio.apply(loop=loop)

        # Create the async generator
        async_gen = async_gen_func(*args, **kwargs)

        # Yield items from the async generator synchronously
        try:
            while True:
                try:
                    item = loop.run_until_complete(async_gen.__anext__())
                    yield item
                except StopAsyncIteration:
                    break
        finally:
            # Ensure the generator is properly closed
            try:
                loop.run_until_complete(async_gen.aclose())
            except (RuntimeError, StopAsyncIteration):
                pass
    else:
        # No running loop, create a new one
        async def run_generator():
            async_gen = async_gen_func(*args, **kwargs)
            items = []
            try:
                async for item in async_gen:
                    items.append(item)
            finally:
                try:
                    await async_gen.aclose()
                except (RuntimeError, StopAsyncIteration):
                    pass
            return items

        items = asyncio.run(run_generator())
        for item in items:
            yield item


# Adapted from
# https://github.com/keflavich/astroquery/blob/30deafc3aa057916bcdca70733cba748f1b36b64/astroquery/utils/process_asyncs.py#L11
def async_to_sync(cls):
    """
    Convert all name_of_thing_async methods to name_of_thing methods

    (see
    http://stackoverflow.com/questions/18048341/add-methods-to-a-class-generated-from-other-methods
    for help understanding)
    """

    def create_method(async_method_name: str):
        """Creates a replacement method for the async method."""

        @ClassOrInstance
        def newmethod(self, *args, **kwargs):
            """The new method that will replace the non-async method."""

            async def wrapper(*args, **kwargs):
                """Wrapper for the function to be called in an async context."""
                return await getattr(self, async_method_name)(*args, **kwargs)

            loop = None

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            if loop:
                nest_asyncio.apply(loop=loop)
                return loop.run_until_complete(wrapper(*args, **kwargs))
            else:
                return asyncio.run(wrapper(*args, **kwargs))

        return newmethod

    methods = cls.__dict__.keys()

    methods_to_update = []
    for k in methods:
        if getattr(cls.__dict__[k], "_skip_conversion", False):
            continue

        if "async" in k and (new_method_name := k.replace("_async", "")):
            new_method = create_method(k)
            new_method.fn.__name__ = new_method_name
            new_method.__name__ = new_method_name

            functools.update_wrapper(new_method, new_method.fn)
            methods_to_update.append(
                {
                    "new_method_name": new_method_name,
                    "new_method": new_method,
                }
            )
    for method_to_update in methods_to_update:
        setattr(
            cls, method_to_update["new_method_name"], method_to_update["new_method"]
        )

    return cls


def skip_async_to_sync(func):
    """Decorator to skip the async to sync conversion for a specific function."""
    func._skip_conversion = True
    return func
