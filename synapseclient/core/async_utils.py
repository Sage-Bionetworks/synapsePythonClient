"""This utility class is to hold any utilities that are needed for async operations."""

import asyncio
from typing import Callable, Union
from opentelemetry import trace
import functools
import nest_asyncio


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
            with tracer.start_as_current_span(
                trace_name or f"Synaspse::{func.__name__}"
            ):
                return await func(self, *arg, **kwargs)

        return otel_trace_method_wrapper

    return decorator


class class_or_instance:
    def __init__(self, fn):
        self.fn = fn

        # if hasattr(fn, "__doc__"):
        #     self.__doc__ = fn.__doc__
        # else:
        #     self.__doc__ = ""

    def __get__(self, obj, cls):
        def f(*args, **kwds):
            if obj is not None:
                return self.fn(obj, *args, **kwds)
            else:
                return self.fn(cls, *args, **kwds)

        functools.update_wrapper(f, self.fn)
        return f


# Adapted from https://github.com/keflavich/astroquery/blob/30deafc3aa057916bcdca70733cba748f1b36b64/astroquery/utils/process_asyncs.py#L11
def async_to_sync(cls):
    """
    Convert all name_of_thing_async methods to name_of_thing methods

    (see
    http://stackoverflow.com/questions/18048341/add-methods-to-a-class-generated-from-other-methods
    for help understanding)
    """

    def create_method(async_method_name):
        @class_or_instance
        def newmethod(self, *args, **kwargs):
            async def wrapper(*args, **kwargs):
                """Wrapper for the function to be called in an async context."""
                return await getattr(self, async_method_name)(*args, **kwargs)

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(wrapper(*args, **kwargs))
            else:
                nest_asyncio.apply(loop=loop)
                return loop.run_until_complete(wrapper(*args, **kwargs))

        return newmethod

    methods = cls.__dict__.keys()

    stuff_to_update_list = []
    for k in methods:
        newmethodname = k.replace("_async", "")
        if "async" in k and newmethodname not in methods:
            newmethod = create_method(k)

            newmethod.fn.__name__ = newmethodname
            newmethod.__name__ = newmethodname

            functools.update_wrapper(newmethod, newmethod.fn)
            stuff_to_update_list.append(
                {
                    "newmethodname": newmethodname,
                    "newmethod": newmethod,
                }
            )
    for stuff_to_update in stuff_to_update_list:
        setattr(cls, stuff_to_update["newmethodname"], stuff_to_update["newmethod"])

    return cls
