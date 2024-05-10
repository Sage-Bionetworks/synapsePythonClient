import functools
import sys
import traceback
from multiprocessing import Lock, Value
from typing import TYPE_CHECKING

from synapseclient.core.utils import printTransferProgress

if TYPE_CHECKING:
    from synapseclient import Synapse


def notifyMe(syn: "Synapse", messageSubject: str = "", retries: int = 0):
    """Function decorator that notifies you via email whenever an function completes running or there is a failure.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        messageSubject: A string with subject line for sent out messages.
        retries: Number of retries to attempt on failure

    Example: Using this function
        As a decorator:

            # to decorate a function that you define
            from synapseutils import notifyMe
            import synapseclient
            syn = synapseclient.login()

            @notifyMe(syn, 'Long running function', retries=2)
            def my_function(x):
                doing_something()
                return long_runtime_func(x)

            my_function(123)

        Wrapping a function:

            # to wrap a function that already exists
            from synapseutils import notifyMe
            import synapseclient
            syn = synapseclient.login()

            notify_decorator = notifyMe(syn, 'Long running query', retries=2)
            my_query = notify_decorator(syn.tableQuery)
            results = my_query("select id from syn1223")
    """

    def notify_decorator(func):
        @functools.wraps(func)
        def with_retry_and_messaging(*args, **kwargs):
            attempt = 0
            destination = syn.getUserProfile()["ownerId"]
            while attempt <= retries:
                try:
                    output = func(*args, **kwargs)
                    syn.sendMessage(
                        [destination],
                        messageSubject,
                        messageBody="Call to %s completed successfully!"
                        % func.__name__,
                    )
                    return output
                except Exception as e:
                    sys.stderr.write(traceback.format_exc())
                    syn.sendMessage(
                        [destination],
                        messageSubject,
                        messageBody=(
                            "Encountered a temporary Failure during upload.  "
                            "Will retry %i more times. \n\n Error message was:\n%s\n\n%s"
                            % (retries - attempt, e, traceback.format_exc())
                        ),
                    )
                    attempt += 1

        return with_retry_and_messaging

    return notify_decorator


def notify_me_async(syn: "Synapse", messageSubject: str = "", retries: int = 0):
    """Function decorator that notifies you via email whenever an function completes
    running or there is a failure. This version of the function is callable within
    an async context.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        messageSubject: A string with subject line for sent out messages.
        retries: Number of retries to attempt on failure

    Example: Using this function
        As a decorator:

            # to decorate a function that you define
            from synapseutils import notify_me_async
            import asyncio
            import synapseclient
            syn = synapseclient.login()

            async def doing_something_async():
                await asyncio.sleep(5)

            @notify_me_async(syn, 'Long running function', retries=2)
            async def my_function():
                return await doing_something_async()

            asyncio.run(my_function())

        Wrapping a function:

            # to wrap a function that already exists
            from synapseutils import notify_me_async
            import asyncio
            import synapseclient
            syn = synapseclient.login()

            async def doing_something_async(data):
                print(f"Doing something async with {data}")
                await asyncio.sleep(5)
                print(f"Did something async with {data}")

            notify_decorator = notify_me_async(syn, 'Long running function', retries=2)
            my_query_async = notify_decorator(doing_something_async)
            results = asyncio.run(my_query_async("select id from syn1223"))
    """

    def notify_decorator(func):
        @functools.wraps(func)
        async def with_retry_and_messaging(*args, **kwargs):
            attempt = 0
            destination = syn.getUserProfile()["ownerId"]
            while attempt <= retries:
                try:
                    output = await func(*args, **kwargs)
                    syn.sendMessage(
                        [destination],
                        messageSubject,
                        messageBody="Call to %s completed successfully!"
                        % func.__name__,
                    )
                    return output
                except Exception as e:
                    sys.stderr.write(traceback.format_exc())
                    syn.sendMessage(
                        [destination],
                        messageSubject,
                        messageBody=(
                            "Encountered a temporary Failure during upload.  "
                            "Will retry %i more times. \n\n Error message was:\n%s\n\n%s"
                            % (retries - attempt, e, traceback.format_exc())
                        ),
                    )
                    attempt += 1

        return with_retry_and_messaging

    return notify_decorator


def with_progress_bar(func, totalCalls, prefix="", postfix="", isBytes=False):
    """Wraps a function to add a progress bar based on the number of calls to that function.

    Arguments:
        func: Function being wrapped with progress Bar
        totalCalls: total number of items/bytes when completed
        prefix: String printed before progress bar
        prefix: String printed after progress bar
        isBytes: A boolean indicating weather to convert bytes to kB, MB, GB etc.

    Returns:
        A wrapped function that contains a progress bar
    """
    completed = Value("d", 0)
    lock = Lock()

    def progress(*args, **kwargs):
        with lock:
            completed.value += 1
        printTransferProgress(completed.value, totalCalls, prefix, postfix, isBytes)
        return func(*args, **kwargs)

    return progress
