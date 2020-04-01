"""
This module provides access to thread pools to use in concurrent work.
Both a multiprocessing style ThreadPool and an Executor based pool
are available, Executors should be preferred for new work as it provides
a more modern interface.

To use these wrappers for single thread environment, set the following:

    synapseclient.config.single_threaded = True
"""

from concurrent.futures import Executor, Future, ThreadPoolExecutor
import multiprocessing
import multiprocessing.dummy

from . import config

# +4 matches ThreadPoolExecutor, at least 5 threads for I/O bound tasks
# https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
DEFAULT_NUM_THREADS = multiprocessing.cpu_count() + 4


class SingleThreadPool:

    def map(self, func, iterable):
        for item in iterable:
            func(item)

    def terminate(self):
        pass


class SingleThreadExecutor(Executor):
    def __init__(self):
        self._shutdown = False

    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')

        f = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)

        return f


class FakeLock:

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass


class SingleValue:

    value = None

    def __init__(self, type, value):
        self.value = value

    def get_lock(self):
        return FakeLock()


def get_pool():
    if config.single_threaded:
        return SingleThreadPool()
    else:
        return multiprocessing.dummy.Pool(DEFAULT_NUM_THREADS)


def get_executor(thread_count=DEFAULT_NUM_THREADS):
    """
    Provides an Executor as defined by the client config suitable
    for running tasks work as defined by the client config.

    :param thread_count:        number of concurrent threads

    :return: an Executor
    """
    if config.single_threaded:
        return SingleThreadExecutor()
    else:
        return ThreadPoolExecutor(max_workers=thread_count)


def get_value(type, value):
    if config.single_threaded:
        return SingleValue(type, value)
    else:
        return multiprocessing.Value(type, value)
