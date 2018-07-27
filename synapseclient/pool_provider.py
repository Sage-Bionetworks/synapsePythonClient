"""

A thin wrapper around multiprocessing.dummy that allows users to use single thread. This module allow us to simplify the
logic of any method that can run with either multiple threads or single thread.

To use this wrapper with multiple threads::
    pool = PoolProvider()
    try:
        pool.map(function, iterable)
    finally:
        pool.terminate()

To use this wrapper for single thread::
    pool = PoolProvider(single_threaded=True)

"""
from multiprocessing.dummy import Pool


DEFAULT_POOL_SIZE = 8


class SingleThreadPool:

    def __init__(self):
        pass

    def map(self, func, iterable):
        for item in iterable:
            func(item)

    def terminate(self):
        pass


def PoolProvider(single_threaded=False):
    if single_threaded:
        return SingleThreadPool()
    else:
        return Pool(DEFAULT_POOL_SIZE)
