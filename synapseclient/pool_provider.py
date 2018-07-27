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
import synapseclient.config


DEFAULT_POOL_SIZE = 8


class SingleThreadPool:

    def map(self, func, iterable):
        for item in iterable:
            func(item)

    def terminate(self):
        pass


def get_pool():
    if synapseclient.config.single_threaded:
        return SingleThreadPool()
    else:
        return Pool(DEFAULT_POOL_SIZE)
