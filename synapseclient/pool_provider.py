"""
This module allow us to simplify the logic of any method that can run with either multiple threads or single thread.

To use this wrapper with multiple threads::
    pool = pool_provider.get_pool()
    try:
        pool.map(function, iterable)
    finally:
        pool.terminate()

To use this wrapper for single thread, change the synapseclient.config.single_threaded::
    synapseclient.config.single_threaded = True

"""
from multiprocessing.dummy import Pool
from . import config


DEFAULT_POOL_SIZE = 8


class SingleThreadPool:

    def map(self, func, iterable):
        for item in iterable:
            func(item)

    def terminate(self):
        pass


def get_pool():
    if config.single_threaded:
        return SingleThreadPool()
    else:
        return Pool(DEFAULT_POOL_SIZE)
