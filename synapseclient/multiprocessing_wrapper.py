"""

A thin wrapper around multiprocessing.dummy that allows users to use single thread. This module allow us to simplify the
logic of any method that can run with either multiple threads or single thread.

To use this wrapper with multiple threads::
    mp = MultiprocessingWrapper()
    try:
        mp.run(function, iterable)
    finally:
        mp.terminate()

To use this wrapper for single thread::
    mp = MultiprocessingWrapper(single_thread = True)
    mp.run(function, iterable)

"""
from multiprocessing.dummy import Pool


DEFAULT_POOL_SIZE = 8


class MultiprocessingWrapper():
    _pool = None
    _with_single_thread = None

    def __init__(self, with_single_thread=False):
        self._with_single_thread = with_single_thread

    def _init_poll(self):
        if not self._with_single_thread and not self._pool:
            self._pool = Pool(DEFAULT_POOL_SIZE)

    def run(self, func, iterable):
        if self._with_single_thread:
            for item in iterable:
                func(item)
        else:
            if not self._pool:
                self._init_poll()
            self._pool.map(func, iterable)

    def terminate(self):
        if self._pool:
            self._pool.terminate()
            self._pool = None
