from mock import call
from nose.tools import assert_is_instance

from multiprocessing.pool import ThreadPool
from synapseclient.pool_provider import get_pool, SingleThreadPool
import synapseclient


class TestSingleThreadPool:

    def test_map(self):
        pool = SingleThreadPool()
        pool.map(self.test_func, range(0, 10))
        self.test_func.assert_has_calls(call(x) for x in range(0, 10))


class TestPoolProvider:

    def test_single_thread(self):
        synapseclient.config.single_threaded = True
        assert_is_instance(get_pool(), SingleThreadPool)

    def test_multiple_thread(self):
        assert_is_instance(get_pool(), ThreadPool)
