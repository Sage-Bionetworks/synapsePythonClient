from mock import call, MagicMock
from nose.tools import assert_is_instance

from multiprocessing.pool import ThreadPool
import synapseclient


class TestSingleThreadPool:
    def setup(self):
        pass

    def test_map(self):
        test_func = MagicMock()
        pool = synapseclient.pool_provider.SingleThreadPool()
        pool.map(test_func, range(0, 10))
        test_func.assert_has_calls(call(x) for x in range(0, 10))


class TestPoolProvider:
    def setup(self):
        pass

    def test_single_thread(self):
        synapseclient.config.single_threaded = True
        assert_is_instance(synapseclient.pool_provider.get_pool(), synapseclient.pool_provider.SingleThreadPool)

    def test_multiple_thread(self):
        assert_is_instance(synapseclient.pool_provider.get_pool(), ThreadPool)
