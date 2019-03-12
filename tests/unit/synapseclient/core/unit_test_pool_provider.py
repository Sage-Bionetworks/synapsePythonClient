from mock import call, MagicMock
from nose.tools import assert_is_instance, assert_equal, assert_false
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.pool import ThreadPool

import synapseclient
from synapseclient.core.pool_provider import SingleThreadPool, SingleValue, get_pool, get_value


class TestSingleThreadPool:
    def setup(self):
        pass

    def test_map(self):
        test_func = MagicMock()
        pool = SingleThreadPool()
        pool.map(test_func, range(0, 10))
        test_func.assert_has_calls(call(x) for x in range(0, 10))


class TestPoolProvider:
    def setup(self):
        pass

    def test_get_pool_for_single_thread(self):
        synapseclient.core.config.single_threaded = True
        assert_is_instance(get_pool(), SingleThreadPool)

    def test_get_pool_for_multiple_thread(self):
        synapseclient.core.config.single_threaded = False
        assert_is_instance(get_pool(), ThreadPool)


class TestGetValue:
    def setup(self):
        pass

    def test_get_value_for_multiple_thread(self):
        synapseclient.core.config.single_threaded = False
        test_value = get_value('d', 500)
        type(test_value)
        assert_is_instance(test_value, Synchronized)
        assert_equal(test_value.value, 500)

        test_value.get_lock()
        test_value.value = 900
        assert_equal(test_value.value, 900)

    def test_get_value_for_single_thread(self):
        synapseclient.core.config.single_threaded = True
        test_value = get_value('d', 500)
        assert_is_instance(test_value, SingleValue)
        test_value.value
        assert_equal(test_value.value, 500)

        test_value.get_lock()
        test_value.value = 900
        assert_equal(test_value.value, 900)

