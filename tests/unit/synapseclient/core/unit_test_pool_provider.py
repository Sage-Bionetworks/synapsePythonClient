from concurrent.futures import ThreadPoolExecutor
from unittest.mock import call, MagicMock, patch, PropertyMock
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.pool import ThreadPool

import synapseclient
from synapseclient.core.pool_provider import (
    SingleThreadExecutor,
    SingleThreadPool,
    SingleValue,
    get_executor,
    get_pool,
    get_value,
)


class TestSingleThreadPool:
    def setup(self):
        pass

    def test_map(self):
        test_func = MagicMock()
        pool = SingleThreadPool()
        pool.map(test_func, range(0, 10))
        test_func.assert_has_calls(call(x) for x in range(0, 10))


class TestSingleThreadExecutor:
    def test_execution(self):
        executor = SingleThreadExecutor()
        for i in range(10):
            result = executor.submit(lambda: i)

            assert result.done
            assert i == result.result()


def _patch_config(single_threaded: bool):
    return patch.object(
        synapseclient.core.config,
        "single_threaded",
        single_threaded,
    )


class TestExecutorProvider:
    def test_get_executor_for_single_thread(self):
        with _patch_config(True):
            assert isinstance(get_executor(), SingleThreadExecutor)

    def test_get_executor_for_multiple_thread(self):
        with _patch_config(False):
            assert isinstance(get_executor(), ThreadPoolExecutor)


class TestPoolProvider:
    def test_get_pool_for_single_thread(self):
        with _patch_config(True):
            assert isinstance(get_pool(), SingleThreadPool)

    def test_get_pool_for_multiple_thread(self):
        with _patch_config(False):
            assert isinstance(get_pool(), ThreadPool)


class TestGetValue:
    @patch.object(
        synapseclient.core.config, "single_threaded", PropertyMock(return_value=False)
    )
    def test_get_value_for_multiple_thread(self):
        synapseclient.core.config.single_threaded = False
        test_value = get_value("d", 500)
        type(test_value)
        assert isinstance(test_value, Synchronized)
        assert test_value.value == 500

        test_value.get_lock()
        test_value.value = 900
        assert test_value.value == 900

    @patch.object(
        synapseclient.core.config, "single_threaded", PropertyMock(return_value=True)
    )
    def test_get_value_for_single_thread(self):
        synapseclient.core.config.single_threaded = True
        test_value = get_value("d", 500)
        assert isinstance(test_value, SingleValue)
        test_value.value
        assert test_value.value == 500

        test_value.get_lock()
        test_value.value = 900
        assert test_value.value == 900
