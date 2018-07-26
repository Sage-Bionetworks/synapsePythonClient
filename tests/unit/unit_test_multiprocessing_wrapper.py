from unittest.mock import MagicMock
from nose.tools import assert_is_none, assert_false, assert_true, assert_is_not_none

from synapseclient.multiprocessing_wrapper import MultiprocessingWrapper, DEFAULT_POOL_SIZE


class TestMultiprocessingWrapper(object):

    def setup(self):
        self.mock_pool = MagicMock()
        self.test_func = MagicMock()

    def test_init(self):
        wrapper = MultiprocessingWrapper()
        assert_is_none(wrapper._pool)
        assert_false(wrapper._is_single_thread)
        self.mock_pool.assert_never_called()

    def test_init_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        assert_is_none(wrapper._pool)
        assert_true(wrapper._is_single_thread)
        self.mock_pool.assert_never_called()

    def test_init_pool(self):
        wrapper = MultiprocessingWrapper()
        assert_is_none(wrapper._pool)
        try:
            wrapper._init_pool()
            self.mock_pool.assert_called_once_with(DEFAULT_POOL_SIZE)
        finally:
            wrapper.terminate()

    def test_init_pool_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        assert_is_none(wrapper._pool)
        wrapper._init_pool()
        self.mock_pool.assert_never_called()

    def test_run_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        wrapper.run(self.test_func, range(0, 10))
        self.mock_pool.assert_never_called()
        self.test_func.assert_called_with(range(0, 10))

    def test_run(self):
        wrapper = MultiprocessingWrapper()
        try:
            wrapper.run(self.test_func, range(0, 10))
            self.mock_pool.assert_called_once_with(DEFAULT_POOL_SIZE)
            self.test_func.assert_called_with(range(0, 10))
        finally:
            wrapper.terminate()

    def test_terminate(self):
        wrapper = MultiprocessingWrapper()
        assert_is_none(wrapper._pool)
        try:
            wrapper.run(self.test_func, range(0, 10))
            assert_is_not_none(wrapper._pool)
        finally:
            wrapper.terminate()
            assert_is_none(wrapper._pool)
