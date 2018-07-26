from mock import MagicMock, call
from nose.tools import assert_is_none, assert_false, assert_true, assert_is_not_none

from synapseclient.multiprocessing_wrapper import MultiprocessingWrapper


class TestMultiprocessingWrapper(object):

    def setup(self):
        self.test_func = MagicMock()

    def test_init(self):
        wrapper = MultiprocessingWrapper()
        assert_is_none(wrapper._pool)
        assert_false(wrapper._with_single_thread)

    def test_init_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        assert_is_none(wrapper._pool)
        assert_true(wrapper._with_single_thread)

    def test_init_pool(self):
        wrapper = MultiprocessingWrapper()
        assert_is_none(wrapper._pool)
        try:
            wrapper._init_pool()
            assert_is_not_none(wrapper._pool)
        finally:
            wrapper.terminate()

    def test_init_pool_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        assert_is_none(wrapper._pool)
        wrapper._init_pool()
        assert_is_none(wrapper._pool)

    def test_run_with_single_thread(self):
        wrapper = MultiprocessingWrapper(with_single_thread=True)
        wrapper.run(self.test_func, range(0, 10))
        assert_is_none(wrapper._pool)
        self.test_func.assert_has_calls(call(x) for x in range(0, 10))

    def test_run(self):
        wrapper = MultiprocessingWrapper()
        try:
            wrapper.run(self.test_func, range(0, 10))
            assert_is_not_none(wrapper._pool)
            self.test_func.assert_has_calls(call(x) for x in range(0, 10))
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
