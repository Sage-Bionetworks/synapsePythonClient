"""Unit tests for the async_to_sync decorator and related utilities."""

import asyncio
import sys
from unittest.mock import MagicMock

import pytest

from synapseclient.core.async_utils import (
    ClassOrInstance,
    async_to_sync,
    skip_async_to_sync,
    wrap_async_to_sync,
)


class TestAsyncToSyncDecorator:
    """Tests for the @async_to_sync class decorator."""

    def test_creates_sync_methods_from_async(self):
        """Verify the decorator generates sync methods for each _async method."""

        @async_to_sync
        class MyClass:
            async def store_async(self):
                return "stored"

            async def get_async(self, item_id):
                return f"got {item_id}"

            async def delete_async(self):
                return "deleted"

        obj = MyClass()
        assert hasattr(obj, "store")
        assert hasattr(obj, "get")
        assert hasattr(obj, "delete")

    def test_sync_method_returns_correct_value(self):
        """Verify sync wrappers propagate return values."""

        @async_to_sync
        class MyClass:
            async def compute_async(self, x, y):
                return x + y

        obj = MyClass()
        result = obj.compute(3, 4)
        assert result == 7

    def test_sync_method_propagates_exceptions(self):
        """Verify sync wrappers propagate exceptions from the async method."""

        @async_to_sync
        class MyClass:
            async def fail_async(self):
                raise ValueError("test error")

        obj = MyClass()
        with pytest.raises(ValueError, match="test error"):
            obj.fail()

    def test_sync_method_passes_args_and_kwargs(self):
        """Verify arguments are forwarded correctly."""

        @async_to_sync
        class MyClass:
            async def method_async(self, a, b, *, key=None):
                return (a, b, key)

        obj = MyClass()
        result = obj.method(1, 2, key="value")
        assert result == (1, 2, "value")

    def test_async_methods_still_work(self):
        """Verify original async methods are not broken by the decorator."""

        @async_to_sync
        class MyClass:
            async def store_async(self):
                return "async_stored"

        obj = MyClass()
        result = asyncio.run(obj.store_async())
        assert result == "async_stored"

    def test_skip_async_to_sync_excludes_method(self):
        """Verify @skip_async_to_sync prevents sync wrapper generation."""

        @async_to_sync
        class MyClass:
            @skip_async_to_sync
            async def internal_async(self):
                return "internal"

            async def public_async(self):
                return "public"

        obj = MyClass()
        assert not hasattr(obj, "internal")
        assert hasattr(obj, "public")

    def test_class_or_instance_method(self):
        """Verify sync wrappers can be called as both class and instance methods."""

        @async_to_sync
        class MyClass:
            async def store_async(self):
                return "stored"

        obj = MyClass()
        # Instance method call should work
        result = obj.store()
        assert result == "stored"

    def test_decorator_preserves_class_attributes(self):
        """Verify the decorator doesn't break non-async class attributes."""

        @async_to_sync
        class MyClass:
            class_var = "hello"

            def sync_method(self):
                return "sync"

            async def async_method_async(self):
                return "async"

        obj = MyClass()
        assert MyClass.class_var == "hello"
        assert obj.sync_method() == "sync"
        assert obj.async_method() == "async"

    def test_none_return_value(self):
        """Verify None return values are handled."""

        @async_to_sync
        class MyClass:
            async def void_async(self):
                pass

        obj = MyClass()
        result = obj.void()
        assert result is None


class TestWrapAsyncToSync:
    """Tests for the wrap_async_to_sync utility function."""

    def test_wraps_coroutine_to_sync(self):
        """Verify a coroutine can be run synchronously."""

        async def my_coro():
            return 42

        result = wrap_async_to_sync(my_coro())
        assert result == 42

    def test_wraps_coroutine_with_exception(self):
        """Verify exceptions from coroutines propagate."""

        async def my_coro():
            raise RuntimeError("async error")

        with pytest.raises(RuntimeError, match="async error"):
            wrap_async_to_sync(my_coro())


class TestPython314Compatibility:
    """Tests for Python 3.14+ behavior where sync wrappers raise RuntimeError
    when an event loop is already active."""

    @pytest.mark.skipif(
        sys.version_info < (3, 14),
        reason="Only applicable on Python 3.14+",
    )
    def test_wrap_async_to_sync_raises_with_active_loop_on_314(self):
        """On Python 3.14+, wrap_async_to_sync should raise RuntimeError
        when called from within an active event loop."""

        async def _inner():
            async def my_coro():
                return 42

            # This should raise because we're inside an active event loop
            wrap_async_to_sync(my_coro())

        with pytest.raises(RuntimeError, match="Python 3.14\\+"):
            asyncio.run(_inner())

    @pytest.mark.skipif(
        sys.version_info < (3, 14),
        reason="Only applicable on Python 3.14+",
    )
    def test_async_to_sync_decorator_raises_with_active_loop_on_314(self):
        """On Python 3.14+, sync methods generated by @async_to_sync should
        raise RuntimeError when called from within an active event loop."""

        @async_to_sync
        class MyClass:
            async def store_async(self):
                return "stored"

        async def _inner():
            obj = MyClass()
            obj.store()

        with pytest.raises(RuntimeError, match="Python 3.14\\+"):
            asyncio.run(_inner())

    @pytest.mark.skipif(
        sys.version_info >= (3, 14),
        reason="Only applicable on Python < 3.14",
    )
    def test_sync_methods_work_without_active_loop_pre_314(self):
        """On Python < 3.14, sync wrappers should work normally
        when no event loop is active."""

        @async_to_sync
        class MyClass:
            async def store_async(self):
                return "stored"

        obj = MyClass()
        result = obj.store()
        assert result == "stored"
