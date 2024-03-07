from unittest.mock import AsyncMock, MagicMock

import pytest
from requests import Response

from synapseclient.core.exceptions import SynapseError
from synapseclient.core.retry import with_retry, with_retry_async


def test_with_retry():
    retryParams = {"retries": 3, "wait": 0}
    response = MagicMock()
    function = MagicMock()
    function.return_value = response

    # -- No failures --
    response.status_code.__eq__.side_effect = lambda x: x == 250
    with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1

    # -- Always fail --
    response.status_code.__eq__.side_effect = lambda x: x == 503
    with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4

    # -- Fail then succeed --
    thirdTimes = [3, 2, 1]

    def theCharm(x):
        if x == 503:
            count = thirdTimes.pop()
            return count != 3
        return x == 503

    response.status_code.__eq__.side_effect = theCharm
    with_retry(function, verbose=True, **retryParams)
    assert function.call_count == 1 + 4 + 3

    # -- Retry with an error message --
    retryErrorMessages = ["Foo"]
    retryParams["retry_errors"] = retryErrorMessages
    response.status_code.__eq__.side_effect = lambda x: x == 500
    response.headers.__contains__.reset_mock()
    response.headers.__contains__.side_effect = lambda x: x == "content-type"
    response.headers.get.side_effect = lambda x, default_value: (
        "application/json" if x == "content-type" else None
    )
    response.json.return_value = {"reason": retryErrorMessages[0]}
    with_retry(function, **retryParams)
    assert response.headers.get.called
    assert function.call_count == 1 + 4 + 3 + 4

    # -- Propagate an error up --
    print("Expect a SynapseError: Bar")

    def foo():
        raise SynapseError("Bar")

    function.side_effect = foo
    pytest.raises(SynapseError, with_retry, function, **retryParams)
    assert function.call_count == 1 + 4 + 3 + 4 + 1


@pytest.mark.parametrize(
    "values",
    (
        None,
        [],
        tuple(),
    ),
)
def test_with_retry__empty_status_codes(values):
    """Verify that passing some Falsey values for the various sequence args is ok"""
    response = MagicMock(spec=Response)
    response.status_code = 200

    fn = MagicMock()
    fn.return_value = response

    # no unexpected exceptions etc should be raised
    returned_response = with_retry(
        fn,
        retry_status_codes=values,
        expected_status_codes=values,
        retry_exceptions=values,
        retry_errors=values,
    )
    assert returned_response == response


def test_with_retry__expected_status_code():
    """Verify using retry expected_status_codes"""

    non_matching_response = MagicMock(spec=Response)
    non_matching_response.status_code = 200

    matching_response = MagicMock(spec=Response)
    matching_response.status_code = 201

    fn = MagicMock()
    fn.side_effect = [
        non_matching_response,
        matching_response,
    ]

    response = with_retry(fn, expected_status_codes=[201])
    assert response == matching_response


def test_with_retry__no_status_code():
    """Verify that with_retry can also be used on any function
    even whose return values don't have status_codes.
    In that case just for its exception retrying
    and back off capabiliies."""

    x = 0

    def fn():
        nonlocal x
        x += 1
        if x < 2:
            raise ValueError("not yet")
        return x

    response = with_retry(fn, retry_exceptions=[ValueError])
    assert 2 == response


class TestAsyncRetry:
    """Unit tests for the with_retry_async function."""

    @pytest.mark.asyncio
    async def test_with_retry(self) -> None:
        retry_params = {"retry_max_wait_before_failure": 1}
        response = AsyncMock()
        function = AsyncMock()
        function.return_value = response

        # -- No failures --
        response.status_code.__eq__.side_effect = lambda x: x == 250
        await with_retry_async(function, verbose=True, **retry_params)
        assert function.call_count == 1

        # -- Always fail --
        response.status_code.__eq__.side_effect = lambda x: x == 503
        await with_retry_async(function, verbose=True, **retry_params)
        assert function.call_count > 5

        # -- Fail then succeed --
        thirdTimes = [3, 2, 1]

        def the_charm(x):
            if x == 503:
                count = thirdTimes.pop()
                return count != 3
            return x == 503

        response.status_code.__eq__.side_effect = the_charm
        await with_retry_async(function, verbose=True, **retry_params)
        assert function.call_count > 8

        # -- Retry with an error message --
        retry_error_messages = ["Foo"]
        retry_params["retry_errors"] = retry_error_messages
        response.status_code.__eq__.side_effect = lambda x: x == 500
        response.headers.__contains__.reset_mock()
        response.headers.__contains__.side_effect = lambda x: x == "content-type"
        response.headers.get.side_effect = lambda x, default_value: (
            "application/json" if x == "content-type" else None
        )
        response.json.return_value = {"reason": retry_error_messages[0]}
        await with_retry_async(function, **retry_params)
        assert response.headers.get.called
        assert function.call_count > 12

        # -- Propagate an error up --
        print("Expect a SynapseError: Bar")

        def foo():
            raise SynapseError("Bar")

        function.side_effect = foo
        with pytest.raises(SynapseError) as ex_cm:
            await with_retry_async(function, **retry_params)
        assert "Bar" in str(ex_cm.value)
        assert function.call_count > 13

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "values",
        (
            None,
            [],
            tuple(),
        ),
    )
    async def test_with_retry__empty_status_codes(self, values) -> None:
        """Verify that passing some Falsey values for the various sequence args is ok"""
        response = AsyncMock(spec=Response)
        response.status_code = 200

        fn = AsyncMock()
        fn.return_value = response

        # no unexpected exceptions etc should be raised
        returned_response = await with_retry_async(
            fn,
            retry_status_codes=values,
            expected_status_codes=values,
            retry_exceptions=values,
            retry_errors=values,
        )
        assert returned_response == response

    @pytest.mark.asyncio
    async def test_with_retry__expected_status_code(self) -> None:
        """Verify using retry expected_status_codes"""

        non_matching_response = AsyncMock(spec=Response)
        non_matching_response.status_code = 200

        matching_response = AsyncMock(spec=Response)
        matching_response.status_code = 201

        fn = AsyncMock()
        fn.side_effect = [
            non_matching_response,
            matching_response,
        ]

        response = await with_retry_async(fn, expected_status_codes=[201])
        assert response == matching_response

    @pytest.mark.asyncio
    async def test_with_retry__no_status_code(self) -> None:
        """Verify that with_retry can also be used on any function
        even whose return values don't have status_codes.
        In that case just for its exception retrying
        and back off capabiliies."""

        x = 0

        async def fn():
            nonlocal x
            x += 1
            if x < 2:
                raise ValueError("not yet")
            return x

        response = await with_retry_async(fn, retry_exceptions=[ValueError])
        assert 2 == response
