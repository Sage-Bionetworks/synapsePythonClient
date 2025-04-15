"""Integration tests for version checking"""

import httpx
from pytest_mock import MockerFixture

from synapseclient.core.version_check import _get_version_info_from_pypi, version_check


async def test_version_check(mocker: MockerFixture):
    """Integration checks for version_check"""
    # Should be higher than current version and return true
    assert version_check(current_version="999.999.999")

    # Test out of date version
    assert not version_check(current_version="0.0.1")

    # Assert httpx.get called  when running version_check
    spy = mocker.spy(httpx, "get")
    version_check()
    spy.assert_called_once()


def test_get_version_info_from_pypi():
    """Integration test for _get_version_info_from_pypi"""
    assert _get_version_info_from_pypi()
