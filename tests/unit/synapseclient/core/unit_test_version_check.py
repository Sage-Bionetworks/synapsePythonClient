"""Unit tests for version check functions"""


from unittest.mock import patch

import pytest

from synapseclient.core.version_check import (
    _get_local_package_metadata,
    _is_current_version_behind,
    _version_check,
    _version_tuple,
)


@pytest.mark.parametrize(
    "current_version, latest_version, check_for_point_releases, expected_result",
    [
        # By default only minor and major versions will trigger a False result
        ("1.0.0", "1.0.0", False, True),
        ("1.0.0", "1.0.1", False, True),
        ("1.0.0", "1.1.0", False, False),
        ("1.0.0", "2.0.0", False, False),
        # If check_for_point_releases is set to True, patch versions will
        #  also trigger a false result
        ("1.0.0", "1.0.0", True, True),
        ("1.0.0", "1.0.1", True, False),
        ("1.0.0", "1.1.0", True, False),
        ("1.0.0", "2.0.0", True, False),
        # When the current version is ahead, this should always result in True
        ("2.0.0", "1.0.0", True, True),
    ],
)
def test__version_check_mocked_pypi(
    current_version: str,
    latest_version: str,
    check_for_point_releases: bool,
    expected_result: bool,
):
    """
    Tests for _version_check function where the version information is obtained from Pypi,
      but the call is mocked.
    """
    with patch(
        "synapseclient.core.version_check._get_version_info_from_pypi",
        return_value=latest_version,
    ):
        assert (
            _version_check(current_version, check_for_point_releases) == expected_result
        )


@pytest.mark.parametrize(
    "current_version, expected_result",
    [
        # As of writing this test the current version is 4.7.0, these should always be behind
        ("4", False),
        ("4.0", False),
        ("4.0.0", False),
        # These should always be ahead
        ("100", True),
        ("100.0", True),
        ("100.0.0", True),
    ],
)
def test__version_check_local(current_version: str, expected_result: bool):
    """
    Tests for _version_check function where the version information is obtained locally
    """
    assert _version_check(current_version, use_local_metadata=True) == expected_result


@pytest.mark.parametrize(
    "current_version, latest_version, levels, expected_result",
    [
        # When versions are equal the current package is never considered behind
        ("1", "1", 1, False),
        ("1", "1", 2, False),
        ("1", "1", 3, False),
        # Where versions differ by major version, the current package is always considered behind
        ("1", "2", 1, True),
        ("1", "2", 2, True),
        ("1", "2", 3, True),
        # Where versions only differ by minor version, the current package is only considered behind
        #  at levels=2 or higher
        ("1.0", "1.1", 1, False),
        ("1.0", "1.1", 2, True),
        ("1.0", "1.1", 3, True),
        # Where versions only differ by patch, the current package is only considered behind
        #  at levels=3 or higher
        ("1.0.0", "1.0.1", 1, False),
        ("1.0.0", "1.0.1", 2, False),
        ("1.0.0", "1.0.1", 3, True),
        # When the current version is ahead, this should result in False
        ("10.0.0", "2.0.0", 1, False),
        ("10.0.0", "2.0.0", 2, False),
        ("10.0.0", "2.0.0", 3, False),
    ],
)
def test_is_current_version_behind(
    current_version: str, latest_version: str, levels: int, expected_result: bool
) -> None:
    """Tests for _is_current_version_behind"""
    assert (
        _is_current_version_behind(current_version, latest_version, levels)
        == expected_result
    )


@pytest.mark.parametrize(
    "input_version, input_levels, expected_result",
    [
        ("0.5.1.dev200", 2, ("0", "5")),
        ("0.5.1.dev200", 3, ("0", "5", "1")),
        ("1.6", 3, ("1", "6", "0")),
        ("1", 1, ("1",)),
        ("1", 2, ("1", "0")),
        ("1", 3, ("1", "0", "0")),
        ("1.1", 1, ("1",)),
        ("1.1", 2, ("1", "1")),
        ("1.1", 3, ("1", "1", "0")),
    ],
)
def test_version_tuple(
    input_version: str, input_levels: int, expected_result: tuple
) -> None:
    """Tests for _version_tuple function"""
    assert _version_tuple(input_version, levels=input_levels) == expected_result


def test_get_local_files() -> None:
    """Tests for _get_local_package_metadata function"""
    assert _get_local_package_metadata()
