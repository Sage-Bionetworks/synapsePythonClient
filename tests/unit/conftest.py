import logging
import platform
import urllib.request

from unittest import mock
from pytest_socket import disable_socket, SocketBlockedError
import pytest
import os, time

from synapseclient import Synapse
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

"""
pytest unit test session level fixtures
"""


def pytest_runtest_setup():
    """Disable socket connections during unit tests.

    This uses the https://pypi.org/project/pytest-socket/ library for this functionality.

    allow_unix_socket=True is required for async to work.
    """
    disable_socket(allow_unix_socket=True, allow_windows_pipe=True)


def test_confirm_connections_blocked():
    """Confirm that socket connections are blocked during unit tests."""
    with pytest.raises(SocketBlockedError) as cm_ex:
        urllib.request.urlopen("http://example.com")
    assert "A test tried to use socket.socket." == str(cm_ex.value)


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"
    if platform.system() != "Windows":
        time.tzset()


@pytest.fixture(scope="session")
def syn():
    """
    Create a Synapse instance that can be shared by all tests in the session.
    """
    syn = Synapse(debug=False, skip_checks=True)
    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    return syn
