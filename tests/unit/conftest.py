"""
pytest unit test session level fixtures
"""

import logging
import os
import platform
import time
import urllib.request

import pytest
from pytest_socket import SocketBlockedError, disable_socket

from synapseclient import Synapse
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

Synapse.allow_client_caching = False


def pytest_runtest_setup():
    """Disable socket connections during unit tests.

    This uses the https://pypi.org/project/pytest-socket/ library for this functionality.

    allow_unix_socket=True is required for async to work.
    """
    # This is a work-around because of https://github.com/python/cpython/issues/77589
    if platform.system() != "Windows":
        disable_socket(allow_unix_socket=True)


def test_confirm_connections_blocked():
    """Confirm that socket connections are blocked during unit tests."""
    if platform.system() != "Windows":
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
    Synapse.set_client(syn)
    return syn
