import logging
import urllib.request

import mock
import pytest

from synapseclient import Synapse
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

"""
pytest unit test session level fixtures
"""

_BLOCKED_CONNECTION_MESSAGE = "Unit tests should not be making remote connections. " \
                                "Mock the remote call or write an integration test instead."


@pytest.fixture(autouse=True, scope='session')
def block_remote_conections(request):
    """
    Block all remote calls during unit tests.
    """
    # inspired by https://stackoverflow.com/q/18601828

    def block_socket_side_effect(*args, **kwargs):
        raise ValueError(_BLOCKED_CONNECTION_MESSAGE)

    block_socket_patcher = mock.patch(
        'socket.socket',
        side_effect=block_socket_side_effect
    )
    block_socket_patcher.start()

    request.addfinalizer(block_socket_patcher.stop)


def test_confirm_connections_blocked():
    """Confirm that socket connections are blocked during unit tests."""
    with pytest.raises(ValueError) as cm_ex:
        urllib.request.urlopen('http://example.com')
    assert _BLOCKED_CONNECTION_MESSAGE == str(cm_ex.value)


@pytest.fixture(scope="session")
def syn():
    """
    Create a Synapse instance that can be shared by all tests in the session.
    """
    syn = Synapse(debug=False, skip_checks=True)
    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    return syn
