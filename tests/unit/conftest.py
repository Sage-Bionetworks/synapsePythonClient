import logging

import pytest

from synapseclient import Synapse
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

"""
pytest unit test session level fixtures
"""


@pytest.fixture(scope="session")
def syn():
    """
    Create a Synapse instance that can be shared by all tests in the session.
    """
    syn = Synapse(debug=False, skip_checks=True)
    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    return syn
