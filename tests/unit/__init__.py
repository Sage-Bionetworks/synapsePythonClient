"""
Unit tests for the Synapse Client for Python
"""

import logging

from synapseclient import Synapse
from synapseclient.core.logging_setup import SILENT_LOGGER_NAME

syn = Synapse(debug=False, skip_checks=True)
syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
syn = syn
