"""
Unit tests for the Synapse Client for Python
"""

import logging
import sys
import synapseclient
from synapseclient.logging_setup import SILENT_LOGGER_NAME


def setup_module(module):
    print("Python version:", sys.version)

    syn = synapseclient.Synapse(debug=False, skip_checks=True)
    syn.logger = logging.getLogger(SILENT_LOGGER_NAME)
    module.syn = syn
