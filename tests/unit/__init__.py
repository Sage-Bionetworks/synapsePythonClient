"""
Unit tests for the Synapse Client for Python
"""

import sys
import synapseclient


def setup_module(module):
    print("Python version:", sys.version)

    syn = synapseclient.Synapse(debug=False, skip_checks=True)
    module.syn = syn