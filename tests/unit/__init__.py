"""
Unit tests for the Synapse Client for Python
"""

import synapseclient


def setup_module(module):
    syn = synapseclient.Synapse(debug=False, skip_checks=True)
    module.syn = syn