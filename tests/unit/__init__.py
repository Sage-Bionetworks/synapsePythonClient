"""
Unit tests for the Synapse Client for Python
"""
from __future__ import unicode_literals
from __future__ import print_function

import sys
import synapseclient


def setup_module(module):
    print("Python version:", sys.version)

    syn = synapseclient.Synapse(debug=False, skip_checks=True)
    module.syn = syn
