"""Test the Synapse command line client.

"""

# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import str
import six

import filecmp
import os
import re
import sys
import uuid
import json
import time
from nose.plugins.attrib import attr
from nose.tools import assert_raises, assert_equals, assert_less
import tempfile
import shutil
import unit
from mock import patch

try:
    import ConfigParser
except:
    import configparser as ConfigParser

import synapseclient
import synapseclient.client as client
import synapseclient.utils as utils
import synapseclient.__main__ as cmdline

from synapseclient.evaluation import Evaluation

import synapseutils

if six.PY2:
    from StringIO import StringIO
else:
    from io import StringIO

def setup(module):

    module.syn = unit.syn

def test_command_sync():
    """Test the sync fuction.

    Since this function only passes argparse arguments for the sync subcommand
    straight to `synapseutils.sync.syncToSynapse`, the only tests here are for
    the command line arguments provided and that the function is called once.

    """

    parser = cmdline.build_parser()
    args = parser.parse_args(['sync', '/tmp/foobarbaz.tsv'])

    assert_equals(args.manifestFile, '/tmp/foobarbaz.tsv')
    assert_equals(args.dryRun, False)
    assert_equals(args.sendMessages, False)
    assert_equals(args.retries, 4)

    with patch.object(synapseutils, "syncToSynapse") as mockedSyncToSynapse:
        cmdline.sync(args, syn)
        mockedSyncToSynapse.assert_called_once_with(syn,
                                                    manifestFile=args.manifestFile,
                                                    dryRun=args.dryRun,
                                                    sendMessages=args.sendMessages,
                                                    retries=args.retries)
