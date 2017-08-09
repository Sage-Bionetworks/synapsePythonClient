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

import integration
from integration import schedule_for_cleanup, QUERY_TIMEOUT_SEC

if six.PY2:
    from StringIO import StringIO
else:
    from io import StringIO
