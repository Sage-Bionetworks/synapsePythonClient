from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os

import unit
from mock import patch, MagicMock, ANY, call
from nose import SkipTest
from nose.tools import assert_dict_equal
from builtins import str

import synapseutils
from synapseclient import Project
from synapseutils import notifyMe

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    import pandas as pd
    import pandas.util.testing as pdt
    pandas_available=True
except:
    pandas_available=False

def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = unit.syn

def test_notifyMe__successful_call():
    subject = "some message subject"
    owner_id = '12434'
    user_profile = {'ownerId':owner_id}
    with patch.object(syn, "sendMessage") as mocked_send_message,\
         patch.object(syn, "getUserProfile", return_value=user_profile) as mocked_get_user_profile:
        mocked_func = MagicMock()
        @notifyMe(syn, messageSubject=subject)
        def test_function():
            mocked_func()

        test_function()
        mocked_get_user_profile.assert_called_once()
        mocked_send_message.assert_called_once_with([owner_id],subject, ANY)

def test_notifyMe__exception_thrown_and_retry_fail():
    subject = "some message subject"
    owner_id = '12434'
    user_profile = {'ownerId':owner_id}
    with patch.object(syn, "sendMessage") as mocked_send_message,\
         patch.object(syn, "getUserProfile", return_value=user_profile) as mocked_get_user_profile:
        mocked_func = MagicMock(side_effect=[Exception('first time fails'), 'second time is Fine'])
        @notifyMe(syn, messageSubject=subject, retries=1)
        def test_function():
            mocked_func()

        test_function()
        mocked_send_message.

        first_call = mocked_send_message.call_args_list[0]
        second_call = mocked_send_message.call_args_list[1]


def test_notifyMe__exception_throw_and_retry_success():
    pass