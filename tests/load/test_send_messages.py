import os
import synapseclient
from synapseclient import Project, File

syn = None


def setup(module):

    module.syn = synapseclient.Synapse()
    module.syn.login()


def test_message_retry():
    synapseclient.client.STANDARD_RETRY_PARAMS['verbose'] = True
    syn.debug = True
    for i in range(40):
        syn.sendMessage([377358], "test_message_retry", "This is a totally bogus test message for testing.")
