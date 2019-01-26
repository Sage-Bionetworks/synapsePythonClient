"""Test the Synapse command line client.

"""

from nose.tools import assert_equals
from mock import patch

import synapseclient.__main__ as cmdline
import synapseutils
import synapseclient

def setup(module):
    module.syn = synapseclient.tests.unit.syn


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
