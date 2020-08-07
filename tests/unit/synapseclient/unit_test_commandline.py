"""Test the Synapse command line client.

"""

from unittest.mock import Mock, patch

import synapseutils
import synapseclient.__main__ as cmdline


def test_command_sync(syn):
    """Test the sync function.

    Since this function only passes argparse arguments for the sync subcommand
    straight to `synapseutils.sync.syncToSynapse`, the only tests here are for
    the command line arguments provided and that the function is called once.

    """

    parser = cmdline.build_parser()
    args = parser.parse_args(['sync', '/tmp/foobarbaz.tsv'])

    assert args.manifestFile == '/tmp/foobarbaz.tsv'
    assert args.dryRun is False
    assert args.sendMessages is False
    assert args.retries == 4

    with patch.object(synapseutils, "syncToSynapse") as mockedSyncToSynapse:
        cmdline.sync(args, syn)
        mockedSyncToSynapse.assert_called_once_with(syn,
                                                    manifestFile=args.manifestFile,
                                                    dryRun=args.dryRun,
                                                    sendMessages=args.sendMessages,
                                                    retries=args.retries)


def test_get_multi_threaded_flag():
    """Test the multi threaded command line flag"""
    parser = cmdline.build_parser()
    args = parser.parse_args(['get', '--multiThreaded', 'syn123'])

    assert args.multiThreaded

    # defaults to True
    args = parser.parse_args(['get', 'syn123'])
    assert args.multiThreaded


@patch('builtins.print')
def test_get_sts_token(mock_print):
    """Test getting an STS token."""
    folder_id = 'syn_1'
    permission = 'read_write'
    syn = Mock()

    expected_output = 'export foo=bar'
    syn.get_sts_storage_token.return_value = expected_output

    parser = cmdline.build_parser()
    args = parser.parse_args(['get-sts-token', folder_id, permission, '-o', 'shell'])
    cmdline.get_sts_token(args, syn)
    syn.get_sts_storage_token.assert_called_with(folder_id, permission, output_format='shell')

    mock_print.assert_called_once_with(expected_output)
