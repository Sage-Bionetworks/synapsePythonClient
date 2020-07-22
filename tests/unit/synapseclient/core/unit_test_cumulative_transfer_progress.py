import threading

from synapseclient.core import cumulative_transfer_progress
from synapseclient.core import utils

from unittest import mock
from nose.tools import assert_equals, assert_false, assert_is


@mock.patch.object(utils, 'printTransferProgress')
def test_not_configured(mock_utils_print_transfer_progress):
    """Verify that if no thread local state is configured that printTransferProgress falls through to utils"""

    args = [5, 10]
    kwargs = {
        'prefix': 'prefix',
        'postfix': 'postfix',
        'isBytes': True,
        'dt': 100,
        'previouslyTransferred': 200,
    }

    cumulative_transfer_progress.printTransferProgress(*args, **kwargs)
    mock_utils_print_transfer_progress.assert_called_once_with(*args, **kwargs)


@mock.patch.object(cumulative_transfer_progress, 'time')
@mock.patch.object(cumulative_transfer_progress, 'sys')
@mock.patch.object(utils, 'printTransferProgress')
def test_progress(mock_utils_print_transfer_progress, mock_sys, mock_time):
    """Verify writing progress via a CumulativeProgress"""

    # mock time for predictability
    mock_time.time.return_value = 0
    progress = cumulative_transfer_progress.CumulativeTransferProgress('Testing')

    mock_time.time.return_value = 100
    with progress.accumulate_progress():
        assert_is(progress, getattr(cumulative_transfer_progress._thread_local, 'cumulative_transfer_progress'))

        # a complete transfer (transferred >= toBeTransferred)
        # should additionally trigger a fall through to utils to print the complete progress bar
        args1 = [100, 100]
        kwargs1 = {
            'prefix': 'prefix1',
            'postfix':  'postfix1',
            'isBytes': True,
            'dt': 300,
            'previouslyTransferred': 0
        }
        cumulative_transfer_progress.printTransferProgress(*args1, **kwargs1)

        cumulative_transfer_progress.printTransferProgress(
            150,
            200,
            prefix='prefix2',
            postfix='postfix2',
            isBytes=True,
            dt=300,
            previouslyTransferred=0
        )

        assert_equals(150, progress._total_transferred)
        assert_equals(150, progress._thread_totals[threading.get_ident()])

        print(progress)

    mock_utils_print_transfer_progress.assert_called_once_with(*args1, **kwargs1)

    expected_stdout_writes = [
        mock.call('\r / Testing 100.0bytes (1.0bytes/s)'),
        mock.call('\r - Testing 150.0bytes (1.5bytes/s)'),
    ]
    assert_equals(expected_stdout_writes, mock_sys.stdout.write.call_args_list)

    assert_false(threading.get_ident() in progress._thread_totals)
    assert_false(hasattr(cumulative_transfer_progress._thread_local, 'cumulative_transfer_progress'))


@mock.patch.object(cumulative_transfer_progress, 'sys')
@mock.patch.object(utils, 'printTransferProgress')
def test_progress__not_tty(mock_utils_print_transfer_progress, mock_sys):
    """Verify nothing written if stdout is not a tty"""
    mock_sys.stdout.isatty.return_value = False
    progress = cumulative_transfer_progress.CumulativeTransferProgress('Testing')

    with progress.accumulate_progress():
        cumulative_transfer_progress.printTransferProgress(100, 100)
        assert_false(mock_sys.stdout.write.called)
        assert_false(mock_utils_print_transfer_progress.called)
