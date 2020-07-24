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
    # first call is in init constructor below, remaining three calls are in print calls to calculate transfer rate
    mock_time.time.side_effect = [0, 100, 200, 300]
    progress = cumulative_transfer_progress.CumulativeTransferProgress('Testing')

    # two prints for file 1 in a separate thread
    # the first halfway through the second print indicates the file is done

    # one print for a second file in this thread showing a file 3/4 done

    args1a = [100, 200]
    kwargs1a = {
        'prefix': 'prefix1',
        'postfix': 'postfix1',
        'isBytes': True,
        'dt': 300,
        'previouslyTransferred': 0,
    }

    args1b = [200, 200]
    kwargs1b = {
        'prefix': 'prefix1',
        'postfix': 'postfix1',
        'isBytes': True,
        'dt': 400,
        'previouslyTransferred': 0,
    }

    args2 = [150, 200]
    kwargs2 = {
        'prefix': 'prefix2',
        'postfix': 'postfix2',
        'isBytes': True,
        'dt': 300,
        'previouslyTransferred': 0,
    }

    def print_completed():
        with progress.accumulate_progress():
            cumulative_transfer_progress.printTransferProgress(*args1a, **kwargs1a)
            cumulative_transfer_progress.printTransferProgress(*args1b, **kwargs1b)
    thread = threading.Thread(target=print_completed)
    thread.start()

    thread.join()

    # the second print for the first file should have passed through to utils
    # since it indicated the file was complete
    mock_utils_print_transfer_progress.assert_called_once_with(*args1b, **kwargs1b)

    with progress.accumulate_progress():
        assert_is(progress, getattr(cumulative_transfer_progress._thread_local, 'cumulative_transfer_progress'))
        cumulative_transfer_progress.printTransferProgress(*args2, **kwargs2)

        # 150 bytes transferred by this thread
        assert_equals(150, cumulative_transfer_progress._thread_local.thread_transferred)

    # total transferred is 350, 200 from the first file in the separate thread,
    # 150 from the transfer above from this thread
    assert_equals(350, progress._total_transferred)

    expected_stdout_writes = [
        mock.call('\r / Testing 100.0bytes (1.0bytes/s)'),
        mock.call('\r - Testing 200.0bytes (1.0bytes/s)'),
        mock.call('\r \\ Testing 350.0bytes (1.2bytes/s)'),
    ]
    assert_equals(expected_stdout_writes, mock_sys.stdout.write.call_args_list)

    # test thread local props cleaned up
    assert_false(hasattr(cumulative_transfer_progress._thread_local, 'thread_transferred'))
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
