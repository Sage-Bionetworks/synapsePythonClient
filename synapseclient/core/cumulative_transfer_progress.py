import threading
import time
from contextlib import contextmanager

from tqdm import tqdm

from synapseclient.core import utils

# Defines a mechanism for printing file transfer progress that includes potentially multiple file transfers to the
# console in a possibly multi threaded manner. Normally each individual file download will write its own output
# to the console, overwriting each other if multiple file downloads are concurrently running.
# This will print a running total of transferred bytes with each finished file printed as it is completed.

# we use a thread local to configure this because we are potentially running file transfers in multiple threads,
# and we don't want individual download implementations to have to know about the details or track
# any other concurrent downloads. instead it is up to a master coordinator that is launching the threads (e.g.
# a Synapse sync) to configure the thread state. individual download implementations don't have to do anything
# but import and use this instead of the underlying utils function.
_thread_local = threading.local()


def printTransferProgress(*args, **kwargs):
    """Prints transfer progress using the cumulative format if that has been configured on the running
    thread, otherwise prints transfer directly to standard out as normal. This function should
    be imported instead of utils.printTransferProgress in locations that may be part of a cumulative
    transfer (i.e. a Synapse sync)."""

    if is_active():
        _thread_local.cumulative_transfer_progress.printTransferProgress(
            *args, **kwargs
        )
    else:
        utils.printTransferProgress(*args, **kwargs)


def is_active():
    """Return whether the current thread is accumulating progress data."""
    return hasattr(_thread_local, "cumulative_transfer_progress")


class CumulativeTransferProgress:
    def __init__(self, label, start=None):
        self._lock = threading.Lock()
        self._label = label

        self._progress_bar: tqdm = None
        self._start = start if start is not None else time.time()

        self._total_transferred = 0

    @contextmanager
    def accumulate_progress(self):
        """Threads should enter this context while they are running their transfers."""

        _thread_local.cumulative_transfer_progress = self
        _thread_local.thread_transferred = 0
        try:
            yield
        finally:
            del _thread_local.cumulative_transfer_progress
            del _thread_local.thread_transferred

    def printTransferProgress(
        self,
        transferred: int,
        toBeTransferred: int,
        prefix: str = "",
        postfix: str = "",
        isBytes: bool = True,
        dt: None = None,
        previouslyTransferred: int = 0,
    ) -> None:
        """
        Parameters match those of synapseclient.core.utils.printTransferProgress.

        Arguments:
            transferred: The number of bytes transferred in the current transfer.
            toBeTransferred: The total number of bytes to be transferred in the current
                transfer.
            prefix: A string to prepend to the progress bar.
            postfix: A string to append to the progress bar.
            isBytes: If True, the progress bar will display bytes. If False, the
                progress bar will display the unit of the transferred data.
            dt: Deprecated.
            previouslyTransferred: Deprecated.

        Returns:
            None
        """
        with self._lock:
            if not self._progress_bar:
                self._progress_bar = tqdm(
                    desc=prefix if prefix else "Transfer Progress",
                    unit_scale=True,
                    total=toBeTransferred,
                    smoothing=0,
                    postfix=postfix,
                    unit="B" if isBytes else None,
                    leave=None,
                )
            # in order to know how much of the transferred data is newly transferred
            # we subtract the previously reported amount. this assumes that the printing
            # of the progress for any particular transfer is always conducted by the same
            # thread, which is true for all current transfer implementations.
            self._total_transferred += transferred - _thread_local.thread_transferred
            _thread_local.thread_transferred = transferred

            self._progress_bar.update(transferred - _thread_local.thread_transferred)
