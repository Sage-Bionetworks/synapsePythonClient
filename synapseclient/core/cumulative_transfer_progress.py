from contextlib import contextmanager
import threading
import time
import sys

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
        _thread_local.cumulative_transfer_progress.printTransferProgress(*args, **kwargs)
    else:
        utils.printTransferProgress(*args, **kwargs)


def is_active():
    """Return whether the current thread is accumulating progress data."""
    return hasattr(_thread_local, 'cumulative_transfer_progress')


class CumulativeTransferProgress:

    def __init__(self, label, start=None):
        self._lock = threading.Lock()
        self._label = label

        self._spinner = utils.Spinner()
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

    def printTransferProgress(self, transferred, toBeTransferred, prefix='', postfix='', isBytes=True, dt=None,
                              previouslyTransferred=0):
        """
        Parameters match those of synapseclient.core.utils.printTransferProgress.
        """

        if not sys.stdout.isatty():
            return

        with self._lock:
            if toBeTransferred == 0 or float(transferred) / toBeTransferred >= 1:
                # if the individual transfer is complete then we pass through the print
                # to the underlying utility method which will print a complete 100%
                # progress bar on a newline.
                utils.printTransferProgress(
                    transferred,
                    toBeTransferred,
                    prefix=prefix,
                    postfix=postfix,
                    isBytes=isBytes,
                    dt=dt,
                    previouslyTransferred=previouslyTransferred
                )

            # in order to know how much of the transferred data is newly transferred
            # we subtract the previously reported amount. this assumes that the printing
            # of the progress for any particular transfer is always conducted by the same
            # thread, which is true for all current transfer implementations.
            self._total_transferred += (transferred - _thread_local.thread_transferred)
            _thread_local.thread_transferred = transferred

            cumulative_dt = time.time() - self._start
            rate = self._total_transferred / float(cumulative_dt)
            rate = '(%s/s)' % utils.humanizeBytes(rate) if isBytes else rate

            # we print a rotating tick with each update
            self._spinner.print_tick()

            sys.stdout.write(f"{self._label} {utils.humanizeBytes(self._total_transferred)} {rate}")
            sys.stdout.flush()
