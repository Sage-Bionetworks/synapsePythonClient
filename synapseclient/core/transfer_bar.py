"""Logic used to handle progress bars for file uploads and downloads."""
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading

from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional, Union

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

if TYPE_CHECKING:
    from synapseclient import Synapse

_thread_local = _threading.local()


@contextmanager
def shared_progress_bar(progress_bar: tqdm, syn: "Synapse"):
    """An outside process that will eventually trigger an upload through this module
    can configure a shared Progress Bar by running its code within this context manager.
    """
    with logging_redirect_tqdm(loggers=[syn.logger]):
        _thread_local.progress_bar = progress_bar
        try:
            yield
        finally:
            _thread_local.progress_bar.close()
            _thread_local.progress_bar.refresh()
            del _thread_local.progress_bar


def increment_progress_bar_total(total: int, progress_bar: Union[tqdm, None]) -> None:
    """Update the total size of the progress bar.

    Arguments:
        total: The total size of the progress bar.
        progress_bar: The progress bar

    Returns:
        None
    """
    if progress_bar is None:
        return None
    progress_bar.total = total + (
        progress_bar.total if progress_bar.total and progress_bar.total > 1 else 0
    )
    progress_bar.refresh()


def increment_progress_bar(n: int, progress_bar: Union[tqdm, None]) -> None:
    """None safe update the the progress bar.

    Arguments:
        n: The amount to increment the progress bar by.
        progress_bar: The progress bar

    Returns:
        None
    """
    if not progress_bar:
        return None
    progress_bar.update(n)


@contextmanager
def shared_download_progress_bar(
    file_size: int, *, synapse_client: Optional["Synapse"] = None
):
    """An outside process that will eventually trigger a download through this module
    can configure a shared Progress Bar by running its code within this context manager.

    Arguments:
        file_size: The size of the file being downloaded.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Yields:
        A context manager that will handle the download progress bar.

    """
    _thread_local.progress_bar_download_context_managed = True
    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)
    with logging_redirect_tqdm(loggers=[syn.logger]):
        get_or_create_download_progress_bar(file_size=file_size, synapse_client=syn)
        try:
            yield
        finally:
            _thread_local.progress_bar_download_context_managed = False
            if _thread_local.progress_bar_download:
                _thread_local.progress_bar_download.close()
                _thread_local.progress_bar_download.refresh()
                del _thread_local.progress_bar_download


def close_download_progress_bar() -> None:
    """Handle closing the download progress bar if it is not context managed."""
    if not _is_context_managed_download_bar():
        progress_bar: tqdm = getattr(_thread_local, "progress_bar_download", None)
        if progress_bar is not None:
            progress_bar.close()
            progress_bar.refresh()
            del _thread_local.progress_bar_download


def _is_context_managed_download_bar() -> bool:
    """Return whether a download progress bar has been started."""
    return getattr(_thread_local, "progress_bar_download_context_managed", False)


def get_or_create_download_progress_bar(
    file_size: int, postfix: str = None, *, synapse_client: Optional["Synapse"] = None
) -> Union[tqdm, None]:
    """Return the existing progress bar if it exists, otherwise create a new one.

    Arguments:
        file_size: The size of the file being downloaded.
        postfix: The postfix to add to the progress bar. When this is called for the
            first time the postfix will be set. If called again the postfix will be
            removed as multiple downloads are sharing this bar.
        synapse_client: The Synapse client.
    """

    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)

    if syn.silent:
        return None

    progress_bar: tqdm = getattr(_thread_local, "progress_bar_download", None)
    if progress_bar is None:
        progress_bar = tqdm(
            total=file_size,
            desc="Downloading files",
            unit="B",
            unit_scale=True,
            smoothing=0,
            postfix=postfix,
        )
        _thread_local.progress_bar_download = progress_bar
    else:
        progress_bar.total = file_size + (
            progress_bar.total if progress_bar.total and progress_bar.total > 1 else 0
        )
        progress_bar.postfix = None
        progress_bar.refresh()
    return progress_bar
