"""Functions related to downloading files from Synapse."""

from .download_async import (
    SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE,
    DownloadRequest,
    PresignedUrlInfo,
    PresignedUrlProvider,
    _MultithreadedDownloader,
    download_file,
    shared_progress_bar,
)
from .download_functions import (
    download_by_file_handle,
    download_file_entity,
    download_from_url,
    download_from_url_multi_threaded,
    ensure_download_location_is_directory,
)

__all__ = [
    # download_functions
    "download_file_entity",
    "ensure_download_location_is_directory",
    "download_by_file_handle",
    "download_from_url",
    "download_from_url_multi_threaded",
    # download_async
    "DownloadRequest",
    "download_file",
    "shared_progress_bar",
    "SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE",
    "PresignedUrlInfo",
    "PresignedUrlProvider",
    "_MultithreadedDownloader",
]
