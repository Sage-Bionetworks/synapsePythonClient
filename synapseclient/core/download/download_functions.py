"""This module handles the various ways that a user can download a file to Synapse."""

import errno
import os
import shutil
import sys
import time
import hashlib
import urllib.parse as urllib_urlparse
import urllib.request as urllib_request
from typing import TYPE_CHECKING, Optional

from synapseclient.api import (
    get_client_authenticated_s3_profile,
    get_file_handle_for_download,
)
from synapseclient.core import exceptions, multithread_download, sts_transfer, utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    SynapseHTTPError,
    SynapseMd5MismatchError,
    SynapseError,
)
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
from synapseclient.core.retry import (
    DEFAULT_RETRY_STATUS_CODES,
    RETRYABLE_CONNECTION_ERRORS,
    RETRYABLE_CONNECTION_EXCEPTIONS,
    with_retry,
)

from synapseclient.core.utils import (
    MB,
)

if TYPE_CHECKING:
    from synapseclient import Synapse, Entity

FILE_BUFFER_SIZE = 2 * MB
REDIRECT_LIMIT = 5

# Defines the standard retry policy applied to the rest methods
# The retry period needs to span a minute because sending messages is limited to 10 per 60 seconds.
STANDARD_RETRY_PARAMS = {
    "retry_status_codes": DEFAULT_RETRY_STATUS_CODES,
    "retry_errors": RETRYABLE_CONNECTION_ERRORS,
    "retry_exceptions": RETRYABLE_CONNECTION_EXCEPTIONS,
    "retries": 60,  # Retries for up to about 30 minutes
    "wait": 1,
    "max_wait": 30,
    "back_off": 2,
}


async def download_file_entity(
    *,
    download_location: str,
    entity: "Entity",
    if_collision: str,
    submission: str,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Download file entity

    Arguments:
        download_location: The download location
        entity:           The Synapse Entity object
        if_collision:      Determines how to handle file collisions.
                            May be

            - `overwrite.local`
            - `keep.local`
            - `keep.both`

        submission:       Access associated files through a submission rather than through an entity.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    # set the initial local state
    entity.path = None
    entity.files = []
    entity.cacheDir = None

    # check to see if an UNMODIFIED version of the file (since it was last downloaded) already exists
    # this location could be either in .synapseCache or a user specified location to which the user previously
    # downloaded the file
    cached_file_path = client.cache.get(
        file_handle_id=entity.dataFileHandleId, path=download_location
    )

    # location in .synapseCache where the file would be corresponding to its FileHandleId
    synapse_cache_location = client.cache.get_cache_dir(
        file_handle_id=entity.dataFileHandleId
    )

    file_name = (
        entity._file_handle.fileName
        if cached_file_path is None
        else os.path.basename(cached_file_path)
    )

    # Decide the best download location for the file
    if download_location is not None:
        # Make sure the specified download location is a fully resolved directory
        download_location = ensure_download_location_is_directory(download_location)
    elif cached_file_path is not None:
        # file already cached so use that as the download location
        download_location = os.path.dirname(cached_file_path)
    else:
        # file not cached and no user-specified location so default to .synapseCache
        download_location = synapse_cache_location

    # resolve file path collisions by either overwriting, renaming, or not downloading, depending on the
    # ifcollision value
    download_path = resolve_download_path_collisions(
        download_location=download_location,
        file_name=file_name,
        if_collision=if_collision,
        synapse_cache_location=synapse_cache_location,
        cached_file_path=cached_file_path,
    )
    if download_path is None:
        return

    if cached_file_path is not None:  # copy from cache
        if download_path != cached_file_path:
            # create the foider if it does not exist already
            if not os.path.exists(download_location):
                os.makedirs(download_location)
            shutil.copy(cached_file_path, download_path)

    else:  # download the file from URL (could be a local file)
        object_type = "FileEntity" if submission is None else "SubmissionAttachment"
        object_id = entity["id"] if submission is None else submission

        # reassign downloadPath because if url points to local file (e.g. file://~/someLocalFile.txt)
        # it won't be "downloaded" and, instead, downloadPath will just point to '~/someLocalFile.txt'
        # _downloadFileHandle may also return None to indicate that the download failed
        download_path = await download_by_file_handle(
            file_handle_id=entity.dataFileHandleId,
            synapse_id=object_id,
            entity_type=object_type,
            destination=download_path,
        )

        if download_path is None or not os.path.exists(download_path):
            return

    # converts the path format from forward slashes back to backward slashes on Windows
    entity.path = os.path.normpath(download_path)
    entity.files = [os.path.basename(download_path)]
    entity.cacheDir = os.path.dirname(download_path)


async def download_by_file_handle(
    file_handle_id: str,
    synapse_id: str,
    entity_type: str,
    destination: str,
    # TODO: Update this retries to be time based to match the upload logic
    retries: int = 5,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Download a file from the given URL to the local file system.

    Arguments:
        file_handle_id: The id of the FileHandle to download
        synapse_id:     The id of the Synapse object that uses the FileHandle e.g. "syn123"
        entity_type:   The type of the Synapse object that uses the FileHandle e.g. "FileEntity"
        destination:  The destination on local file system
        retries:      The Number of download retries attempted before throwing an exception.

    Returns:
        The path to downloaded file
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    while retries > 0:
        try:
            file_handle_result = await get_file_handle_for_download(
                file_handle_id=file_handle_id,
                synapse_id=synapse_id,
                entity_type=entity_type,
                synapse_client=client,
            )
            file_handle = file_handle_result["fileHandle"]
            concrete_type = file_handle["concreteType"]
            storage_location_id = file_handle.get("storageLocationId")

            if concrete_type == concrete_types.EXTERNAL_OBJECT_STORE_FILE_HANDLE:
                profile = get_client_authenticated_s3_profile(
                    endpoint=file_handle["endpointUrl"],
                    bucket=file_handle["bucket"],
                    config_path=client.configPath,
                )
                downloaded_path = S3ClientWrapper.download_file(
                    bucket=file_handle["bucket"],
                    endpoint_url=file_handle["endpointUrl"],
                    remote_file_key=file_handle["fileKey"],
                    download_file_path=destination,
                    profile_name=profile,
                    show_progress=not client.silent,
                )

            elif (
                sts_transfer.is_boto_sts_transfer_enabled(syn=client)
                and await sts_transfer.is_storage_location_sts_enabled_async(
                    syn=client, entity_id=synapse_id, location=storage_location_id
                )
                and concrete_type == concrete_types.S3_FILE_HANDLE
            ):
                # TODO: Some work is needed here to run these in a thread executor
                def download_fn(credentials):
                    return S3ClientWrapper.download_file(
                        bucket=file_handle["bucketName"],
                        endpoint_url=None,
                        remote_file_key=file_handle["key"],
                        download_file_path=destination,
                        credentials=credentials,
                        show_progress=not client.silent,
                        # pass through our synapse threading config to boto s3
                        transfer_config_kwargs={"max_concurrency": client.max_threads},
                    )

                downloaded_path = sts_transfer.with_boto_sts_credentials(
                    fn=download_fn,
                    syn=client,
                    entity_id=synapse_id,
                    permission="read_only",
                )

            elif (
                client.multi_threaded
                and concrete_type == concrete_types.S3_FILE_HANDLE
                and file_handle.get("contentSize", 0)
                > multithread_download.SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
            ):
                # run the download multi threaded if the file supports it, we're configured to do so,
                # and the file is large enough that it would be broken into parts to take advantage of
                # multiple downloading threads. otherwise it's more efficient to run the download as a simple
                # single threaded URL download.
                downloaded_path = await download_from_url_multi_threaded(
                    file_handle_id=file_handle_id,
                    object_id=synapse_id,
                    object_type=entity_type,
                    destination=destination,
                    expected_md5=file_handle.get("contentMd5"),
                    synapse_client=client,
                )

            else:
                downloaded_path = await download_from_url(
                    url=file_handle_result["preSignedURL"],
                    destination=destination,
                    file_handle_id=file_handle["id"],
                    expected_md5=file_handle.get("contentMd5"),
                    synapse_client=client,
                )
            client.cache.add(file_handle["id"], downloaded_path)
            return downloaded_path

        except Exception as ex:
            if not is_retryable_download_error(ex):
                raise

            exc_info = sys.exc_info()
            ex.progress = 0 if not hasattr(ex, "progress") else ex.progress
            client.logger.debug(
                f"\nRetrying download on error: [{exc_info[0]}] after progressing {ex.progress} bytes",
                exc_info=True,
            )  # this will include stack trace
            if ex.progress == 0:  # No progress was made reduce remaining retries.
                retries -= 1
            if retries <= 0:
                # Re-raise exception
                raise

    raise Exception("should not reach this line")


async def download_from_url_multi_threaded(
    file_handle_id: str,
    object_id: str,
    object_type: str,
    destination: str,
    *,
    expected_md5: str = None,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Download a file from the given URL using multiple threads.

    Arguments:
        file_handle_id: The id of the FileHandle to download
        object_id:      The id of the Synapse object that uses the FileHandle e.g. "syn123"
        object_type:    The type of the Synapse object that uses the FileHandle e.g. "FileEntity"
        destination:    The destination on local file system
        expected_md5:   The expected MD5
    Raises:
        SynapseMd5MismatchError: If the actual MD5 does not match expected MD5.

    Returns:
        The path to downloaded file
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    destination = os.path.abspath(destination)
    temp_destination = utils.temp_download_filename(
        destination=destination, file_handle_id=file_handle_id
    )

    request = multithread_download.DownloadRequest(
        file_handle_id=int(file_handle_id),
        object_id=object_id,
        object_type=object_type,
        path=temp_destination,
        debug=client.debug,
    )

    multithread_download.download_file(client=client, download_request=request)

    if expected_md5:  # if md5 not set (should be the case for all except http download)
        actual_md5 = utils.md5_for_file(temp_destination).hexdigest()
        # check md5 if given
        if actual_md5 != expected_md5:
            try:
                os.remove(temp_destination)
            except FileNotFoundError:
                # file already does not exist. nothing to do
                pass
            raise SynapseMd5MismatchError(
                f"Downloaded file {temp_destination}'s md5 {actual_md5} does not match expected MD5 of {expected_md5}"
            )
    # once download completed, rename to desired destination
    shutil.move(temp_destination, destination)

    return destination


async def download_from_url(
    url: str,
    destination: str,
    file_handle_id: Optional[str] = None,
    expected_md5: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Download a file from the given URL to the local file system.

    Arguments:
        url:           The source of download
        destination:   The destination on local file system
        file_handle_id:  Optional. If given, the file will be given a temporary name that includes the file
                                handle id which allows resuming partial downloads of the same file from previous
                                sessions
        expected_md5:  Optional. If given, check that the MD5 of the downloaded file matches the expected MD5

    Raises:
        IOError:                 If the local file does not exist.
        SynapseError:            If fail to download the file.
        SynapseHTTPError:        If there are too many redirects.
        SynapseMd5MismatchError: If the actual MD5 does not match expected MD5.

    Returns:
        The path to downloaded file
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    destination = os.path.abspath(destination)
    actual_md5 = None
    redirect_count = 0
    delete_on_md5_mismatch = True
    client.logger.debug(f"Downloading from {url} to {destination}")
    while redirect_count < REDIRECT_LIMIT:
        redirect_count += 1
        scheme = urllib_urlparse.urlparse(url).scheme
        if scheme == "file":
            delete_on_md5_mismatch = False
            destination = utils.file_url_to_path(url, verify_exists=True)
            if destination is None:
                raise IOError(f"Local file ({url}) does not exist.")
            break
        elif scheme == "sftp":
            username, password = client._getUserCredentials(url)
            destination = SFTPWrapper.download_file(
                url=url,
                localFilepath=destination,
                username=username,
                password=password,
                show_progress=not client.silent,
            )
            break
        elif scheme == "ftp":
            transfer_start_time = time.time()

            def _ftp_report_hook(
                block_number: int, read_size: int, total_size: int
            ) -> None:
                show_progress = not client.silent
                if show_progress:
                    client._print_transfer_progress(
                        transferred=block_number * read_size,
                        toBeTransferred=total_size,
                        prefix="Downloading ",
                        postfix=os.path.basename(destination),
                        dt=time.time() - transfer_start_time,
                    )

            urllib_request.urlretrieve(
                url=url, filename=destination, reporthook=_ftp_report_hook
            )
            break
        elif scheme == "http" or scheme == "https":
            # if a partial download exists with the temporary name,
            temp_destination = utils.temp_download_filename(
                destination=destination, file_handle_id=file_handle_id
            )
            range_header = (
                {"Range": f"bytes={os.path.getsize(filename=temp_destination)}-"}
                if os.path.exists(temp_destination)
                else {}
            )

            # pass along synapse auth credentials only if downloading directly from synapse
            auth = (
                client.credentials
                if is_synapse_uri(uri=url, synapse_client=client)
                else None
            )
            # TODO: Some more work is needed for streaming this data:
            # https://www.python-httpx.org/quickstart/#streaming-responses
            # response = await client.rest_get_async(
            #     uri=url,
            #     headers=client._generate_headers(range_header),
            #     stream=True,
            #     allow_redirects=False,
            #     auth=auth,
            # )
            response = with_retry(
                lambda: client._requests_session.get(
                    url,
                    headers=client._generate_headers(range_header),
                    stream=True,
                    allow_redirects=False,
                    auth=auth,
                ),
                verbose=client.debug,
                **STANDARD_RETRY_PARAMS,
            )
            try:
                exceptions._raise_for_status(response, verbose=client.debug)
            except SynapseHTTPError as err:
                if err.response.status_code == 404:
                    raise SynapseError(f"Could not download the file at {url}") from err
                elif (
                    err.response.status_code == 416
                ):  # Requested Range Not Statisfiable
                    # this is a weird error when the client already finished downloading but the loop continues
                    # When this exception occurs, the range we request is guaranteed to be >= file size so we
                    # assume that the file has been fully downloaded, rename it to destination file
                    # and break out of the loop to perform the MD5 check.
                    # If it fails the user can retry with another download.
                    shutil.move(temp_destination, destination)
                    break
                raise

            # handle redirects
            if response.status_code in [301, 302, 303, 307, 308]:
                url = response.headers["location"]
                # don't break, loop again
            else:
                # get filename from content-disposition, if we don't have it already
                if os.path.isdir(destination):
                    filename = utils.extract_filename(
                        content_disposition_header=response.headers.get(
                            "content-disposition", None
                        ),
                        default_filename=utils.guess_file_name(url),
                    )
                    destination = os.path.join(destination, filename)
                # Stream the file to disk
                if "content-length" in response.headers:
                    to_be_transferred = float(response.headers["content-length"])
                else:
                    to_be_transferred = -1
                transferred = 0

                # Servers that respect the Range header return 206 Partial Content
                if response.status_code == 206:
                    mode = "ab"
                    previously_transferred = os.path.getsize(filename=temp_destination)
                    to_be_transferred += previously_transferred
                    transferred += previously_transferred
                    sig = utils.md5_for_file(filename=temp_destination)
                else:
                    mode = "wb"
                    previously_transferred = 0
                    sig = hashlib.new("md5", usedforsecurity=False)

                try:
                    with open(temp_destination, mode) as fd:
                        t0 = time.time()
                        for _, chunk in enumerate(
                            response.iter_content(FILE_BUFFER_SIZE)
                        ):
                            fd.write(chunk)
                            sig.update(chunk)

                            # the 'content-length' header gives the total number of bytes that will be transferred
                            # to us len(chunk) cannot be used to track progress because iter_content automatically
                            # decodes the chunks if the response body is encoded so the len(chunk) could be
                            # different from the total number of bytes we've read read from the response body
                            # response.raw.tell() is the total number of response body bytes transferred over the
                            # wire so far
                            transferred = response.raw.tell() + previously_transferred
                            client._print_transfer_progress(
                                transferred,
                                to_be_transferred,
                                "Downloading ",
                                os.path.basename(destination),
                                dt=time.time() - t0,
                            )
                except (
                    Exception
                ) as ex:  # We will add a progress parameter then push it back to retry.
                    ex.progress = transferred - previously_transferred
                    raise

                # verify that the file was completely downloaded and retry if it is not complete
                if to_be_transferred > 0 and transferred < to_be_transferred:
                    client.logger.warning(
                        "\nRetrying download because the connection ended early.\n"
                    )
                    continue

                actual_md5 = sig.hexdigest()
                # rename to final destination
                shutil.move(temp_destination, destination)
                break
        else:
            client.logger.error(f"Unable to download URLs of type {scheme}")
            return None

    else:  # didn't break out of loop
        raise SynapseHTTPError("Too many redirects")

    if (
        actual_md5 is None
    ):  # if md5 not set (should be the case for all except http download)
        actual_md5 = utils.md5_for_file(destination).hexdigest()

    # check md5 if given
    if expected_md5 and actual_md5 != expected_md5:
        if delete_on_md5_mismatch and os.path.exists(destination):
            os.remove(destination)
        raise SynapseMd5MismatchError(
            f"Downloaded file {destination}'s md5 {actual_md5} does not match expected MD5 of {expected_md5}"
        )

    return destination


def is_retryable_download_error(ex: Exception) -> bool:
    """
    Check if the download error is retryable

    Arguments:
        ex: An exception

    Returns:
        Boolean value indicating whether the download error is retryable
    """
    # some exceptions caught during download indicate non-recoverable situations that
    # will not be remedied by a repeated download attempt.
    return not (
        (isinstance(ex, OSError) and ex.errno == errno.ENOSPC)
        or isinstance(ex, SynapseMd5MismatchError)  # out of disk space
    )


def resolve_download_path_collisions(
    download_location: str,
    file_name: str,
    if_collision: str,
    synapse_cache_location: str,
    cached_file_path: str,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Resolve file path collisions

    Arguments:
        download_location:      The download location
        file_name:             The file name
        if_collision:           Determines how to handle file collisions.
                                May be "overwrite.local", "keep.local", or "keep.both".
        synapse_cache_location: The location in .synapseCache where the file would be
                                corresponding to its FileHandleId.
        cached_file_path:      The file path of the cached copy

    Raises:
        ValueError: Invalid ifcollision. Should be "overwrite.local", "keep.local", or "keep.both".

    Returns:
        The download file path with collisions resolved
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # always overwrite if we are downloading to .synapseCache
    if utils.normalize_path(download_location) == synapse_cache_location:
        if if_collision is not None and if_collision != "overwrite.local":
            client.logger.warning(
                "\n"
                + "!" * 50
                + f"\nifcollision={if_collision} "
                + "is being IGNORED because the download destination is synapse's cache."
                ' Instead, the behavior is "overwrite.local". \n' + "!" * 50 + "\n"
            )
        if_collision = "overwrite.local"
    # if ifcollision not specified, keep.local
    if_collision = if_collision or "keep.both"

    download_path = utils.normalize_path(os.path.join(download_location, file_name))
    # resolve collison
    if os.path.exists(path=download_path):
        if if_collision == "overwrite.local":
            pass
        elif if_collision == "keep.local":
            # Don't want to overwrite the local file.
            return None
        elif if_collision == "keep.both":
            if download_path != cached_file_path:
                return utils.unique_filename(download_path)
        else:
            raise ValueError(
                f'Invalid parameter: "{if_collision}" is not a valid value for "ifcollision"'
            )
    return download_path


def ensure_download_location_is_directory(download_location: str) -> str:
    """
    Check if the download location is a directory

    Arguments:
        download_location: The download location

    Raises:
        ValueError: If the download_location is not a directory

    Returns:
        The download location
    """
    download_dir = os.path.expandvars(os.path.expanduser(download_location))
    if os.path.isfile(download_dir):
        raise ValueError(
            "Parameter 'download_location' should be a directory, not a file."
        )
    return download_dir


def is_synapse_uri(
    uri: str,
    synapse_client: Optional["Synapse"] = None,
) -> bool:
    """
    Check whether the given uri is hosted at the configured Synapse repo endpoint

    Arguments:
        uri: A given uri

    Returns:
        A boolean value indicating whether the given uri is hosted at the configured Synapse repo endpoint
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri_domain = urllib_urlparse.urlparse(uri).netloc
    synapse_repo_domain = urllib_urlparse.urlparse(client.repoEndpoint).netloc
    return uri_domain.lower() == synapse_repo_domain.lower()