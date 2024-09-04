"""This module handles the various ways that a user can download a file to Synapse."""

import asyncio
import datetime
import errno
import hashlib
import os
import shutil
import sys
import urllib.parse as urllib_urlparse
import urllib.request as urllib_request
from typing import TYPE_CHECKING, Dict, Optional, Union

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from synapseclient.api.configuration_services import get_client_authenticated_s3_profile
from synapseclient.api.file_services import (
    get_file_handle_for_download,
    get_file_handle_for_download_async,
)
from synapseclient.core import exceptions, sts_transfer, utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.constants.method_flags import (
    COLLISION_KEEP_BOTH,
    COLLISION_KEEP_LOCAL,
    COLLISION_OVERWRITE_LOCAL,
)
from synapseclient.core.download import (
    SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE,
    DownloadRequest,
    PresignedUrlProvider,
    _pre_signed_url_expiration_time,
    download_file,
)
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseHTTPError,
    SynapseMd5MismatchError,
)
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
from synapseclient.core.retry import (
    DEFAULT_RETRY_STATUS_CODES,
    RETRYABLE_CONNECTION_ERRORS,
    RETRYABLE_CONNECTION_EXCEPTIONS,
    with_retry,
)
from synapseclient.core.transfer_bar import (
    close_download_progress_bar,
    get_or_create_download_progress_bar,
    increment_progress_bar,
    increment_progress_bar_total,
)
from synapseclient.core.utils import MB

if TYPE_CHECKING:
    from synapseclient import Entity, Synapse
    from synapseclient.models import File

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
    download_location: str,
    entity: "Entity",
    if_collision: str,
    submission: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Download file entity

    Arguments:
        download_location: The location on disk where the entity will be downloaded. If
            there is a matching file at the location, the download collision will be
            handled according to the `if_collision` argument.
        entity:           The Synapse Entity object
        if_collision:      Determines how to handle file collisions.
                            May be

            - `overwrite.local`
            - `keep.local`
            - `keep.both`

        submission:       Access associated files through a submission rather than through an entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.
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
            client.logger.info(
                f"Copying existing file from {cached_file_path} to {download_path}"
            )
            shutil.copy(cached_file_path, download_path)

    else:  # download the file from URL (could be a local file)
        object_type = "FileEntity" if submission is None else "SubmissionAttachment"
        object_id = entity["id"] if submission is None else submission

        # reassign downloadPath because if url points to local file (e.g. file://~/someLocalFile.txt)
        # it won't be "downloaded" and, instead, downloadPath will just point to '~/someLocalFile.txt'
        # _downloadFileHandle may also return None to indicate that the download failed
        with logging_redirect_tqdm(loggers=[client.logger]):
            download_path = await download_by_file_handle(
                file_handle_id=entity.dataFileHandleId,
                synapse_id=object_id,
                entity_type=object_type,
                destination=download_path,
                synapse_client=client,
            )

        if download_path is None or not os.path.exists(download_path):
            return

    # converts the path format from forward slashes back to backward slashes on Windows
    entity.path = os.path.normpath(download_path)
    entity.files = [os.path.basename(download_path)]
    entity.cacheDir = os.path.dirname(download_path)


async def download_file_entity_model(
    download_location: Union[str, None],
    file: "File",
    if_collision: str,
    submission: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Download file entity

    Arguments:
        download_location: The location on disk where the entity will be downloaded. If
            there is a matching file at the location, the download collision will be
            handled according to the `if_collision` argument.
        entity:           The File object
        if_collision:      Determines how to handle file collisions.
                            May be

            - `overwrite.local`
            - `keep.local`
            - `keep.both`

        submission:       Access associated files through a submission rather than through an entity.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    # set the initial local state
    file.path = None

    # check to see if an UNMODIFIED version of the file (since it was last downloaded) already exists
    # this location could be either in .synapseCache or a user specified location to which the user previously
    # downloaded the file
    cached_file_path = client.cache.get(
        file_handle_id=file.data_file_handle_id, path=download_location
    )

    # location in .synapseCache where the file would be corresponding to its FileHandleId
    synapse_cache_location = client.cache.get_cache_dir(
        file_handle_id=file.data_file_handle_id
    )

    file_name = (
        file.file_handle.file_name
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
            client.logger.info(
                f"Copying existing file from {cached_file_path} to {download_path}"
            )
            shutil.copy(cached_file_path, download_path)

    else:  # download the file from URL (could be a local file)
        object_type = "FileEntity" if submission is None else "SubmissionAttachment"
        object_id = file.id if submission is None else submission

        # reassign downloadPath because if url points to local file (e.g. file://~/someLocalFile.txt)
        # it won't be "downloaded" and, instead, downloadPath will just point to '~/someLocalFile.txt'
        # _downloadFileHandle may also return None to indicate that the download failed
        with logging_redirect_tqdm(loggers=[client.logger]):
            download_path = await download_by_file_handle(
                file_handle_id=file.data_file_handle_id,
                synapse_id=object_id,
                entity_type=object_type,
                destination=download_path,
                synapse_client=client,
            )

        if download_path is None or not os.path.exists(download_path):
            return

    # converts the path format from forward slashes back to backward slashes on Windows
    file.path = os.path.normpath(download_path)


def _get_aws_credentials() -> None:
    """This is a stub function and only used for testing purposes."""
    return None


async def download_by_file_handle(
    file_handle_id: str,
    synapse_id: str,
    entity_type: str,
    destination: str,
    retries: int = 5,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> str:
    """
    Download a file from the given URL to the local file system.

    Arguments:
        file_handle_id: The id of the FileHandle to download
        synapse_id: The id of the Synapse object that uses the FileHandle e.g. "syn123"
        entity_type: The type of the Synapse object that uses the FileHandle e.g. "FileEntity"
        destination: The destination on local file system
        retries: The Number of download retries attempted before throwing an exception.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The path to downloaded file


    ```mermaid
    sequenceDiagram
        title Multi-Threaded Download Process with Retry Mechanism

        actor Client as Client
        participant download_functions as download_functions
        participant download_async as download_async
        participant download_execution as download_execution
        participant multi_threaded_download as multi_threaded_download
        participant remote_storage_server as remote_storage_server
        participant file as file

        activate Client
        Client ->> download_functions: download_by_file_handle
        activate download_functions

        loop retryable

            alt Download type = multi_threaded
                note over download_functions: download_from_url_multi_threaded

                download_functions ->> download_async: download_file
                activate download_async

                download_async ->> download_async: _generate_stream_and_write_chunk_tasks


                loop for each download task
                    download_async ->> download_execution: _execute_download_tasks
                    activate download_execution

                    par MULTI-THREADED: Run in thread executor
                        download_execution ->> multi_threaded_download: _stream_and_write_chunk
                        activate multi_threaded_download

                        loop stream chunk into memory
                            multi_threaded_download ->> remote_storage_server: stream chunk from remote server
                            remote_storage_server -->> multi_threaded_download: Return partial range
                        end

                        note over multi_threaded_download: Chunk loaded into memory

                        alt obtain thread lock [Failed]
                            note over multi_threaded_download: Wait to obtain lock
                        else obtain thread lock [Success]
                            multi_threaded_download ->> file: write chunk to file
                            file -->> multi_threaded_download: .
                            note over multi_threaded_download: Update progress bar
                            note over multi_threaded_download: Release lock
                        end
                        multi_threaded_download -->> download_execution: .
                    end
                    download_execution -->> download_async: .
                    note over download_async: Run garbage collection every 100 iterations
                    deactivate multi_threaded_download
                    deactivate download_execution
                end

                download_async -->> download_functions: .
                deactivate download_async

                download_functions ->> download_functions: md5_for_file
                download_functions -->> Client: File downloaded
                deactivate download_functions
            else Download type = non multi_threaded
                note over download_functions: Execute `download_from_url`
            else Download type = external s3 object store
                note over download_functions: Execute `S3ClientWrapper.download_file`
            else Download type = aws s3 sts storage
                note over download_functions: Execute `S3ClientWrapper.download_file` with with_boto_sts_credentials
            end
        end

        deactivate Client
    ```
    """
    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    while retries > 0:
        try:
            file_handle_result: Dict[
                str, str
            ] = await get_file_handle_for_download_async(
                file_handle_id=file_handle_id,
                synapse_id=synapse_id,
                entity_type=entity_type,
                synapse_client=syn,
            )
            file_handle = file_handle_result["fileHandle"]
            concrete_type = file_handle["concreteType"]
            storage_location_id = file_handle.get("storageLocationId")

            if concrete_type == concrete_types.EXTERNAL_OBJECT_STORE_FILE_HANDLE:
                profile = get_client_authenticated_s3_profile(
                    endpoint=file_handle["endpointUrl"],
                    bucket=file_handle["bucket"],
                    config_path=syn.configPath,
                )

                progress_bar = get_or_create_download_progress_bar(
                    file_size=1, postfix=synapse_id, synapse_client=syn
                )
                loop = asyncio.get_running_loop()
                downloaded_path = await loop.run_in_executor(
                    syn._get_thread_pool_executor(asyncio_event_loop=loop),
                    lambda: S3ClientWrapper.download_file(
                        bucket=file_handle["bucket"],
                        endpoint_url=file_handle["endpointUrl"],
                        remote_file_key=file_handle["fileKey"],
                        download_file_path=destination,
                        profile_name=profile,
                        credentials=_get_aws_credentials(),
                        progress_bar=progress_bar,
                    ),
                )

            elif (
                sts_transfer.is_boto_sts_transfer_enabled(syn=syn)
                and await sts_transfer.is_storage_location_sts_enabled_async(
                    syn=syn, entity_id=synapse_id, location=storage_location_id
                )
                and concrete_type == concrete_types.S3_FILE_HANDLE
            ):
                progress_bar = get_or_create_download_progress_bar(
                    file_size=1, postfix=synapse_id, synapse_client=syn
                )

                def download_fn(
                    credentials: Dict[str, str],
                    file_handle: Dict[str, str] = file_handle,
                ) -> str:
                    """Use the STS credentials to download the file from S3.

                    Arguments:
                        credentials: The STS credentials

                    Returns:
                        The path to the downloaded file
                    """
                    return S3ClientWrapper.download_file(
                        bucket=file_handle["bucketName"],
                        endpoint_url=None,
                        remote_file_key=file_handle["key"],
                        download_file_path=destination,
                        credentials=credentials,
                        progress_bar=progress_bar,
                        # pass through our synapse threading config to boto s3
                        transfer_config_kwargs={"max_concurrency": syn.max_threads},
                    )

                loop = asyncio.get_running_loop()
                downloaded_path = await loop.run_in_executor(
                    syn._get_thread_pool_executor(asyncio_event_loop=loop),
                    lambda: sts_transfer.with_boto_sts_credentials(
                        download_fn, syn, synapse_id, "read_only"
                    ),
                )

            elif (
                syn.multi_threaded
                and concrete_type == concrete_types.S3_FILE_HANDLE
                and file_handle.get("contentSize", 0)
                > SYNAPSE_DEFAULT_DOWNLOAD_PART_SIZE
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
                    synapse_client=syn,
                )

            else:
                loop = asyncio.get_running_loop()
                progress_bar = get_or_create_download_progress_bar(
                    file_size=1, postfix=synapse_id, synapse_client=syn
                )

                downloaded_path = await loop.run_in_executor(
                    syn._get_thread_pool_executor(asyncio_event_loop=loop),
                    lambda: download_from_url(
                        url=file_handle_result["preSignedURL"],
                        destination=destination,
                        entity_id=synapse_id,
                        file_handle_associate_type=entity_type,
                        file_handle_id=file_handle["id"],
                        expected_md5=file_handle.get("contentMd5"),
                        progress_bar=progress_bar,
                        synapse_client=syn,
                    ),
                )

            syn.logger.info(f"Downloaded {synapse_id} to {downloaded_path}")
            syn.cache.add(
                file_handle["id"], downloaded_path, file_handle.get("contentMd5", None)
            )
            close_download_progress_bar()
            return downloaded_path

        except Exception as ex:
            if not is_retryable_download_error(ex):
                close_download_progress_bar()
                raise

            exc_info = sys.exc_info()
            ex.progress = 0 if not hasattr(ex, "progress") else ex.progress
            syn.logger.debug(
                f"\nRetrying download on error: [{exc_info[0]}] after progressing {ex.progress} bytes",
                exc_info=True,
            )  # this will include stack trace
            if ex.progress == 0:  # No progress was made reduce remaining retries.
                retries -= 1
            if retries <= 0:
                close_download_progress_bar()
                # Re-raise exception
                raise

    close_download_progress_bar()
    raise RuntimeError("should not reach this line")


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
        object_id:      The id of the Synapse object that uses the FileHandle
            e.g. "syn123"
        object_type:    The type of the Synapse object that uses the
            FileHandle e.g. "FileEntity". Any of
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html>
        destination:    The destination on local file system
        expected_md5:   The expected MD5
        content_size:   The size of the content
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.
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

    request = DownloadRequest(
        file_handle_id=int(file_handle_id),
        object_id=object_id,
        object_type=object_type,
        path=temp_destination,
        debug=client.debug,
    )

    await download_file(client=client, download_request=request)

    if expected_md5:  # if md5 not set (should be the case for all except http download)
        actual_md5 = utils.md5_for_file_hex(filename=temp_destination)
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


def download_from_url(
    url: str,
    destination: str,
    entity_id: Optional[str],
    file_handle_associate_type: Optional[str],
    file_handle_id: Optional[str] = None,
    expected_md5: Optional[str] = None,
    progress_bar: Optional[tqdm] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Union[str, None]:
    """
    Download a file from the given URL to the local file system.

    Arguments:
        url:           The source of download
        destination:   The destination on local file system
        entity_id:      The id of the Synapse object that uses the FileHandle
            e.g. "syn123"
        file_handle_associate_type:    The type of the Synapse object that uses the
            FileHandle e.g. "FileEntity". Any of
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html>
        file_handle_id:  Optional. If given, the file will be given a temporary name that includes the file
                                handle id which allows resuming partial downloads of the same file from previous
                                sessions
        expected_md5:  Optional. If given, check that the MD5 of the downloaded file matches the expected MD5
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Raises:
        IOError:                 If the local file does not exist.
        SynapseError:            If fail to download the file.
        SynapseHTTPError:        If there are too many redirects.
        SynapseMd5MismatchError: If the actual MD5 does not match expected MD5.

    Returns:
        The path to downloaded file or None if the download failed
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
            if progress_bar is not None:
                file_size = os.path.getsize(destination)
                increment_progress_bar_total(total=file_size, progress_bar=progress_bar)
                increment_progress_bar(n=progress_bar.total, progress_bar=progress_bar)
            break
        elif scheme == "sftp":
            username, password = client._getUserCredentials(url)
            destination = SFTPWrapper.download_file(
                url=url,
                localFilepath=destination,
                username=username,
                password=password,
                progress_bar=progress_bar,
            )
            break
        elif scheme == "ftp":
            updated_progress_bar_with_total = False

            def _ftp_report_hook(
                _: int,
                read_size: int,
                total_size: int,
            ) -> None:
                """Report hook for urllib.request.urlretrieve to show download progress.

                Arguments:
                    _: The number of blocks transferred so far
                    read_size: The size of each block
                    total_size: The total size of the file

                Returns:
                    None
                """
                nonlocal updated_progress_bar_with_total
                if progress_bar is not None:
                    if not updated_progress_bar_with_total:
                        updated_progress_bar_with_total = True
                        increment_progress_bar_total(
                            total=total_size, progress_bar=progress_bar
                        )
                    increment_progress_bar(n=read_size, progress_bar=progress_bar)

            urllib_request.urlretrieve(
                url=url, filename=destination, reporthook=_ftp_report_hook
            )
            break
        elif scheme in ["http", "https"]:
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

            try:
                url_has_expiration = "Expires" in urllib_urlparse.urlparse(url).query
                url_is_expired = False
                if url_has_expiration:
                    url_is_expired = datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ) + PresignedUrlProvider._TIME_BUFFER >= _pre_signed_url_expiration_time(
                        url
                    )
                if url_is_expired:
                    response = get_file_handle_for_download(
                        file_handle_id=file_handle_id,
                        synapse_id=entity_id,
                        entity_type=file_handle_associate_type,
                        synapse_client=client,
                    )
                    url = response["preSignedURL"]
                response = with_retry(
                    lambda url=url, range_header=range_header, auth=auth: client._requests_session.get(
                        url=url,
                        headers=client._generate_headers(range_header),
                        stream=True,
                        allow_redirects=False,
                        auth=auth,
                    ),
                    verbose=client.debug,
                    **STANDARD_RETRY_PARAMS,
                )
                exceptions._raise_for_status(response, verbose=client.debug)
            except SynapseHTTPError as err:
                if err.response.status_code == 403:
                    url_has_expiration = (
                        "Expires" in urllib_urlparse.urlparse(url).query
                    )
                    url_is_expired = False
                    if url_has_expiration:
                        url_is_expired = datetime.datetime.now(
                            tz=datetime.timezone.utc
                        ) + PresignedUrlProvider._TIME_BUFFER >= _pre_signed_url_expiration_time(
                            url
                        )
                    if url_is_expired:
                        response = get_file_handle_for_download(
                            file_handle_id=file_handle_id,
                            synapse_id=entity_id,
                            entity_type=file_handle_associate_type,
                            synapse_client=client,
                        )
                        refreshed_url = response["preSignedURL"]
                        response = with_retry(
                            lambda url=refreshed_url, range_header=range_header, auth=auth: client._requests_session.get(
                                url=url,
                                headers=client._generate_headers(range_header),
                                stream=True,
                                allow_redirects=False,
                                auth=auth,
                            ),
                            verbose=client.debug,
                            **STANDARD_RETRY_PARAMS,
                        )
                    else:
                        raise
                elif err.response.status_code == 404:
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
                else:
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
                    increment_progress_bar_total(
                        total=to_be_transferred, progress_bar=progress_bar
                    )
                    increment_progress_bar(n=transferred, progress_bar=progress_bar)
                    client.logger.debug(
                        f"Resuming partial download to {temp_destination}. "
                        f"{previously_transferred}/{to_be_transferred} bytes already "
                        "transferred."
                    )
                    sig = utils.md5_for_file(filename=temp_destination)
                else:
                    mode = "wb"
                    previously_transferred = 0
                    increment_progress_bar_total(
                        total=to_be_transferred, progress_bar=progress_bar
                    )
                    sig = hashlib.new("md5", usedforsecurity=False)  # nosec

                try:
                    with open(temp_destination, mode) as fd:
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
                            increment_progress_bar(
                                n=len(chunk), progress_bar=progress_bar
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
        actual_md5 = utils.md5_for_file_hex(filename=destination)

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
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Union[str, None]:
    """
    Resolve file path collisions

    Arguments:
        download_location: The location on disk where the entity will be downloaded. If
            there is a matching file at the location, the download collision will be
            handled according to the `if_collision` argument.
        file_name:             The file name
        if_collision:           Determines how to handle file collisions.
                                May be "overwrite.local", "keep.local", or "keep.both".
        synapse_cache_location: The location in .synapseCache where the file would be
                                corresponding to its FileHandleId.
        cached_file_path:      The file path of the cached copy

    Raises:
        ValueError: Invalid ifcollision. Should be "overwrite.local", "keep.local", or "keep.both".

    Returns:
        The download file path with collisions resolved or None if the file should
        not be downloaded
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # always overwrite if we are downloading to .synapseCache
    if utils.normalize_path(download_location) == synapse_cache_location:
        if if_collision is not None and if_collision != COLLISION_OVERWRITE_LOCAL:
            client.logger.warning(
                "\n"
                + "!" * 50
                + f"\nifcollision={if_collision} "
                + "is being IGNORED because the download destination is synapse's cache."
                f' Instead, the behavior is "{COLLISION_OVERWRITE_LOCAL}". \n'
                + "!" * 50
                + "\n"
            )
        if_collision = COLLISION_OVERWRITE_LOCAL
    # if ifcollision not specified, keep.local
    if_collision = if_collision or COLLISION_KEEP_BOTH

    download_path = utils.normalize_path(os.path.join(download_location, file_name))
    # resolve collision
    if os.path.exists(path=download_path):
        if if_collision == COLLISION_OVERWRITE_LOCAL:
            pass  # Let the download proceed and overwrite the local file.
        elif if_collision == COLLISION_KEEP_LOCAL:
            client.logger.info(
                f"Found existing file at {download_path}, skipping download."
            )

            # Don't want to overwrite the local file.
            download_path = None
        elif if_collision == COLLISION_KEEP_BOTH:
            if download_path != cached_file_path:
                download_path = utils.unique_filename(download_path)
        else:
            raise ValueError(
                f'Invalid parameter: "{if_collision}" is not a valid value for "ifcollision"'
            )
    return download_path


def ensure_download_location_is_directory(download_location: str) -> str:
    """
    Check if the download location is a directory

    Arguments:
        download_location: The location on disk where the entity will be downloaded.

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
    *,
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
