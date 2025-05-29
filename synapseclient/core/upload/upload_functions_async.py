"""This module handles the various ways that a user can upload a file to Synapse."""

import asyncio
import os
import time
import threading
import urllib.parse as urllib_parse
import uuid
from typing import TYPE_CHECKING, Dict, Optional, Union

from opentelemetry import trace

from synapseclient.api import (
    get_client_authenticated_s3_profile,
    get_file_handle,
    get_upload_destination,
    post_external_filehandle,
    post_external_object_store_filehandle,
    post_external_s3_file_handle,
)
from synapseclient.core import sts_transfer, utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseMd5MismatchError
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
from synapseclient.core.upload.multipart_upload_async import multipart_upload_file_async
from synapseclient.core.utils import as_url, file_url_to_path, id_of, is_url
from synapseclient.core.telemetry_integration import TransferMonitor, monitored_transfer

if TYPE_CHECKING:
    from synapseclient import Synapse


async def upload_file_handle(
    syn: "Synapse",
    parent_entity_id: str,
    path: str,
    synapse_store: bool = True,
    md5: str = None,
    file_size: int = None,
    mimetype: str = None,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Union[str, int]]:
    """
    Uploads the file in the provided path (if necessary) to a storage location based
    on project settings. Returns a new FileHandle as a dict to represent the
    stored file.

    Arguments:
        syn: The synapse client
        parent_entity_id: The ID of the parent entity that the file will be attached to.
        path: The file path to the file being uploaded
        synapse_store: If False, will not upload the file, but instead create an
            ExternalFileHandle that references the file on the local machine. If True,
            will upload the file based on StorageLocation determined by the
            parent_entity_id.
        md5: The MD5 checksum for the file, if known. Otherwise if the file is a
            local file, it will be calculated automatically.
        file_size: The size the file, if known. Otherwise if the file is a local file,
            it will be calculated automatically.
        mimetype: The MIME type the file, if known. Otherwise if the file is a local
            file, it will be calculated automatically.

    Returns:
        A dictionary of a new FileHandle as a dict that represents the uploaded file
    """
    if path is None:
        raise ValueError("path can not be None")

    expanded_upload_path = os.path.expandvars(os.path.expanduser(path))
    if file_size is None and os.path.isfile(expanded_upload_path):
        file_size = os.path.getsize(expanded_upload_path)

    # Top-level monitored_transfer context for upload_file_handle
    with monitored_transfer(
        operation="upload_file_handle",
        file_path=path,
        file_size=file_size or 0,
        destination=f"parent:{parent_entity_id}",
        mime_type=mimetype,
        with_progress_bar=False,
        parent_id=parent_entity_id,
    ) as monitor:
        file_handle = None
        try:
            monitor.span.set_attribute("synapse.transfer.direction", "upload")
            monitor.span.set_attribute("synapse.file.size_bytes", file_size or 0)
            monitor.span.set_attribute("synapse.transfer.method", "auto_dispatch")
            monitor.span.set_attribute("synapse.file.path", path)

            # if doing a external file handle with no actual upload
            if not synapse_store:
                file_handle = await create_external_file_handle(
                    syn, path, mimetype=mimetype, md5=md5, file_size=file_size
                )
                monitor.span.set_attribute("synapse.transfer.status", "completed")
                monitor.span.set_attribute("synapse.file_handle_id", file_handle.get("id"))
                return file_handle

            # expand the path because past this point an upload is required and some upload functions require an absolute path
            if md5 is None and os.path.isfile(expanded_upload_path):
                md5 = utils.md5_for_file_hex(filename=expanded_upload_path)

            entity_parent_id = id_of(parent_entity_id)

            # determine the upload function based on the UploadDestination
            location = await get_upload_destination(
                entity_id=entity_parent_id, synapse_client=syn
            )
            upload_destination_type = location.get("concreteType", None) if location else None
            monitor.span.set_attribute("synapse.upload_destination_type", upload_destination_type)

            # Dispatch to the correct async upload implementation
            if (
                sts_transfer.is_boto_sts_transfer_enabled(syn)
                and await sts_transfer.is_storage_location_sts_enabled_async(
                    syn, entity_parent_id, location
                )
                and upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION
            ):
                file_handle = await upload_synapse_sts_boto_s3(
                    syn=syn,
                    parent_id=entity_parent_id,
                    upload_destination=location,
                    local_path=expanded_upload_path,
                    mimetype=mimetype,
                    md5=md5,
                    storage_str="Uploading file to external S3 storage using boto3",
                )
            elif upload_destination_type in (
                concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION,
                concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION,
                concrete_types.EXTERNAL_GCP_UPLOAD_DESTINATION,
            ):
                if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION:
                    storage_str = "Uploading to Synapse storage"
                elif upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION:
                    storage_str = "Uploading to your external S3 storage"
                else:
                    storage_str = "Uploading to your external Google Bucket storage"
                file_handle = await upload_synapse_s3(
                    syn=syn,
                    file_path=expanded_upload_path,
                    storage_location_id=location["storageLocationId"],
                    mimetype=mimetype,
                    md5=md5,
                    storage_str=storage_str,
                    transfer_monitor=monitor,
                )
            elif upload_destination_type == concrete_types.EXTERNAL_UPLOAD_DESTINATION:
                if location["uploadType"] == "SFTP":
                    storage_str = (
                        f"Uploading to: {urllib_parse.urlparse(location['url']).netloc}"
                    )
                    banner = location.get("banner", None)
                    if banner:
                        syn.logger.info(banner)
                    file_handle = await upload_external_file_handle_sftp(
                        syn=syn,
                        file_path=expanded_upload_path,
                        sftp_url=location["url"],
                        mimetype=mimetype,
                        md5=md5,
                        storage_str=storage_str,
                    )
                else:
                    raise NotImplementedError("Can only handle SFTP upload locations.")
            elif (
                upload_destination_type
                == concrete_types.EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION
            ):
                storage_str = (
                    f"Uploading to endpoint: [{location.get('endpointUrl')}] "
                    f"bucket: [{location.get('bucket')}]"
                )
                banner = location.get("banner", None)
                if banner:
                    syn.logger.info(banner)
                file_handle = await upload_client_auth_s3(
                    syn=syn,
                    file_path=expanded_upload_path,
                    bucket=location["bucket"],
                    endpoint_url=location["endpointUrl"],
                    key_prefix=location["keyPrefixUUID"],
                    storage_location_id=location["storageLocationId"],
                    mimetype=mimetype,
                    md5=md5,
                    storage_str=storage_str,
                )
            else:  # unknown storage location
                file_handle = await upload_synapse_s3(
                    syn=syn,
                    file_path=expanded_upload_path,
                    storage_location_id=None,
                    mimetype=mimetype,
                    md5=md5,
                    storage_str="Uploading to Synapse storage",
                )

            monitor.span.set_attribute("synapse.transfer.status", "completed")
            monitor.span.set_attribute("synapse.file_handle_id", file_handle.get("id"))
            return file_handle

        except Exception as ex:
            monitor.span.set_attribute("synapse.transfer.status", "failed")
            monitor.span.set_attribute("synapse.transfer.error_message", str(ex))
            monitor.span.set_attribute("synapse.transfer.error_type", type(ex).__name__)
            monitor.record_retry(error=ex)
            raise


async def create_external_file_handle(
    syn: "Synapse",
    path: str,
    mimetype: str = None,
    md5: str = None,
    file_size: int = None,
) -> Dict[str, Union[str, int]]:
    """Create a file handle in Synapse without uploading any files. This is used in
    cases where one wishes to store a reference to a file that is not in Synapse."""
    # Expand path
    expanded_path = os.path.expandvars(os.path.expanduser(path))

    # Determine file size if local file
    is_local_file = False
    if file_size is None and os.path.isfile(expanded_path):
        file_size = os.path.getsize(expanded_path)
        is_local_file = True

    with monitored_transfer(
        operation="create_external_handle",
        file_path=path,
        file_size=file_size or 0,
        destination="external:file_handle",
        mime_type=mimetype,
        with_progress_bar=False,
    ) as monitor:
        try:
            # Set external file attributes
            url = as_url(expanded_path)
            monitor.span.set_attribute("synapse.external.url", url)
            monitor.span.set_attribute("synapse.transfer.type", "external_reference")

            # Validate URL
            if is_url(url):
                parsed_url = urllib_parse.urlparse(url)
                parsed_path = file_url_to_path(url)

                if parsed_url.scheme == "file" and os.path.isfile(parsed_path):
                    # Local file reference
                    monitor.span.set_attribute("synapse.external.is_local", True)

                    # Calculate MD5 for local file
                    actual_md5 = utils.md5_for_file_hex(filename=parsed_path)
                    if md5 is not None and md5 != actual_md5:
                        raise SynapseMd5MismatchError(
                            f"The specified md5 [{md5}] does not match the calculated md5 "
                            f"[{actual_md5}] for local file [{parsed_path}]"
                        )
                    md5 = actual_md5
                    file_size = os.stat(parsed_path).st_size
                    is_local_file = True

                    monitor.file_size = file_size
            else:
                raise ValueError(f"externalUrl [{url}] is not a valid url")

            # No actual transfer for external file handles
            monitor.transferred_bytes = 0
            monitor.span.set_attribute("synapse.transfer.status", "completed")
            monitor.span.set_attribute("synapse.transfer.skip_reason", "external_reference")

            # Create the file handle
            file_handle = await post_external_filehandle(
                external_url=url, mimetype=mimetype, md5=md5, file_size=file_size
            )

            # Update final metrics
            monitor.span.set_attribute("synapse.file_handle_id", file_handle["id"])

            # Cache local files
            if is_local_file:
                syn.cache.add(
                    file_handle_id=file_handle["id"], path=file_url_to_path(url), md5=md5
                )
                monitor.record_cache_hit(True)

            return file_handle

        except Exception as ex:
            monitor.span.set_attribute("synapse.transfer.status", "failed")
            monitor.span.set_attribute("synapse.transfer.error_message", str(ex))
            monitor.span.set_attribute("synapse.transfer.error_type", type(ex).__name__)
            raise


async def upload_external_file_handle_sftp(
    syn: "Synapse",
    file_path: str,
    sftp_url: str,
    mimetype: str = None,
    md5: str = None,
    storage_str: str = None,
) -> Dict[str, Union[str, int]]:
    """Upload a file to an SFTP server and create a file handle in Synapse."""
    # Get file metadata
    file_size = os.path.getsize(file_path)
    parsed_url = urllib_parse.urlparse(sftp_url)
    destination = f"sftp:{parsed_url.netloc}:{parsed_url.path}"

    with monitored_transfer(
        operation="upload_sftp",
        file_path=file_path,
        file_size=file_size,
        destination=destination,
        mime_type=mimetype,
        storage_provider="sftp",
        host=parsed_url.netloc,
    ) as monitor:
        try:
            # Set SFTP-specific attributes
            monitor.span.set_attribute("synapse.sftp.host", parsed_url.netloc)
            monitor.span.set_attribute("synapse.sftp.url", sftp_url)
            monitor.span.set_attribute("synapse.transfer.protocol", "sftp")
            monitor.span.set_attribute("synapse.upload_destination_type", "EXTERNAL_UPLOAD_DESTINATION")

            # Get credentials
            username, password = syn._getUserCredentials(url=sftp_url)

            try:
                # Execute SFTP upload
                uploaded_url = SFTPWrapper.upload_file(
                    file_path,
                    urllib_parse.unquote(sftp_url),
                    username,
                    password,
                    storage_str=storage_str
                )

                # Ensure full file size is recorded
                # TODO: Is this needed?
                monitor.transferred_bytes = file_size

            except Exception:
                raise

            # Calculate MD5 if not provided
            file_md5 = md5 or utils.md5_for_file_hex(filename=file_path)

            # Create external file handle
            file_handle = await post_external_filehandle(
                external_url=uploaded_url,
                mimetype=mimetype,
                md5=file_md5,
                file_size=file_size
            )

            # Update final metrics
            monitor.span.set_attribute("synapse.file_handle_id", file_handle["id"])
            monitor.span.set_attribute("synapse.transfer.status", "completed")

            # Cache the uploaded file
            syn.cache.add(file_handle_id=file_handle["id"], path=file_path, md5=file_md5)

            return file_handle

        except Exception as ex:
            monitor.span.set_attribute("synapse.transfer.status", "failed")
            monitor.span.set_attribute("synapse.transfer.error_message", str(ex))
            monitor.span.set_attribute("synapse.transfer.error_type", type(ex).__name__)
            monitor.record_retry(error=ex)
            raise


async def upload_synapse_s3(
    syn: "Synapse",
    file_path: str,
    storage_location_id: Optional[int] = None,
    mimetype: str = None,
    force_restart: bool = False,
    md5: str = None,
    storage_str: str = None,
    progress_callback: Optional[callable] = None,
    transfer_monitor: Optional[TransferMonitor] = None,
) -> Dict[str, Union[str, int]]:
    """Upload a file to Synapse storage and create a file handle in Synapse.

    Argments:
        syn: The synapse client
        file_path: The path to the file to upload.
        storage_location_id: The storage location ID.
        mimetype: The mimetype of the file.
        force_restart: If True, will force the upload to restart.
        md5: The MD5 checksum for the file.
        storage_str: The storage string.

    Returns:
        A dictionary of the file handle.
    """
    # Get file size for telemetry tracking
    file_size = os.path.getsize(file_path)

    # Create a descriptive destination string
    destination = f"synapse:s3:{storage_location_id if storage_location_id else 'default'}"

    # Use monitored_transfer for OpenTelemetry tracing
    with monitored_transfer(
        operation="upload",
        file_path=file_path,
        file_size=file_size,
        destination=destination,
        with_progress_bar=False,  # Progress is handled by multipart upload
        md5=md5,
        multipart=True,
        storage_location_id=storage_location_id,
        force_restart=force_restart,
        reuse_monitor=transfer_monitor,
        storage_provider="s3",
    ) as monitor:
        try:
            # Track if an actual transfer occurs
            initial_transferred_bytes = monitor.transferred_bytes

            file_handle_id = await multipart_upload_file_async(
                syn=syn,
                file_path=file_path,
                content_type=mimetype,
                storage_location_id=storage_location_id,
                md5=md5,
                force_restart=force_restart,
                storage_str=storage_str,
                progress_callback=progress_callback,
            )

            if monitor.transferred_bytes == initial_transferred_bytes:
                monitor.transferred_bytes = 0
                monitor.span.set_attribute("synapse.file.transfer_size_bytes", 0)
            else:
                # Only set to file_size if actual transfer happened
                monitor.transferred_bytes = file_size

            # Cache the file
            syn.cache.add(file_handle_id=file_handle_id, path=file_path, md5=md5)
            # Add file handle ID to span
            monitor.span.set_attribute("synapse.file_handle_id", file_handle_id)
            # Get and return the file handle
            file_handle = await get_file_handle(file_handle_id=file_handle_id, synapse_client=syn)
            return file_handle
        except Exception as ex:
            monitor.record_retry(error=ex)
            raise


def _get_aws_credentials() -> None:
    """This is a stub function and only used for testing purposes."""
    return None


async def upload_synapse_sts_boto_s3(
    syn: "Synapse",
    parent_id: str,
    upload_destination: Dict[str, Union[str, int]],
    local_path: str,
    mimetype: str = None,
    md5: str = None,
    storage_str: str = None,
) -> Dict[str, Union[str, int, bool]]:
    """
    When uploading to Synapse storage normally the back end will generate a random prefix
    for our uploaded object. Since in this case the client is responsible for the remote
    key, the client will instead generate a random prefix. this both ensures we don't have a collision
    with an existing S3 object and also mitigates potential performance issues, although
    key locality performance issues are likely resolved as of:
    <https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/>

    Arguments:
        syn: The synapse client
        parent_id: The synapse ID of the parent.
        upload_destination: The upload destination
        local_path: The local path to the file to upload.
        mimetype: The mimetype is known. Defaults to None.
        md5: MD5 checksum for the file, if known.

    Returns:
        _description_
    """
    # Get file metadata for telemetry
    file_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
    bucket_name = upload_destination["bucket"]
    storage_location_id = upload_destination["storageLocationId"]

    # Create descriptive destination string
    destination = f"synapse:s3:sts:{bucket_name}:{parent_id}"

    with monitored_transfer(
        operation="upload_sts_s3",
        file_path=local_path,
        file_size=file_size,
        destination=destination,
        mime_type=mimetype,
        storage_provider="s3",
        parent_id=parent_id,
        bucket=bucket_name,
        storage_location_id=storage_location_id,
    ) as monitor:
        try:
            # Set S3-specific attributes
            monitor.span.set_attribute("synapse.s3.bucket", bucket_name)
            monitor.span.set_attribute("synapse.s3.storage_location_id", storage_location_id)
            monitor.span.set_attribute("synapse.transfer.auth_method", "sts")
            monitor.span.set_attribute("synapse.upload_destination_type", "EXTERNAL_S3_UPLOAD_DESTINATION")

            # Track STS credential fetch time
            sts_start = time.time()

            key_prefix = str(uuid.uuid4())
            remote_file_key = "/".join([
                upload_destination.get("baseKey", ""),
                key_prefix,
                os.path.basename(local_path),
            ])
            monitor.span.set_attribute("synapse.s3.key", remote_file_key)

            # Create progress callback that updates monitor
            progress_callback = create_thread_safe_progress_callback(monitor)

            def upload_fn(credentials: Dict[str, str]) -> str:
                # Record STS credential fetch duration
                sts_duration = time.time() - sts_start
                monitor.span.set_attribute("synapse.auth.sts_duration_seconds", sts_duration)

                return S3ClientWrapper.upload_file(
                    bucket=bucket_name,
                    endpoint_url=None,
                    remote_file_key=remote_file_key,
                    upload_file_path=local_path,                    credentials=credentials,
                    transfer_config_kwargs={"max_concurrency": syn.max_threads},
                    storage_str=storage_str,
                    progress_callback=progress_callback
                )

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                syn._get_thread_pool_executor(asyncio_event_loop=loop),
                lambda: sts_transfer.with_boto_sts_credentials(
                    upload_fn, syn, parent_id, "read_write"
                )
            )

            # Create file handle
            result = await post_external_s3_file_handle(
                bucket_name=bucket_name,
                s3_file_key=remote_file_key,
                file_path=local_path,
                storage_location_id=storage_location_id,
                mimetype=mimetype,
                md5=md5,
                synapse_client=syn
            )

            # Update final metrics
            monitor.transferred_bytes = file_size
            monitor.span.set_attribute("synapse.file_handle_id", result["id"])
            monitor.span.set_attribute("synapse.transfer.status", "completed")

            # Cache the uploaded file
            if md5 is None:
                md5 = utils.md5_for_file_hex(filename=local_path)
            syn.cache.add(file_handle_id=result["id"], path=local_path, md5=md5)

            return result

        except Exception as ex:
            monitor.span.set_attribute("synapse.transfer.status", "failed")
            monitor.span.set_attribute("synapse.transfer.error_message", str(ex))
            monitor.span.set_attribute("synapse.transfer.error_type", type(ex).__name__)
            monitor.record_retry(error=ex)
            raise


async def upload_client_auth_s3(
    syn: "Synapse",
    file_path: str,
    bucket: str,
    endpoint_url: str,
    key_prefix: str,
    storage_location_id: int,
    mimetype: str = None,
    md5: str = None,
    storage_str: str = None,
) -> Dict[str, Union[str, int]]:
    """Use the S3 client to upload a file to an S3 bucket."""
    # Get file metadata
    file_size = os.path.getsize(file_path)

    # Create descriptive destination
    destination = f"external:s3:{endpoint_url}:{bucket}"

    with monitored_transfer(
        operation="upload_external_s3",
        file_path=file_path,
        file_size=file_size,
        destination=destination,
        mime_type=mimetype,
        storage_provider="s3",
        endpoint_url=endpoint_url,
        bucket=bucket,
        storage_location_id=storage_location_id,
    ) as monitor:
        try:
            # Set external S3 attributes
            monitor.span.set_attribute("synapse.s3.bucket", bucket)
            monitor.span.set_attribute("synapse.s3.endpoint", endpoint_url)
            monitor.span.set_attribute("synapse.transfer.auth_method", "client_auth")
            monitor.span.set_attribute("synapse.upload_destination_type", "EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION")

            # Get S3 profile
            profile = get_client_authenticated_s3_profile(
                endpoint=endpoint_url, bucket=bucket, config_path=syn.configPath
            )
            monitor.span.set_attribute("synapse.s3.profile", profile)

            # Create file key
            file_key = key_prefix + "/" + os.path.basename(file_path)
            monitor.span.set_attribute("synapse.s3.key", file_key)

            # Create thread-safe progress callback
            progress_callback = create_thread_safe_progress_callback(monitor)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                syn._get_thread_pool_executor(asyncio_event_loop=loop),
                lambda: S3ClientWrapper.upload_file(
                    bucket=bucket,
                    endpoint_url=endpoint_url,
                    remote_file_key=file_key,
                    upload_file_path=file_path,
                    profile_name=profile,
                    credentials=_get_aws_credentials(),
                    storage_str=storage_str,
                    progress_callback=progress_callback
                )
            )

            # Create file handle
            file_handle = await post_external_object_store_filehandle(
                s3_file_key=file_key,
                file_path=file_path,
                storage_location_id=storage_location_id,
                mimetype=mimetype,
                md5=md5,
                synapse_client=syn
            )

            # Update final metrics
            monitor.transferred_bytes = file_size
            monitor.span.set_attribute("synapse.file_handle_id", file_handle["id"])
            monitor.span.set_attribute("synapse.transfer.status", "completed")

            # Cache the uploaded file
            if md5 is None:
                md5 = utils.md5_for_file_hex(filename=file_path)
            syn.cache.add(file_handle_id=file_handle["id"], path=file_path, md5=md5)

            return file_handle

        except Exception as ex:
            monitor.span.set_attribute("synapse.transfer.status", "failed")
            monitor.span.set_attribute("synapse.transfer.error_message", str(ex))
            monitor.span.set_attribute("synapse.transfer.error_type", type(ex).__name__)
            monitor.record_retry(error=ex)
            raise


# Thread-safe progress callback helper
def create_thread_safe_progress_callback(monitor):
    """
    Create a thread-safe progress callback that properly tracks per-thread progress.

    This is essential for multi-threaded uploads where multiple threads may be
    uploading different parts of the file simultaneously.

    Args:
        monitor: The TransferMonitor instance to update with progress

    Returns:
        A callback function that can be passed to S3 upload methods
    """
    def progress_callback(bytes_transferred, total_bytes):
        # Get current thread ID
        thread_id = threading.get_ident()

        # Thread-safe update
        with monitor._lock:
            # Initialize thread tracking if needed
            if not hasattr(monitor, '_thread_bytes'):
                monitor._thread_bytes = {}

            # Calculate bytes transferred since last update for this thread
            last_bytes = monitor._thread_bytes.get(thread_id, 0)
            bytes_delta = bytes_transferred - last_bytes

            # Only update if we have actual progress
            if bytes_delta > 0:
                # Store the new value for next comparison
                monitor._thread_bytes[thread_id] = bytes_transferred
                # Update the monitor with the bytes transferred in this update
                monitor.update(bytes_delta)

    return progress_callback
