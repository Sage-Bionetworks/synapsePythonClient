"""This module handles the various ways that a user can upload a file to Synapse."""

import asyncio
import os
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

    # if doing a external file handle with no actual upload
    if not synapse_store:
        return await create_external_file_handle(
            syn, path, mimetype=mimetype, md5=md5, file_size=file_size
        )

    # expand the path because past this point an upload is required and some upload functions require an absolute path
    expanded_upload_path = os.path.expandvars(os.path.expanduser(path))

    if md5 is None and os.path.isfile(expanded_upload_path):
        md5 = utils.md5_for_file_hex(filename=expanded_upload_path)

    entity_parent_id = id_of(parent_entity_id)

    # determine the upload function based on the UploadDestination
    location = await get_upload_destination(
        entity_id=entity_parent_id, synapse_client=syn
    )
    upload_destination_type = location.get("concreteType", None) if location else None
    trace.get_current_span().set_attributes(
        {
            "synapse.parent_id": entity_parent_id,
            "synapse.upload_destination_type": upload_destination_type,
        }
    )

    if (
        sts_transfer.is_boto_sts_transfer_enabled(syn)
        and await sts_transfer.is_storage_location_sts_enabled_async(
            syn, entity_parent_id, location
        )
        and upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION
    ):
        return await upload_synapse_sts_boto_s3(
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

        return await upload_synapse_s3(
            syn=syn,
            file_path=expanded_upload_path,
            storage_location_id=location["storageLocationId"],
            mimetype=mimetype,
            md5=md5,
            storage_str=storage_str,
        )
    # external file handle (sftp)
    elif upload_destination_type == concrete_types.EXTERNAL_UPLOAD_DESTINATION:
        if location["uploadType"] == "SFTP":
            storage_str = (
                f"Uploading to: {urllib_parse.urlparse(location['url']).netloc}"
            )
            banner = location.get("banner", None)
            if banner:
                syn.logger.info(banner)
            return await upload_external_file_handle_sftp(
                syn=syn,
                file_path=expanded_upload_path,
                sftp_url=location["url"],
                mimetype=mimetype,
                md5=md5,
                storage_str=storage_str,
            )
        else:
            raise NotImplementedError("Can only handle SFTP upload locations.")
    # client authenticated S3
    elif (
        upload_destination_type
        == concrete_types.EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION
    ):
        storage_str = f"Uploading to endpoint: [{location.get('endpointUrl')}] bucket: [{location.get('bucket')}]"
        banner = location.get("banner", None)
        if banner:
            syn.logger.info(banner)
        return await upload_client_auth_s3(
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
        return await upload_synapse_s3(
            syn=syn,
            file_path=expanded_upload_path,
            storage_location_id=None,
            mimetype=mimetype,
            md5=md5,
            storage_str="Uploading to Synapse storage",
        )


async def create_external_file_handle(
    syn: "Synapse",
    path: str,
    mimetype: str = None,
    md5: str = None,
    file_size: int = None,
) -> Dict[str, Union[str, int]]:
    """Create a file handle in Synapse without uploading any files. This is used in
    cases where one wishes to store a reference to a file that is not in Synapse."""
    is_local_file = False  # defaults to false
    url = as_url(os.path.expandvars(os.path.expanduser(path)))
    if is_url(url):
        parsed_url = urllib_parse.urlparse(url)
        parsed_path = file_url_to_path(url)
        if parsed_url.scheme == "file" and os.path.isfile(parsed_path):
            actual_md5 = utils.md5_for_file_hex(filename=parsed_path)
            if md5 is not None and md5 != actual_md5:
                raise SynapseMd5MismatchError(
                    f"The specified md5 [{md5}] does not match the calculated md5 "
                    f"[{actual_md5}] for local file [{parsed_path}]",
                )
            md5 = actual_md5
            file_size = os.stat(parsed_path).st_size
            is_local_file = True
    else:
        raise ValueError(f"externalUrl [{url}] is not a valid url")

    # just creates the file handle because there is nothing to upload
    file_handle = await post_external_filehandle(
        external_url=url, mimetype=mimetype, md5=md5, file_size=file_size
    )
    if is_local_file:
        syn.cache.add(
            file_handle_id=file_handle["id"], path=file_url_to_path(url), md5=md5
        )
    trace.get_current_span().set_attributes(
        {"synapse.file_handle_id": file_handle["id"]}
    )
    return file_handle


async def upload_external_file_handle_sftp(
    syn: "Synapse",
    file_path: str,
    sftp_url: str,
    mimetype: str = None,
    md5: str = None,
    storage_str: str = None,
) -> Dict[str, Union[str, int]]:
    """Upload a file to an SFTP server and create a file handle in Synapse."""
    username, password = syn._getUserCredentials(url=sftp_url)
    uploaded_url = SFTPWrapper.upload_file(
        file_path,
        urllib_parse.unquote(sftp_url),
        username,
        password,
        storage_str=storage_str,
    )

    file_md5 = md5 or utils.md5_for_file_hex(filename=file_path)
    file_handle = await post_external_filehandle(
        external_url=uploaded_url,
        mimetype=mimetype,
        md5=file_md5,
        file_size=os.stat(file_path).st_size,
    )
    syn.cache.add(file_handle_id=file_handle["id"], path=file_path, md5=file_md5)
    return file_handle


async def upload_synapse_s3(
    syn: "Synapse",
    file_path: str,
    storage_location_id: Optional[int] = None,
    mimetype: str = None,
    force_restart: bool = False,
    md5: str = None,
    storage_str: str = None,
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
    file_handle_id = await multipart_upload_file_async(
        syn=syn,
        file_path=file_path,
        content_type=mimetype,
        storage_location_id=storage_location_id,
        md5=md5,
        force_restart=force_restart,
        storage_str=storage_str,
    )
    syn.cache.add(file_handle_id=file_handle_id, path=file_path, md5=md5)

    return await get_file_handle(file_handle_id=file_handle_id, synapse_client=syn)


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
    key_prefix = str(uuid.uuid4())

    bucket_name = upload_destination["bucket"]
    storage_location_id = upload_destination["storageLocationId"]
    remote_file_key = "/".join(
        [
            upload_destination.get("baseKey", ""),
            key_prefix,
            os.path.basename(local_path),
        ]
    )

    def upload_fn(credentials: Dict[str, str]) -> str:
        """Wrapper for the upload function."""
        return S3ClientWrapper.upload_file(
            bucket=bucket_name,
            endpoint_url=None,
            remote_file_key=remote_file_key,
            upload_file_path=local_path,
            credentials=credentials,
            transfer_config_kwargs={"max_concurrency": syn.max_threads},
            storage_str=storage_str,
        )

    loop = asyncio.get_event_loop()

    await loop.run_in_executor(
        syn._get_thread_pool_executor(asyncio_event_loop=loop),
        lambda: sts_transfer.with_boto_sts_credentials(
            upload_fn, syn, parent_id, "read_write"
        ),
    )

    return await post_external_s3_file_handle(
        bucket_name=bucket_name,
        s3_file_key=remote_file_key,
        file_path=local_path,
        storage_location_id=storage_location_id,
        mimetype=mimetype,
        md5=md5,
        synapse_client=syn,
    )


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
    profile = get_client_authenticated_s3_profile(
        endpoint=endpoint_url, bucket=bucket, config_path=syn.configPath
    )
    file_key = key_prefix + "/" + os.path.basename(file_path)
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
        ),
    )

    file_handle = await post_external_object_store_filehandle(
        s3_file_key=file_key,
        file_path=file_path,
        storage_location_id=storage_location_id,
        mimetype=mimetype,
        md5=md5,
        synapse_client=syn,
    )
    syn.cache.add(file_handle_id=file_handle["id"], path=file_path, md5=md5)

    return file_handle
