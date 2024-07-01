"""This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/#org.sagebionetworks.repo.web.controller.EntityController>
"""

import json
import mimetypes
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from synapseclient.api.entity_services import get_upload_destination
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    SynapseAuthorizationError,
    SynapseFileNotFoundError,
)

if TYPE_CHECKING:
    from synapseclient import Synapse


async def post_file_multipart(
    upload_request_payload: Dict[str, Any],
    force_restart: bool,
    endpoint: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, str]:
    """
    <https://rest-docs.synapse.org/rest/POST/file/multipart.html>

    Arguments:
        upload_request_payload: The request matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartRequest.html>
        force_restart: Optional parameter. When True, any upload state for the given
            file will be cleared and a new upload will be started.
        endpoint: Server endpoint to call to.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The requested multipart upload status matching
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_post_async(
        f"/file/multipart?forceRestart={str(force_restart).lower()}",
        json.dumps(upload_request_payload),
        endpoint=endpoint,
    )


@dataclass
class AddPartResponse:
    """Result of a part add.

    Attributes:
        upload_id: The unique identifier of a multi-part request.
        part_number: The part number of the add.
        add_part_state: The state of this add.
        error_message: If the added failed, this will contain the error message of the
            cause. Will be None when the add is successful.
    """

    upload_id: str
    part_number: int
    add_part_state: str
    error_message: str


async def put_file_multipart_add(
    upload_id: str,
    part_number: int,
    md5_hex: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AddPartResponse:
    """
    <https://rest-docs.synapse.org/rest/PUT/file/multipart/uploadId/add/partNumber.html>

    Arguments:
        upload_id: The unique identifier of the file upload.
        part_number: The part number to add. Must be a number between 1 and 10,000.
        md5_hex: The MD5 of the uploaded part represented as a hexadecimal string. If
            the provided MD5 does not match the MD5 of the uploaded part, the add
            will fail.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        Object matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/AddPartResponse.html>
    """
    try:
        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)
        response = await client.rest_put_async(
            f"/file/multipart/{upload_id}/add/{part_number}?partMD5Hex={md5_hex}",
            endpoint=client.fileHandleEndpoint,
        )
        return AddPartResponse(
            upload_id=response.get("uploadId", None),
            part_number=response.get("partNumber", None),
            add_part_state=response.get("addPartState", None),
            error_message=response.get("errorMessage", None),
        )
    except Exception:
        client.logger.exception(
            f"Error adding part {part_number} to upload {upload_id} with MD5: {md5_hex}"
        )


async def put_file_multipart_complete(
    upload_id: str,
    endpoint: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, str]:
    """
    <https://rest-docs.synapse.org/rest/PUT/file/multipart/uploadId/complete.html>

    Arguments:
        upload_id: The unique identifier of the file upload.
        endpoint: Server endpoint to call to.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        Object matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_put_async(
        f"/file/multipart/{upload_id}/complete",
        endpoint=endpoint,
    )


async def post_file_multipart_presigned_urls(
    upload_id: str,
    part_numbers: List[int],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    <https://rest-docs.synapse.org/rest/PUT/file/multipart/uploadId/add/partNumber.html>

    Arguments:
        upload_id: The unique identifier of the file upload.
        part_numbers: The part numbers to get pre-signed URLs for.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        Object matching
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/AddPartResponse.html>
    """
    from synapseclient import Synapse

    uri = f"/file/multipart/{upload_id}/presigned/url/batch"
    body = {
        "uploadId": upload_id,
        "partNumbers": part_numbers,
    }

    client = Synapse.get_client(synapse_client=synapse_client)
    client.logger.debug(
        f"Fetching presigned URLs for {part_numbers} with upload_id {upload_id}"
    )
    return await client.rest_post_async(
        uri,
        json.dumps(body),
        endpoint=client.fileHandleEndpoint,
    )


async def post_external_object_store_filehandle(
    s3_file_key: str,
    file_path: str,
    storage_location_id: int,
    mimetype: str = None,
    md5: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Create a new FileHandle representing an external object.
    <https://rest-docs.synapse.org/rest/POST/externalFileHandle.html>

    Arguments:
        s3_file_key:         S3 key of the uploaded object
        file_path:           The local path of the uploaded file
        storage_location_id: The optional storage location descriptor
        mimetype:            The Mimetype of the file, if known.
        md5:                 The file's content MD5, if known.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        A FileHandle for objects that are stored externally.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/ExternalFileHandleInterface.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    if mimetype is None:
        mimetype, _ = mimetypes.guess_type(file_path, strict=False)
    file_handle = {
        "concreteType": concrete_types.EXTERNAL_OBJECT_STORE_FILE_HANDLE,
        "fileKey": s3_file_key,
        "fileName": os.path.basename(file_path),
        "contentMd5": md5 or utils.md5_for_file(file_path).hexdigest(),
        "contentSize": os.stat(file_path).st_size,
        "storageLocationId": storage_location_id,
        "contentType": mimetype,
    }

    return await client.rest_post_async(
        "/externalFileHandle", json.dumps(file_handle), client.fileHandleEndpoint
    )


async def post_external_filehandle(
    external_url: str,
    mimetype: str = None,
    md5: str = None,
    file_size: int = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Create a new FileHandle representing an external object.
    <https://rest-docs.synapse.org/rest/POST/externalFileHandle.html>

    Arguments:
        externalURL:  An external URL
        mimetype:     The Mimetype of the file, if known.
        md5:          The file's content MD5.
        file_size:    The size of the file in bytes.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        A FileHandle for objects that are stored externally.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/ExternalFileHandleInterface.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    file_name = external_url.split("/")[-1]
    external_url = utils.as_url(external_url)
    file_handle = {
        "concreteType": concrete_types.EXTERNAL_FILE_HANDLE,
        "fileName": file_name,
        "externalURL": external_url,
        "contentMd5": md5,
        "contentSize": file_size,
    }
    if mimetype is None:
        (mimetype, _) = mimetypes.guess_type(external_url, strict=False)
    if mimetype is not None:
        file_handle["contentType"] = mimetype
    return await client.rest_post_async(
        "/externalFileHandle", json.dumps(file_handle), client.fileHandleEndpoint
    )


async def post_external_s3_file_handle(
    bucket_name: str,
    s3_file_key: str,
    file_path: str,
    parent: str = None,
    storage_location_id: str = None,
    mimetype: str = None,
    md5: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int, bool]]:
    """
    Create an external S3 file handle for e.g. a file that has been uploaded directly to
    an external S3 storage location.

    <https://rest-docs.synapse.org/rest/POST/externalFileHandle/s3.html>

    Arguments:
        bucket_name: Name of the S3 bucket
        s3_file_key: S3 key of the uploaded object
        file_path: Local path of the uploaded file
        parent: Parent entity to create the file handle in, the file handle will be
            created in the default storage location of the parent. Mutually exclusive
            with storage_location_id
        storage_location_id: Explicit storage location id to create the file handle in,
            mutually exclusive with parent
        mimetype: Mimetype of the file, if known
        md5: MD5 of the file, if known
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        The created file handle.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/S3FileHandle.html>

    Raises:
        ValueError: If neither parent nor storage_location_id is specified, or if
            both are specified.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if storage_location_id:
        if parent:
            raise ValueError("Pass parent or storage_location_id, not both")
    elif not parent:
        raise ValueError("One of parent or storage_location_id is required")
    else:
        upload_destination = await get_upload_destination(
            entity_id=parent, synapse_client=client
        )
        storage_location_id = upload_destination["storageLocationId"]

    if mimetype is None:
        mimetype, _ = mimetypes.guess_type(file_path, strict=False)

    file_handle = {
        "concreteType": concrete_types.S3_FILE_HANDLE,
        "key": s3_file_key,
        "bucketName": bucket_name,
        "fileName": os.path.basename(file_path),
        "contentMd5": md5 or utils.md5_for_file(file_path).hexdigest(),
        "contentSize": os.stat(file_path).st_size,
        "storageLocationId": storage_location_id,
        "contentType": mimetype,
    }

    return await client.rest_post_async(
        "/externalFileHandle/s3",
        json.dumps(file_handle),
        endpoint=client.fileHandleEndpoint,
    )


async def get_file_handle(
    file_handle_id: Dict[str, Union[str, int]],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Union[str, int]]:
    """
    Retrieve a fileHandle from the fileHandle service.
    Note: You must be the creator of the filehandle to use this method.
    Otherwise, an 403-Forbidden error will be raised.

    <https://rest-docs.synapse.org/rest/GET/fileHandle/handleId.html>

    Arguments:
        file_handle_id: The ID of the file handle to look up.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns:
        A file handle retrieved from the file handle service.
        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandle.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(
        f"/fileHandle/{file_handle_id}", endpoint=client.fileHandleEndpoint
    )


async def get_file_handle_for_download_async(
    file_handle_id: str,
    synapse_id: str,
    entity_type: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, str]:
    """
    Gets the URL and the metadata as filehandle object for a filehandle or fileHandleId

    Arguments:
        file_handle_id:   ID of fileHandle to download
        synapse_id:       The ID of the object associated with the file e.g. syn234
        entity_type:     Type of object associated with a file e.g. FileEntity,
            TableEntity
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html>
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Raises:
        SynapseFileNotFoundError: If the fileHandleId is not found in Synapse.
        SynapseError: If the user does not have the permission to access the
            fileHandleId.

    Returns:
        A dictionary with keys: fileHandle, fileHandleId and preSignedURL
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    body = {
        "includeFileHandles": True,
        "includePreSignedURLs": True,
        "requestedFiles": [
            {
                "fileHandleId": file_handle_id,
                "associateObjectId": synapse_id,
                "associateObjectType": entity_type or "FileEntity",
            }
        ],
    }
    response = await client.rest_post_async(
        "/fileHandle/batch", body=json.dumps(body), endpoint=client.fileHandleEndpoint
    )

    result = response["requestedFiles"][0]
    failure = result.get("failureCode")
    if failure == "NOT_FOUND":
        raise SynapseFileNotFoundError(
            f"The fileHandleId {file_handle_id} could not be found"
        )
    elif failure == "UNAUTHORIZED":
        raise SynapseAuthorizationError(
            f"You are not authorized to access fileHandleId {file_handle_id} "
            f"associated with the Synapse {entity_type}: {synapse_id}"
        )
    return result


def get_file_handle_for_download(
    file_handle_id: str,
    synapse_id: str,
    entity_type: str = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, str]:
    """
    Gets the URL and the metadata as filehandle object for a filehandle or fileHandleId

    Arguments:
        file_handle_id:   ID of fileHandle to download
        synapse_id:       The ID of the object associated with the file e.g. syn234
        entity_type:     Type of object associated with a file e.g. FileEntity,
            TableEntity
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/FileHandleAssociateType.html>
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Raises:
        SynapseFileNotFoundError: If the fileHandleId is not found in Synapse.
        SynapseError: If the user does not have the permission to access the
            fileHandleId.

    Returns:
        A dictionary with keys: fileHandle, fileHandleId and preSignedURL
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    body = {
        "includeFileHandles": True,
        "includePreSignedURLs": True,
        "requestedFiles": [
            {
                "fileHandleId": file_handle_id,
                "associateObjectId": synapse_id,
                "associateObjectType": entity_type or "FileEntity",
            }
        ],
    }

    response = client.restPOST(
        "/fileHandle/batch", body=json.dumps(body), endpoint=client.fileHandleEndpoint
    )

    result = response["requestedFiles"][0]
    failure = result.get("failureCode")
    if failure == "NOT_FOUND":
        raise SynapseFileNotFoundError(
            f"The fileHandleId {file_handle_id} could not be found"
        )
    elif failure == "UNAUTHORIZED":
        raise SynapseAuthorizationError(
            f"You are not authorized to access fileHandleId {file_handle_id} "
            f"associated with the Synapse {entity_type}: {synapse_id}"
        )
    return result
