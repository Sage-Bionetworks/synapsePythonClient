import os
import urllib.parse as urllib_parse
import uuid

from synapseclient.core.utils import (
    is_url,
    md5_for_file,
    as_url,
    file_url_to_path,
    id_of,
)
from synapseclient.core import cumulative_transfer_progress
from synapseclient.core.constants import concrete_types
from synapseclient.core.remote_file_storage_wrappers import S3ClientWrapper, SFTPWrapper
from synapseclient.core import sts_transfer
from synapseclient.core.upload.multipart_upload import multipart_upload_file
from synapseclient.core.exceptions import SynapseMd5MismatchError


def log_upload_message(syn, message):
    # if this upload is in the context of a larger, multi threaded sync upload as indicated by a cumulative progress
    # then we don't print the individual upload messages to the console since they wouldn't be properly interleaved.
    if not cumulative_transfer_progress.is_active():
        syn.logger.info(message)


def upload_file_handle(
    syn,
    parent_entity,
    path,
    synapseStore=True,
    md5=None,
    file_size=None,
    mimetype=None,
    max_threads=None,
):
    """Uploads the file in the provided path (if necessary) to a storage location based on project settings.
    Returns a new FileHandle as a dict to represent the stored file.

    :param parent_entity:   Entity object or id of the parent entity.
    :param path:            file path to the file being uploaded
    :param synapseStore:    If False, will not upload the file, but instead create an ExternalFileHandle that references
                            the file on the local machine.
                            If True, will upload the file based on StorageLocation determined by the entity_parent_id
    :param md5:             The MD5 checksum for the file, if known. Otherwise if the file is a local file, it will be
                            calculated automatically.
    :param file_size:       The size the file, if known. Otherwise if the file is a local file, it will be calculated
                            automatically.
    :param file_size:       The MIME type the file, if known. Otherwise if the file is a local file, it will be
                            calculated automatically.

    :returns: a dict of a new FileHandle as a dict that represents the uploaded file
    """
    if path is None:
        raise ValueError("path can not be None")

    # if doing a external file handle with no actual upload
    if not synapseStore:
        return create_external_file_handle(
            syn, path, mimetype=mimetype, md5=md5, file_size=file_size
        )

    # expand the path because past this point an upload is required and some upload functions require an absolute path
    expanded_upload_path = os.path.expandvars(os.path.expanduser(path))

    entity_parent_id = id_of(parent_entity)

    # determine the upload function based on the UploadDestination
    location = syn._getDefaultUploadDestination(entity_parent_id)
    upload_destination_type = location["concreteType"]

    if (
        sts_transfer.is_boto_sts_transfer_enabled(syn)
        and sts_transfer.is_storage_location_sts_enabled(
            syn, entity_parent_id, location
        )
        and upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION
    ):
        log_upload_message(
            syn,
            "\n"
            + "#" * 50
            + "\n Uploading file to external S3 storage using boto3 \n"
            + "#" * 50
            + "\n",
        )

        return upload_synapse_sts_boto_s3(
            syn,
            entity_parent_id,
            location,
            expanded_upload_path,
            mimetype=mimetype,
        )

    elif upload_destination_type in (
        concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION,
        concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION,
        concrete_types.EXTERNAL_GCP_UPLOAD_DESTINATION,
    ):
        if upload_destination_type == concrete_types.SYNAPSE_S3_UPLOAD_DESTINATION:
            storage_str = "Synapse"
        elif upload_destination_type == concrete_types.EXTERNAL_S3_UPLOAD_DESTINATION:
            storage_str = "your external S3"
        else:
            storage_str = "your external Google Bucket"

        log_upload_message(
            syn,
            "\n"
            + "#" * 50
            + "\n Uploading file to "
            + storage_str
            + " storage \n"
            + "#" * 50
            + "\n",
        )

        return upload_synapse_s3(
            syn,
            expanded_upload_path,
            location["storageLocationId"],
            mimetype=mimetype,
            max_threads=max_threads,
        )
    # external file handle (sftp)
    elif upload_destination_type == concrete_types.EXTERNAL_UPLOAD_DESTINATION:
        if location["uploadType"] == "SFTP":
            log_upload_message(
                syn,
                "\n%s\n%s\nUploading to: %s\n%s\n"
                % (
                    "#" * 50,
                    location.get("banner", ""),
                    urllib_parse.urlparse(location["url"]).netloc,
                    "#" * 50,
                ),
            )
            return upload_external_file_handle_sftp(
                syn, expanded_upload_path, location["url"], mimetype=mimetype
            )
        else:
            raise NotImplementedError("Can only handle SFTP upload locations.")
    # client authenticated S3
    elif (
        upload_destination_type
        == concrete_types.EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION
    ):
        log_upload_message(
            syn,
            "\n%s\n%s\nUploading to endpoint: [%s] bucket: [%s]\n%s\n"
            % (
                "#" * 50,
                location.get("banner", ""),
                location.get("endpointUrl"),
                location.get("bucket"),
                "#" * 50,
            ),
        )
        return upload_client_auth_s3(
            syn,
            expanded_upload_path,
            location["bucket"],
            location["endpointUrl"],
            location["keyPrefixUUID"],
            location["storageLocationId"],
            mimetype=mimetype,
        )
    else:  # unknown storage location
        log_upload_message(
            syn,
            "\n%s\n%s\nUNKNOWN STORAGE LOCATION. Defaulting upload to Synapse.\n%s\n"
            % ("!" * 50, location.get("banner", ""), "!" * 50),
        )
        return upload_synapse_s3(
            syn, expanded_upload_path, None, mimetype=mimetype, max_threads=max_threads
        )


def create_external_file_handle(syn, path, mimetype=None, md5=None, file_size=None):
    is_local_file = False  # defaults to false
    url = as_url(os.path.expandvars(os.path.expanduser(path)))
    if is_url(url):
        parsed_url = urllib_parse.urlparse(url)
        if parsed_url.scheme == "file" and os.path.isfile(parsed_url.path):
            actual_md5 = md5_for_file(parsed_url.path).hexdigest()
            if md5 is not None and md5 != actual_md5:
                raise SynapseMd5MismatchError(
                    "The specified md5 [%s] does not match the calculated md5 [%s] for local file [%s]",
                    md5,
                    actual_md5,
                    parsed_url.path,
                )
            md5 = actual_md5
            file_size = os.stat(parsed_url.path).st_size
            is_local_file = True
    else:
        raise ValueError("externalUrl [%s] is not a valid url", url)

    # just creates the file handle because there is nothing to upload
    file_handle = syn._createExternalFileHandle(
        url, mimetype=mimetype, md5=md5, fileSize=file_size
    )
    if is_local_file:
        syn.cache.add(file_handle["id"], file_url_to_path(url))
    return file_handle


def upload_external_file_handle_sftp(syn, file_path, sftp_url, mimetype=None):
    username, password = syn._getUserCredentials(sftp_url)
    uploaded_url = SFTPWrapper.upload_file(
        file_path, urllib_parse.unquote(sftp_url), username, password
    )

    file_handle = syn._createExternalFileHandle(
        uploaded_url,
        mimetype=mimetype,
        md5=md5_for_file(file_path).hexdigest(),
        fileSize=os.stat(file_path).st_size,
    )
    syn.cache.add(file_handle["id"], file_path)
    return file_handle


def upload_synapse_s3(
    syn, file_path, storageLocationId=None, mimetype=None, max_threads=None
):
    file_handle_id = multipart_upload_file(
        syn,
        file_path,
        content_type=mimetype,
        storage_location_id=storageLocationId,
        max_threads=max_threads,
    )
    syn.cache.add(file_handle_id, file_path)

    return syn._get_file_handle_as_creator(file_handle_id)


def upload_synapse_sts_boto_s3(
    syn, parent_id, upload_destination, local_path, mimetype=None
):
    # when uploading to synapse storage normally the back end will generate a random prefix
    # for our uploaded object. since in this case the client is responsible for the remote
    # key, the client will instead generate a random prefix. this both ensures we don't have a collision
    # with an existing S3 object and also mitigates potential performance issues, although
    # key locality performance issues are likely resolved as of:
    # https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/
    key_prefix = str(uuid.uuid4())

    bucket_name = upload_destination["bucket"]
    storage_location_id = upload_destination["storageLocationId"]
    remote_file_key = "/".join(
        [upload_destination["baseKey"], key_prefix, os.path.basename(local_path)]
    )

    def upload_fn(credentials):
        return S3ClientWrapper.upload_file(
            bucket_name,
            None,
            remote_file_key,
            local_path,
            credentials=credentials,
            transfer_config_kwargs={"max_concurrency": syn.max_threads},
        )

    sts_transfer.with_boto_sts_credentials(upload_fn, syn, parent_id, "read_write")
    return syn.create_external_s3_file_handle(
        bucket_name,
        remote_file_key,
        local_path,
        storage_location_id=storage_location_id,
        mimetype=mimetype,
    )


def upload_client_auth_s3(
    syn, file_path, bucket, endpoint_url, key_prefix, storage_location_id, mimetype=None
):
    profile = syn._get_client_authenticated_s3_profile(endpoint_url, bucket)
    file_key = key_prefix + "/" + os.path.basename(file_path)

    S3ClientWrapper.upload_file(
        bucket, endpoint_url, file_key, file_path, profile_name=profile
    )

    file_handle = syn._createExternalObjectStoreFileHandle(
        file_key, file_path, storage_location_id, mimetype=mimetype
    )
    syn.cache.add(file_handle["id"], file_path)

    return file_handle
