# These are all of the models that are used by the Synapse client.
from .annotations import set_annotations
from .entity_bundle_services_v2 import (
    get_entity_id_bundle2,
    get_entity_id_version_bundle2,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from .entity_services import (
    get_entity,
    get_upload_destination,
    get_upload_destination_location,
    post_entity,
    put_entity,
    create_access_requirements_if_none,
)
from .file_services import (
    get_file_handle,
    post_external_filehandle,
    post_external_object_store_filehandle,
    post_external_s3_file_handle,
    post_file_multipart,
    put_file_multipart_complete,
    post_file_multipart_presigned_urls,
    put_file_multipart_add,
    AddPartResponse,
)

__all__ = [
    # annotations
    "set_annotations",
    "get_entity_id_bundle2",
    "get_entity_id_version_bundle2",
    "post_entity_bundle2_create",
    "put_entity_id_bundle2",
    # file_services
    "post_file_multipart",
    "put_file_multipart_add",
    "put_file_multipart_complete",
    "post_external_object_store_filehandle",
    "post_external_s3_file_handle",
    "get_file_handle",
    "post_external_filehandle",
    "post_file_multipart_presigned_urls",
    "put_file_multipart_add",
    "AddPartResponse",
    # entity_services
    "get_entity",
    "put_entity",
    "post_entity",
    "get_upload_destination",
    "get_upload_destination_location",
    "create_access_requirements_if_none",
]
