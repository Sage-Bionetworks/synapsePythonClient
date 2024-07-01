# These are all of the models that are used by the Synapse client.
from .annotations import set_annotations, set_annotations_async
from .configuration_services import (
    get_client_authenticated_s3_profile,
    get_config_authentication,
    get_config_file,
    get_config_section_dict,
    get_transfer_config,
)
from .entity_bundle_services_v2 import (
    get_entity_id_bundle2,
    get_entity_id_version_bundle2,
    post_entity_bundle2_create,
    put_entity_id_bundle2,
)
from .entity_factory import get_from_entity_factory
from .entity_services import (
    create_access_requirements_if_none,
    delete_entity_generated_by,
    get_entities_by_md5,
    get_entity,
    get_entity_path,
    get_upload_destination,
    get_upload_destination_location,
    post_entity,
    put_entity,
)
from .file_services import (
    AddPartResponse,
    get_file_handle,
    get_file_handle_for_download,
    get_file_handle_for_download_async,
    post_external_filehandle,
    post_external_object_store_filehandle,
    post_external_s3_file_handle,
    post_file_multipart,
    post_file_multipart_presigned_urls,
    put_file_multipart_add,
    put_file_multipart_complete,
)

__all__ = [
    # annotations
    "set_annotations",
    "set_annotations_async",
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
    "get_file_handle_for_download_async",
    "get_file_handle_for_download",
    # entity_services
    "get_entity",
    "put_entity",
    "post_entity",
    "get_upload_destination",
    "get_upload_destination_location",
    "create_access_requirements_if_none",
    "delete_entity_generated_by",
    "get_entity_path",
    "get_entities_by_md5",
    # configuration_services
    "get_config_file",
    "get_config_section_dict",
    "get_config_authentication",
    "get_client_authenticated_s3_profile",
    "get_transfer_config",
    # entity_factory
    "get_from_entity_factory",
]
