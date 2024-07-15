"""Script to work with Synapse files."""

import asyncio
import dataclasses
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from opentelemetry import context

from synapseclient import File as SynapseFile
from synapseclient import Synapse
from synapseclient.api import get_from_entity_factory
from synapseclient.core import utils
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseFileNotFoundError,
    SynapseMalformedEntityError,
)
from synapseclient.core.upload.upload_functions_async import upload_file_handle
from synapseclient.core.utils import (
    delete_none_keys,
    guess_file_name,
    merge_dataclass_entities,
    run_and_attach_otel_context,
)
from synapseclient.entity import File as Synapse_File
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.protocols.file_protocol import FileSynchronousProtocol
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity import store_entity
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)

if TYPE_CHECKING:
    from synapseclient.models import Folder, Project


@dataclass()
class FileHandle:
    """A file handle is a pointer to a file stored in a specific location.

    Attributes:
        id: The ID of this FileHandle. All references to this FileHandle will use this
            ID. Synapse will generate this ID when the FileHandle is created.
        etag: FileHandles are immutable from the perspective of the API. The only field
            that can be change is the previewId. When a new previewId is set, the
            etag will change.
        created_by: The ID Of the user that created this file.
        created_on: The date when this file was uploaded.
        modified_on: The date when the file was modified. This is handled by the backend
            and cannot be modified.
        concrete_type: This is used to indicate the implementation of this interface.
            For example, an S3FileHandle should be set to:
            org.sagebionetworks.repo.model.file.S3FileHandle
        content_type: Must be: <http://en.wikipedia.org/wiki/Internet_media_type>.
        content_md5: The file's content MD5.
        file_name: The short, user visible name for this file.
        storage_location_id: The optional storage location descriptor.
        content_size: The size of the file in bytes.
        status: The status of the file handle as computed by the backend. This value
            cannot be changed, any file handle that is not AVAILABLE should not be used.
        bucket_name: The name of the bucket where this file resides.
        key: The path or resource name for this object.
        preview_id: If this file has a preview, then this will be the file ID of the
            preview.
        is_preview: Whether or not this is a preview of another file.
        external_url: The URL of the file if it is stored externally.
    """

    id: Optional[str] = None
    """The ID of this FileHandle. All references to this FileHandle will use this ID.
        Synapse will generate this ID when the FileHandle is created."""

    etag: Optional[str] = None
    """
    FileHandles are immutable from the perspective of the API. The only field that can
    be change is the previewId. When a new previewId is set, the etag will change.
    """

    created_by: Optional[str] = None
    """The ID Of the user that created this file."""

    created_on: Optional[str] = None
    """The date when this file was uploaded."""

    modified_on: Optional[str] = None
    """The date when the file was modified. This is handled by the backend and cannot
    be modified."""

    concrete_type: Optional[str] = None
    """
    This is used to indicate the implementation of this interface. For example,
    an S3FileHandle should be set to: org.sagebionetworks.repo.model.file.S3FileHandle
    """

    content_type: Optional[str] = None
    """Must be: <http://en.wikipedia.org/wiki/Internet_media_type>."""

    content_md5: Optional[str] = None
    """The file's content MD5."""

    file_name: Optional[str] = None
    """The short, user visible name for this file."""

    storage_location_id: Optional[int] = None
    """The optional storage location descriptor."""

    content_size: Optional[int] = None
    """The size of the file in bytes."""

    status: Optional[str] = None
    """The status of the file handle as computed by the backend. This value cannot be
    changed, any file handle that is not AVAILABLE should not be used."""

    bucket_name: Optional[str] = None
    """The name of the bucket where this file resides."""

    key: Optional[str] = None
    """The path or resource name for this object."""

    preview_id: Optional[str] = None
    """If this file has a preview, then this will be the file ID of the preview."""

    is_preview: Optional[bool] = None
    """Whether or not this is a preview of another file."""

    external_url: Optional[str] = None
    """The URL of the file if it is stored externally."""

    def fill_from_dict(
        self, synapse_instance: Dict[str, Union[bool, str, int]]
    ) -> "FileHandle":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_instance: The response from the REST API.
            set_annotations: Whether to set the annotations from the response.

        Returns:
            The File object.
        """
        file_handle = self or FileHandle()
        file_handle.id = synapse_instance.get("id", None)
        file_handle.etag = synapse_instance.get("etag", None)
        file_handle.created_by = synapse_instance.get("createdBy", None)
        file_handle.created_on = synapse_instance.get("createdOn", None)
        file_handle.modified_on = synapse_instance.get("modifiedOn", None)
        file_handle.concrete_type = synapse_instance.get("concreteType", None)
        file_handle.content_type = synapse_instance.get("contentType", None)
        file_handle.content_md5 = synapse_instance.get("contentMd5", None)
        file_handle.file_name = synapse_instance.get("fileName", None)
        file_handle.storage_location_id = synapse_instance.get(
            "storageLocationId", None
        )
        file_handle.content_size = synapse_instance.get("contentSize", None)
        file_handle.status = synapse_instance.get("status", None)
        file_handle.bucket_name = synapse_instance.get("bucketName", None)
        file_handle.key = synapse_instance.get("key", None)
        file_handle.preview_id = synapse_instance.get("previewId", None)
        file_handle.is_preview = synapse_instance.get("isPreview", None)
        file_handle.external_url = synapse_instance.get("externalURL", None)

        return self

    def _convert_into_legacy_file_handle(self) -> Dict[str, Union[str, bool, int]]:
        """Convert the file handle object into a legacy File Handle object."""
        return_data = {
            "id": self.id,
            "etag": self.etag,
            "createdBy": self.created_by,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "concreteType": self.concrete_type,
            "contentType": self.content_type,
            "contentMd5": self.content_md5,
            "fileName": self.file_name,
            "storageLocationId": self.storage_location_id,
            "contentSize": self.content_size,
            "status": self.status,
            "bucketName": self.bucket_name,
            "key": self.key,
            "previewId": self.preview_id,
            "isPreview": self.is_preview,
            "externalURL": self.external_url,
        }
        delete_none_keys(return_data)
        return return_data


@dataclass()
@async_to_sync
class File(FileSynchronousProtocol, AccessControllable):
    """A file within Synapse.

    Attributes:
        id: The unique immutable ID for this file. A new ID will be generated for new
            Files. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses. If not specified, the name will be
            derived from the file name.
        path: The path to the file on disk. Using shorthand `~` will be expanded to the
            user's home directory.

            This is used during a `get` operation to specify where to download the file
            to. It should be pointing to a directory.

            This is also used during a `store` operation to specify the file to upload.
            It should be pointing to a file.

        description: The description of this file. Must be 1000 characters or less.
        parent_id: The ID of the Entity that is the parent of this Entity. Setting this
            to a new value and storing it will move this File under the new parent.
        version_label: The version label for this entity. Updates to the entity will
            increment the version number.
        version_comment: The version comment for this entity
        data_file_handle_id: ID of the file associated with this entity. You may define
            an existing data_file_handle_id to use the existing data_file_handle_id. The
            creator of the file must also be the owner of the data_file_handle_id to
            have permission to store the file.
        external_url: The external URL of this file. If this is set AND `synapse_store`
            is False, only a reference to this URL and the file metadata will be stored
            in Synapse. The file itself will not be uploaded. If this attribute is set
            it will override the `path`.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance. Activity
            cannot be removed during a store operation by setting it to None. You must
            use: [synapseclient.models.Activity.delete_async][] or
            [synapseclient.models.Activity.disassociate_from_entity_async][].
        annotations: Additional metadata associated with the folder. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list. To remove all annotations set this
            to an empty dict `{}` or None and store the entity.

    Attributes:
        content_type: (New Upload Only)
            Used to manually specify Content-type header, for example
            'application/png' or 'application/json; charset=UTF-8'. If not specified,
            the content type will be derived from the file extension.


            This can be specified only during the initial store of this file or any time
            there is a new file to upload.
            In order to change this after the File has been created use
            [synapseclient.models.File.change_metadata][].
        content_size: (New Upload Only)
            The size of the file in bytes. This can be specified only during the initial
            creation of the File. This is also only applicable to files not uploaded to
            Synapse. ie: `synapse_store` is False.

    Attributes:
        content_md5: (Store only) The MD5 of the file is known. If not supplied this
            will be computed in the client is possible. If supplied for a file entity
            already stored in Synapse it will be calculated again to check if a new
            upload needs to occur. This will not be filled in during a read for data. It
            is only used during a store operation. To retrieve the md5 of the file after
            read from synapse use the `.file_handle.content_md5` attribute.
        create_or_update: (Store only)
            Indicates whether the method should automatically perform an
            update if the `file` conflicts with an existing Synapse object.
        force_version: (Store only)
            Indicates whether the method should increment the version of the object if
            something within the entity has changed. For example updating the
            description or name. You may set this to False and an update to the
            entity will not increment the version.

            Updating the `version_label` attribute will also cause a version update
            regardless  of this flag.

            An update to the MD5 of the file will force a version update regardless of
            this  flag.
        is_restricted: (Store only)
            If set to true, an email will be sent to the Synapse access control
            team to start the process of adding terms-of-use or review board approval
            for this entity. You will be contacted with regards to the specific data
            being restricted and the requirements of access.

            This may be used only by an administrator of the specified file.
        merge_existing_annotations: (Store only)
            Works in conjunction with `create_or_update` in that this is only evaluated
            if `create_or_update` is True. If this entity exists in Synapse that has
            annotations that are not present in a store operation, these annotations
            will be added to the entity. If this is False any annotations that are not
            present within a store operation will be removed from this entity. This
            allows one to complete a destructive update of annotations on an entity.
        associate_activity_to_new_version: (Store only)
            Works in conjunction with `create_or_update` in that this is only evaluated
            if `create_or_update` is True. When true an activity already attached to the
            current version of this entity will be associated the new version during a
            store operation if the version was updated. This is useful if you are
            updating the entity and want to ensure that the activity is persisted onto
            the new version the entity.
        synapse_store: (Store only)
            Whether the File should be uploaded or if false: only the path should
            be stored when [synapseclient.models.File.store][] is called.

    Attributes:
        download_file: (Get only) If True the file will be downloaded.
        if_collision: (Get only)
            Determines how to handle file collisions. Defaults to "keep.both". May be:

            - `overwrite.local`
            - `keep.local`
            - `keep.both`
        synapse_container_limit: (Get only)
            A Synanpse ID used to limit the search in Synapse if
            file is specified as a local file. That is, if the file is stored in
            multiple locations in Synapse only the ones in the specified folder/project
            will be returned.

    Attributes:
        etag: (Read Only) Synapse employs an Optimistic Concurrency Control (OCC) scheme
            to handle concurrent updates. Since the E-Tag changes every time an entity
            is updated it is used to detect when a client's current representation of an
            entity is out-of-date.
        created_on: (Read Only) The date this entity was created.
        modified_on: (Read Only) The date this entity was last modified.
        created_by: (Read Only) The ID of the user that created this entity.
        modified_by: (Read Only) The ID of the user that last modified this entity.
        version_number: (Read Only) The version number issued to this version on the
            object.
        is_latest_version: (Read Only) If this is the latest version of the object.
        file_handle: (Read Only) The file handle associated with this entity.
    """

    id: Optional[str] = None
    """The unique immutable ID for this file. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued."""

    name: Optional[str] = None
    """
    The name of this entity. Must be 256 characters or less.
    Names may only contain: letters, numbers, spaces, underscores, hyphens, periods,
    plus signs, apostrophes, and parentheses. If not specified, the name will be
    derived from the file name.
    """

    path: Optional[str] = field(default=None, compare=False)
    """The path to the file on disk. Using shorthand `~` will be expanded to the user's
    home directory.

    This is used during a `get` operation to specify where to download the file to. It
    should be pointing to a directory.

    This is also used during a `store` operation to specify the file to upload. It
    should be pointing to a file."""

    description: Optional[str] = None
    """The description of this file. Must be 1000 characters or less."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this Entity. Setting this to a new
    value and storing it will move this File under the new parent."""

    version_label: Optional[str] = None
    """The version label for this entity. Updates to the entity will increment the
    version number."""

    version_comment: Optional[str] = None
    """The version comment for this entity."""

    data_file_handle_id: Optional[str] = None
    """
    ID of the file handle associated with this entity. You may define an existing
    data_file_handle_id to use the existing data_file_handle_id. The creator of the
    file must also be the owner of the data_file_handle_id to have permission to
    store the file.
    """

    external_url: Optional[str] = field(default=None, compare=False)
    """
    The external URL of this file. If this is set AND `synapse_store` is False, only
    a reference to this URL and the file metadata will be stored in Synapse. The file
    itself will not be uploaded. If this attribute is set it will override the `path`.
    """

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analygous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance. Activity cannot
    be removed during a store operation by setting it to None. You must use:
    [synapseclient.models.Activity.delete_async][] or
    [synapseclient.models.Activity.disassociate_from_entity_async][].
    """

    annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = field(default_factory=dict, compare=False)
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`."""

    content_type: Optional[str] = None
    """
    (New Upload Only)
    Used to manually specify Content-type header, for example 'application/png'
    or 'application/json; charset=UTF-8'. If not specified, the content type will be
    derived from the file extension.

    This can be specified only during the initial store of this file. In order to change
    this after the File has been created use
    [synapseclient.models.File.change_metadata][].
    """

    content_size: Optional[int] = None
    """
    (New Upload Only)
    The size of the file in bytes. This can be specified only during the initial
    creation of the File. This is also only applicable to files not uploaded to Synapse.
    ie: `synapse_store` is False.
    """

    content_md5: Optional[str] = field(default=None, compare=False)
    """
    (Store only)
    The MD5 of the file is known. If not supplied this will be computed in the client
    is possible. If supplied for a file entity already stored in Synapse it will be
    calculated again to check if a new upload needs to occur. This will not be filled
    in during a read for data. It is only used during a store operation. To retrieve
    the md5 of the file after read from synapse use the `.file_handle.content_md5`
    attribute.
    """

    create_or_update: bool = field(default=True, repr=False, compare=False)
    """
    (Store only)

    Indicates whether the method should automatically perform an update if the file
    conflicts with an existing Synapse object.
    """

    force_version: bool = field(default=True, repr=False, compare=False)
    """
    (Store only)

    Indicates whether the method should increment the version of the object if something
    within the entity has changed. For example updating the description or name.
    You may set this to False and an update to the entity will not increment the
    version.

    Updating the `version_label` attribute will also cause a version update regardless
    of this flag.

    An update to the MD5 of the file will force a version update regardless of this
    flag.
    """

    is_restricted: bool = field(default=False, repr=False)
    """
    (Store only)

    If set to true, an email will be sent to the Synapse access control team to start
    the process of adding terms-of-use or review board approval for this entity.
    You will be contacted with regards to the specific data being restricted and the
    requirements of access.

    This may be used only by an administrator of the specified file.
    """

    merge_existing_annotations: bool = field(default=True, repr=False, compare=False)
    """
    (Store only)

    Works in conjunction with `create_or_update` in that this is only evaluated if
    `create_or_update` is True. If this entity exists in Synapse that has annotations
    that are not present in a store operation, these annotations will be added to the
    entity. If this is False any annotations that are not present within a store
    operation will be removed from this entity. This allows one to complete a
    destructive update of annotations on an entity.
    """

    associate_activity_to_new_version: bool = field(
        default=False, repr=False, compare=False
    )
    """
    (Store only)

    Works in conjunction with `create_or_update` in that this is only evaluated if
    `create_or_update` is True. When true an activity already attached to the current
    version of this entity will be associated the new version during a store operation
    if the version was updated. This is useful if you are updating the entity and want
    to ensure that the activity is persisted onto the new version the entity.

    When this is False the activity will not be associated to the new version of the
    entity during a store operation.

    Regardless of this setting, if you have an Activity object on the entity it will be
    persisted onto the new version. This is only used when you don't have an Activity
    object on the entity.
    """

    _present_manifest_fields: List[str] = field(default=None, repr=False, compare=False)
    """Hidden attribute to pass along what columns were present in a manifest upload."""

    synapse_store: bool = field(default=True, repr=False)
    """
    (Store only)

    Whether the File should be uploaded or if false: only the path should be stored when
    [synapseclient.models.File.store][] is called.
    """

    download_file: bool = field(default=True, repr=False, compare=False)
    """
    (Get only)

    If True the file will be downloaded."""

    if_collision: str = field(default="keep.both", repr=False, compare=False)
    """
    (Get only)

    Determines how to handle file collisions. Defaults to "keep.both".
            May be

            - `overwrite.local`
            - `keep.local`
            - `keep.both`
    """

    synapse_container_limit: Optional[str] = field(
        default=None, repr=False, compare=False
    )
    """A Synanpse ID used to limit the search in Synapse if file is specified as a local
    file. That is, if the file is stored in multiple locations in Synapse only the
    ones in the specified folder/project will be returned."""

    etag: Optional[str] = field(default=None, compare=False)
    """
    (Read Only)
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = field(default=None, compare=False)
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """(Read Only) The ID of the user that last modified this entity."""

    version_number: Optional[int] = field(default=None, compare=False)
    """(Read Only) The version number issued to this version on the object."""

    is_latest_version: Optional[bool] = field(default=None, compare=False)
    """(Read Only) If this is the latest version of the object."""

    file_handle: Optional[FileHandle] = field(default=None, compare=False)
    """(Read Only) The file handle associated with this entity."""

    _last_persistent_instance: Optional["File"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = dataclasses.replace(self)
        self._last_persistent_instance.activity = (
            dataclasses.replace(self.activity) if self.activity else None
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

    def _fill_from_file_handle(self) -> None:
        """Fill the file object from the file handle."""
        if self.file_handle:
            self.data_file_handle_id = self.file_handle.id
            self.content_type = self.file_handle.content_type
            self.content_size = self.file_handle.content_size
            self.external_url = self.file_handle.external_url

    def fill_from_dict(
        self,
        synapse_file: Union[Synapse_File, Dict[str, Union[bool, str, int]]],
        set_annotations: bool = True,
    ) -> "File":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_file: The response from the REST API.
            set_annotations: Whether to set the annotations from the response.

        Returns:
            The File object.
        """
        self.id = synapse_file.get("id", None)
        self.name = synapse_file.get("name", None)
        self.description = synapse_file.get("description", None)
        self.etag = synapse_file.get("etag", None)
        self.created_on = synapse_file.get("createdOn", None)
        self.modified_on = synapse_file.get("modifiedOn", None)
        self.created_by = synapse_file.get("createdBy", None)
        self.modified_by = synapse_file.get("modifiedBy", None)
        self.parent_id = synapse_file.get("parentId", None)
        self.version_number = synapse_file.get("versionNumber", None)
        self.version_label = synapse_file.get("versionLabel", None)
        self.version_comment = synapse_file.get("versionComment", None)
        self.is_latest_version = synapse_file.get("isLatestVersion", False)
        self.data_file_handle_id = synapse_file.get("dataFileHandleId", None)
        self.path = synapse_file.get("path", self.path)
        synapse_file_handle = synapse_file.get("_file_handle", None)
        if synapse_file_handle:
            file_handle = self.file_handle or FileHandle()
            self.file_handle = file_handle.fill_from_dict(
                synapse_instance=synapse_file_handle
            )
            self._fill_from_file_handle()

        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_file.get("annotations", {})
            )
        return self

    def _cannot_store(self) -> bool:
        """Determines based on some guard conditions if we are unable to continue with
        a store operation."""
        return (
            not (
                self.id is not None
                and (self.path is not None or self.data_file_handle_id is not None)
            )
            and not (self.path is not None and self.parent_id is not None)
            and not (
                self.parent_id is not None and self.data_file_handle_id is not None
            )
        )

    async def _load_local_md5(self) -> None:
        """Load the MD5 of the file if it's a local file and we have not already loaded
        it."""
        if not self.content_md5 and self.path and os.path.isfile(self.path):
            self.content_md5 = utils.md5_for_file_hex(filename=self.path)

    async def _find_existing_file(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> Union["File", None]:
        """Determines if the file already exists in Synapse. If it does it will return
        the file object, otherwise it will return None. This is used to determine if the
        file should be updated or created."""

        async def get_file(existing_id: str) -> "File":
            """Small wrapper to retrieve a file instance without raising an error if it
            does not exist.

            Arguments:
                existing_id: The ID of the file to retrieve.

            Returns:
                The file object if it exists, otherwise None.
            """
            try:
                file_copy = File(
                    id=existing_id,
                    download_file=False,
                    version_number=self.version_number,
                    synapse_container_limit=self.synapse_container_limit,
                    parent_id=self.parent_id,
                )
                return await file_copy.get_async(
                    synapse_client=synapse_client,
                    include_activity=self.activity is not None
                    or self.associate_activity_to_new_version,
                )
            except SynapseFileNotFoundError:
                return None

        if (
            self.create_or_update
            and not self._last_persistent_instance
            and (
                existing_file_id := await get_id(
                    entity=self,
                    failure_strategy=None,
                    synapse_client=synapse_client,
                )
            )
            and (existing_file := await get_file(existing_file_id))
        ):
            return existing_file
        return None

    def _determine_fields_to_ignore_in_merge(self) -> List[str]:
        """This is used to determine what fields should not be merged when merging two
        entities. This allows for a fine tuned destructive update of an entity.

        This also has special handling during a manifest upload of files. If a manifest
        is specifying fields we'll use those values rather than copying them from the
        existing entity. This is to allow for a destructive update of an entity.

        """
        fields_to_not_merge = []
        if not self.merge_existing_annotations:
            fields_to_not_merge.append("annotations")

        if not self.associate_activity_to_new_version:
            fields_to_not_merge.append("activity")

        if self._present_manifest_fields:
            if "name" in self._present_manifest_fields:
                fields_to_not_merge.append("name")

            if "contentType" in self._present_manifest_fields:
                fields_to_not_merge.append("content_type")

        return fields_to_not_merge

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Store: {self.path if self.path else self.id}"
    )
    async def store_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Store the file in Synapse. With this method you may:

        - Upload a file into Synapse
        - Update the metadata of a file in Synapse
        - Store a File object in Synapse without updating a file by setting
            `synapse_store` to False.
        - Change the name of a file in Synapse by setting the `name` attribute of the
            File object. Also see the [synapseclient.models.File.change_metadata][]
            method for changing the name of the downloaded file.
        - Moving a file to a new parent by setting the `parent_id` attribute of the
            File object.

        If no Name is specified this will be derived from the file name. This is the
        reccommended way to store a file in Synapse.

        Please note:
        The file, as it appears on disk, will be the file that is downloaded from
        Synapse. The name of the actual File is different from the name of the File
        Entity in Synapse. It is generally not reccommended to specify a different
        name for the Entity and the file as it will cause confusion and potential
        conflicts later on.

        Arguments:
            parent: The parent folder or project to store the file in. May also be
                specified in the File object. If both are provided the parent passed
                into `store` will take precedence.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.

        Raises:
            ValueError: If the file does not have an ID and a path, or a path and a
                parent ID, or a data file handle ID and a parent ID.

        Example: Using this function
            File with the ID `syn123` at path `path/to/file.txt`:

                file_instance = await File(id="syn123", path="path/to/file.txt").store_async()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = await File(path="path/to/file.txt", parent_id="syn456").store_async()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = await File(path="path/to/file.txt").store_async(parent=Folder(id="syn456"))

            File with a parent and existing file handle (This allows multiple entities to reference the underlying file):

                file_instance = await File(data_file_handle_id="123", parent_id="syn456").store_async()

            Rename a file (Does not update the file on disk or the name of the downloaded file):

                file_instance = await File(id="syn123", download_file=False).get_async()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                await file_instance.change_metadata_async(name="my_new_name_file.txt")

            Rename a file, and the name of the file as downloaded
                (Does not update the file on disk). Is is reccommended that `name` and
                `download_as` match to prevent confusion later on:

                file_instance = await File(id="syn123", download_file=False).get_async()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                await file_instance.change_metadata_async(name="my_new_name_file.txt", download_as="my_new_name_file.txt")

        """
        self.parent_id = parent.id if parent else self.parent_id
        if self._cannot_store():
            raise ValueError(
                "The file must have an (ID with a (path or `data_file_handle_id`)), or a "
                "(path with a (`parent_id` or parent with an id)), or a "
                "(data_file_handle_id with a (`parent_id` or parent with an id)) to store."
            )
        self.name = self.name or (guess_file_name(self.path) if self.path else None)
        client = Synapse.get_client(synapse_client=synapse_client)

        if existing_file := await self._find_existing_file(synapse_client=client):
            merge_dataclass_entities(
                source=existing_file,
                destination=self,
                fields_to_ignore=self._determine_fields_to_ignore_in_merge(),
            )

        if self.path:
            self.path = os.path.expanduser(self.path)
            async with client._get_parallel_file_transfer_semaphore(
                asyncio_event_loop=asyncio.get_running_loop()
            ):
                await self._upload_file(synapse_client=client)
        elif self.data_file_handle_id:
            self.path = client.cache.get(file_handle_id=self.data_file_handle_id)

        if self.has_changed:
            synapse_file = Synapse_File(
                id=self.id,
                path=self.path,
                description=self.description,
                etag=self.etag,
                name=self.name,
                parent=parent.id if parent else self.parent_id,
                contentType=self.content_type,
                contentSize=self.content_size,
                dataFileHandleId=self.data_file_handle_id,
                synapseStore=self.synapse_store,
                modifiedOn=self.modified_on,
                versionLabel=self.version_label,
                versionNumber=self.version_number,
                versionComment=self.version_comment,
            )
            delete_none_keys(synapse_file)

            entity = await store_entity(
                resource=self, entity=synapse_file, synapse_client=client
            )

            self.fill_from_dict(synapse_file=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self, synapse_client=client
        )
        if re_read_required:
            before_download_file = self.download_file
            self.download_file = False
            await self.get_async(
                synapse_client=client,
            )
            self.download_file = before_download_file

        self._set_last_persistent_instance()

        client.logger.debug(f"Stored File {self.name}, id: {self.id}: {self.path}")
        # Clear the content_md5 so that it is recalculated if the file is updated
        self.content_md5 = None
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Change_Metadata: {self.id}"
    )
    async def change_metadata_async(
        self,
        name: Optional[str] = None,
        download_as: Optional[str] = None,
        content_type: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Change File Entity metadata for properties that are immutable after creation
        through the store method.

        Arguments:
            name: Specify to change the filename of a file as seen on Synapse.
            download_as: Specify filename to change the filename of a filehandle.
            content_type: Specify content type to change the content type of a
                filehandle.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Can be used to change the filename, the filename when the file is
            downloaded, or the file content-type without downloading:

                file_entity = await File(id="syn123", download_file=False).get_async()
                print(os.path.basename(file_entity.path))  ## prints, e.g., "my_file.txt"
                file_entity = await file_entity.change_metadata_async(name="my_new_name_file.txt", download_as="my_new_downloadAs_name_file.txt", content_type="text/plain")
                print(os.path.basename(file_entity.path))  ## prints, "my_new_downloadAs_name_file.txt"
                print(file_entity.name) ## prints, "my_new_name_file.txt"

        Raises:
            ValueError: If the file does not have an ID to change metadata.
        """
        if not self.id:
            raise ValueError("The file must have an ID to change metadata.")
        from synapseutils.copy_functions import changeFileMetaData

        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        syn = Synapse.get_client(synapse_client=synapse_client)
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: changeFileMetaData(
                    syn=syn,
                    entity=self.id,
                    name=name,
                    downloadAs=download_as,
                    contentType=content_type,
                    forceVersion=self.force_version,
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_file=entity, set_annotations=True)
        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Change metadata for file {self.name}, id: {self.id}: {self.path}"
        )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Get: {self.id}, {self.path}"
    )
    async def get_async(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Get the file from Synapse. You may retrieve a File entity by either:

        - id
        - path


        If you specify both, the `id` will take precedence.


        If you specify the `path` and the file is stored in multiple locations in
        Synapse only the first one found will be returned. The other matching files
        will be printed to the console.


        You may also specify a `version_number` to get a specific version of the file.

        Arguments:
            include_activity: If True the activity will be included in the file
                if it exists.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.

        Raises:
            ValueError: If the file does not have an ID or path to get.


        Example: Using this function
            Assuming you have a file with the ID "syn123":

                file_instance = await File(id="syn123").get_async()

            Assuming you want to download a file to this directory: "path/to/directory":

                file_instance = await File(path="path/to/directory").get_async()
        """
        if not self.id and not self.path:
            raise ValueError("The file must have an ID or path to get.")
        syn = Synapse.get_client(synapse_client=synapse_client)

        await self._load_local_md5()

        await get_from_entity_factory(
            entity_to_update=self,
            synapse_id_or_path=self.id or self.path,
            version=self.version_number,
            if_collision=self.if_collision,
            limit_search=self.synapse_container_limit or self.parent_id,
            download_file=self.download_file,
            download_location=os.path.dirname(self.path)
            if self.path and os.path.isfile(self.path)
            else self.path,
            md5=self.content_md5,
        )

        if (
            self.data_file_handle_id
            and (not self.path or (self.path and not os.path.isfile(self.path)))
            and (cached_path := syn.cache.get(file_handle_id=self.data_file_handle_id))
        ):
            self.path = cached_path

        if include_activity:
            self.activity = await Activity.from_parent_async(
                parent=self, synapse_client=synapse_client
            )

        self._set_last_persistent_instance()
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Got file {self.name}, id: {self.id}, path: {self.path}"
        )
        return self

    @classmethod
    async def from_id_async(
        cls,
        synapse_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Wrapper for [synapseclient.models.File.get][].

        Arguments:
            synapse_id: The ID of the file in Synapse.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Assuming you have a file with the ID "syn123":

                file_instance = await File.from_id_async(synapse_id="syn123")
        """
        return await cls(id=synapse_id).get_async(
            synapse_client=synapse_client,
        )

    @classmethod
    async def from_path_async(
        cls,
        path: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """Get the file from Synapse. If the path of the file matches multiple files
        within Synapse the first one found will be returned. The other matching
        files will be printed to the console.


        Wrapper for [synapseclient.models.File.get][].

        Arguments:
            path: The path to the file on disk.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The file object.

        Example: Using this function
            Assuming you have a file at the path "path/to/file.txt":

                file_instance = await File.from_path_async(path="path/to/file.txt")
        """
        return await cls(path=path).get_async(
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Delete: {self.id}"
    )
    async def delete_async(
        self,
        version_only: Optional[bool] = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the file from Synapse using the ID of the file.

        Arguments:
            version_only: If True only the version specified in the `version_number`
                attribute of the file will be deleted. If False the entire file will
                be deleted.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            None

        Raises:
            ValueError: If the file does not have an ID to delete.
            ValueError: If the file does not have a version number to delete a version,
                and `version_only` is True.

        Example: Using this function
            Assuming you have a file with the ID "syn123":

                await File(id="syn123").delete_async()
        """
        if not self.id:
            raise ValueError("The file must have an ID to delete.")
        if version_only and not self.version_number:
            raise ValueError("The file must have a version number to delete a version.")

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                    obj=self.id,
                    version=self.version_number if version_only else None,
                ),
                current_context,
            ),
        )
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Deleted file {self.id}"
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Copy: {self.id}"
    )
    async def copy_async(
        self,
        parent_id: str,
        update_existing: bool = False,
        copy_annotations: bool = True,
        copy_activity: Union[str, None] = "traceback",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """
        Copy the file to another Synapse location. Defaults to the latest version of the
        file, or the version_number specified in the instance.

        Arguments:
            parent_id: Synapse ID of a folder/project that the copied entity is being
                copied to
            update_existing: When the destination has a file that has the same name,
                users can choose to update that file.
            copy_annotations: True to copy the annotations.
            copy_activity: Has three options to set the activity of the copied file:

                    - traceback: Creates a copy of the source files Activity.
                    - existing: Link to the source file's original Activity (if it exists)
                    - None: No activity is set
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The copied file object.

        Example: Using this function
            Assuming you have a file with the ID "syn123" and you want to copy it to a folder with the ID "syn456":

                new_file_instance = await File(id="syn123").copy_async(parent_id="syn456")

            Copy the file but do not persist annotations or activity:

                new_file_instance = await File(id="syn123").copy_async(parent_id="syn456", copy_annotations=False, copy_activity=None)

        Raises:
            ValueError: If the file does not have an ID and parent_id to copy.
        """
        if not self.id or not parent_id:
            raise ValueError("The file must have an ID and parent_id to copy.")
        from synapseutils.copy_functions import copy

        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        syn = Synapse.get_client(synapse_client=synapse_client)
        source_and_destination = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: copy(
                    syn=syn,
                    version=self.version_number,
                    entity=self.id,
                    destinationId=parent_id,
                    skipCopyAnnotations=not copy_annotations,
                    updateExisting=update_existing,
                    setProvenance=copy_activity,
                ),
                current_context,
            ),
        )

        parent_id = source_and_destination.get(self.id, None)
        if not parent_id:
            raise SynapseError("Failed to copy file.")
        file_copy = await File(id=parent_id, download_file=False).get_async(
            synapse_client=synapse_client
        )
        file_copy.download_file = True
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Copied from file {self.id} to {parent_id} with new id of {file_copy.id}"
        )
        return file_copy

    async def _needs_upload(self, syn: Synapse) -> bool:
        """
        Determines if a file needs to be uploaded to Synapse. The following conditions
        apply:

        - The file exists and is an ExternalFileHandle and the url has changed
        - The file exists and is a local file and the MD5 has changed
        - The file is not present in Synapse

        If the file is already specifying a data_file_handle_id then it is assumed that
        the file is already uploaded to Synapse. It does not need to be uploaded and
        the only thing that will occur is the File metadata will be added to Synapse
        outside of this upload process.

        Arguments:
            syn: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            True if the file needs to be uploaded, otherwise False.
        """
        needs_upload = False
        # Check if the file should be uploaded
        if self._last_persistent_instance is not None:
            if (
                self.file_handle
                and self.file_handle.concrete_type
                == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
            ):
                # switching away from ExternalFileHandle or the url was updated
                needs_upload = self.synapse_store or (
                    self.file_handle.external_url != self.external_url
                )
            else:
                # Check if we need to upload a new version of an existing
                # file. If the file referred to by entity['path'] has been
                # modified, we want to upload the new version.
                # If synapeStore is false then we must upload a ExternalFileHandle
                needs_upload = (
                    not self.synapse_store
                    or not self.file_handle
                    or not (
                        exists_in_cache := syn.cache.contains(
                            self.file_handle.id, self.path
                        )
                    )
                )

                md5_stored_in_synapse = (
                    self.file_handle.content_md5 if self.file_handle else None
                )

                # Check if we got an MD5 checksum from Synapse and compare it to the local file
                if (
                    self.synapse_store
                    and needs_upload
                    and os.path.isfile(self.path)
                    and md5_stored_in_synapse
                ):
                    await self._load_local_md5()
                    if md5_stored_in_synapse == (
                        local_file_md5_hex := self.content_md5
                    ):
                        needs_upload = False

                    # If we had a cache miss, but already uploaded to Synapse we
                    # can add the file to the cache.
                    if (
                        not exists_in_cache
                        and self.file_handle
                        and self.file_handle.id
                        and local_file_md5_hex
                    ):
                        syn.cache.add(
                            file_handle_id=self.file_handle.id,
                            path=self.path,
                            md5=local_file_md5_hex,
                        )
        elif self.data_file_handle_id is not None:
            needs_upload = False
        else:
            needs_upload = True
        return needs_upload

    async def _upload_file(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "File":
        """The upload process for a file. This will upload the file to Synapse if it
        needs to be uploaded. If the file does not need to be uploaded the file
        metadata will be added to Synapse outside of this upload process.

        Arguments:
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.
        """
        syn = Synapse.get_client(synapse_client=synapse_client)

        needs_upload = await self._needs_upload(syn=syn)

        if needs_upload:
            parent_id_for_upload = self.parent_id

            if not parent_id_for_upload:
                raise SynapseMalformedEntityError(
                    "Entities of type File must have a parentId."
                )

            updated_file_handle = await upload_file_handle(
                syn=syn,
                parent_entity_id=parent_id_for_upload,
                path=(
                    self.path
                    if (self.synapse_store or self.external_url is None)
                    else self.external_url
                ),
                synapse_store=self.synapse_store,
                md5=self.content_md5,
                file_size=self.content_size,
                mimetype=self.content_type,
            )

            self.file_handle = FileHandle().fill_from_dict(updated_file_handle)
            self._fill_from_file_handle()

        return self

    def _convert_into_legacy_file(self) -> SynapseFile:
        """Convert the file object into a SynapseFile object."""
        return_data = SynapseFile(
            id=self.id,
            name=self.name,
            description=self.description,
            etag=self.etag,
            createdOn=self.created_on,
            modifiedOn=self.modified_on,
            createdBy=self.created_by,
            modifiedBy=self.modified_by,
            parentId=self.parent_id,
            versionNumber=self.version_number,
            versionLabel=self.version_label,
            versionComment=self.version_comment,
            dataFileHandleId=self.data_file_handle_id,
            path=self.path,
            properties={
                "isLatestVersion": self.is_latest_version,
            },
            _file_handle=(
                self.file_handle._convert_into_legacy_file_handle()
                if self.file_handle
                else None
            ),
            annotations=self.annotations,
        )
        delete_none_keys(return_data)
        return return_data
