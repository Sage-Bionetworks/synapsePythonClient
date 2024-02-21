import asyncio
import dataclasses
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from opentelemetry import context, trace

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.utils import (
    delete_none_keys,
    guess_file_name,
    run_and_attach_otel_context,
)
from synapseclient.entity import File as Synapse_File
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.protocols.file_protocol import FileSynchronousProtocol
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)
from synapseutils.copy_functions import changeFileMetaData, copy

if TYPE_CHECKING:
    from synapseclient.models import Folder, Project


tracer = trace.get_tracer("synapseclient")


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
        path: The path to the file on disk.
        content_type: Used to manually specify Content-type header, for example
            'application/png' or 'application/json; charset=UTF-8'. If not specified,
            the content type will be derived from the file extension.


            (Create Only)
            This can be specified only during the initial store of this file.
            In order to change this after the File has been created use
            [synapseclient.models.File.change_metadata][].
        description: The description of this file. Must be 1000 characters or less.
        etag: (Read Only) Synapse employs an Optimistic Concurrency Control (OCC) scheme
            to handle concurrent updates. Since the E-Tag changes every time an entity
            is updated it is used to detect when a client's current representation of an
            entity is out-of-date.
        created_on: (Read Only) The date this entity was created.
        modified_on: (Read Only) The date this entity was last modified.
        created_by: (Read Only) The ID of the user that created this entity.
        modified_by: (Read Only) The ID of the user that last modified this entity.
        parent_id: The ID of the Entity that is the parent of this Entity. Setting this
            to a new value and storing it will move this File under the new parent.
        version_number: (Read Only) The version number issued to this version on the
            object.
        version_label: The version label for this entity
        version_comment: The version comment for this entity
        is_latest_version: (Read Only) If this is the latest version of the object.
        data_file_handle_id: ID of the file associated with this entity. You may define
            an existing data_file_handle_id to use the existing data_file_handle_id. The
            creator of the file must also be the owner of the data_file_handle_id to
            have permission to store the file.
        file_handle: (Read Only) The file handle associated with this entity.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance.
        annotations: Additional metadata associated with the folder. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list. To remove all annotations set this
            to an empty dict `{}`.
        create_or_update: (Store only)
            Indicates whether the method should automatically perform an
            update if the 'obj' conflicts with an existing Synapse object.
        force_version: (Store only)
            Indicates whether the method should increment the version of the
            object even if nothing has changed. An update to the MD5 of the file will
            force a version update regardless of this flag.
        is_restricted: (Store only)
            If set to true, an email will be sent to the Synapse access control
            team to start the process of adding terms-of-use or review board approval
            for this entity. You will be contacted with regards to the specific data
            being restricted and the requirements of access.
        synapse_store: (Store only)
            Whether the File should be uploaded or if false: only the path should
            be stored when [synapseclient.models.File.store][] is called.
        download_file: (Get only) If True the file will be downloaded.
        download_location: (Get only) The location to download the file to.
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

    path: Optional[str] = None
    """The path to the file on disk."""

    content_type: Optional[str] = None
    """
    Used to manually specify Content-type header, for example 'application/png'
    or 'application/json; charset=UTF-8'. If not specified, the content type will be
    derived from the file extension.


    (Create Only)
    This can be specified only during the initial store of this file. In order to change
    this after the File has been created use
    [synapseclient.models.File.change_metadata][].
    """

    description: Optional[str] = None
    """The description of this file. Must be 1000 characters or less."""

    etag: Optional[str] = None
    """
    (Read Only)
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = None
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = None
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this Entity. Setting this to a new
    value and storing it will move this File under the new parent."""

    version_number: Optional[int] = None
    """(Read Only) The version number issued to this version on the object."""

    version_label: Optional[str] = None
    """The version label for this entity"""

    version_comment: Optional[str] = None
    """The version comment for this entity"""

    is_latest_version: Optional[bool] = False
    """(Read Only) If this is the latest version of the object."""

    data_file_handle_id: Optional[str] = None
    """
    ID of the file associated with this entity. You may define an existing
    data_file_handle_id to use the existing data_file_handle_id. The creator of the
    file must also be the owner of the data_file_handle_id to have permission to
    store the file.
    """

    file_handle: Optional[FileHandle] = None
    """(Read Only) The file handle associated with this entity."""

    activity: Optional[Activity] = None
    """The Activity model represents the main record of Provenance in Synapse.  It is
    analygous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

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
    ] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict `{}`."""

    create_or_update: bool = field(default=True, repr=False)
    """
    (Store only)

    Indicates whether the method should automatically perform an update if the file
    conflicts with an existing Synapse object.
    """

    force_version: bool = field(default=True, repr=False)
    """
    (Store only)

    Indicates whether the method should increment the version of the object even if
    nothing has changed. An update to the MD5 of the file will force a version update
    regardless of this flag.
    """

    is_restricted: bool = field(default=False, repr=False)
    """
    (Store only)

    If set to true, an email will be sent to the Synapse access control team to start
    the process of adding terms-of-use or review board approval for this entity.
    You will be contacted with regards to the specific data being restricted and the
    requirements of access.
    """

    synapse_store: bool = field(default=True, repr=False)
    """
    (Store only)

    Whether the File should be uploaded or if false: only the path should be stored when
    [synapseclient.models.File.store][] is called.
    """

    download_file: bool = field(default=True, repr=False)
    """
    (Get only)

    If True the file will be downloaded."""

    download_location: str = field(default=None, repr=False)
    """
    (Get only)

    The location to download the file to."""

    if_collision: str = field(default="keep.both", repr=False)
    """
    (Get only)

    Determines how to handle file collisions. Defaults to "keep.both".
            May be

            - `overwrite.local`
            - `keep.local`
            - `keep.both`
    """

    synapse_container_limit: Optional[str] = field(default=None, repr=False)
    """A Synanpse ID used to limit the search in Synapse if file is specified as a local
    file. That is, if the file is stored in multiple locations in Synapse only the
    ones in the specified folder/project will be returned."""

    _last_persistent_instance: Optional["File"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        del self._last_persistent_instance
        self._last_persistent_instance = dataclasses.replace(self)
        self._last_persistent_instance.activity = (
            dataclasses.replace(self.activity) if self.activity else None
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else None
        )

    def fill_from_dict(
        self, synapse_file: Union[Synapse_File, Dict], set_annotations: bool = True
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
        self.path = synapse_file.get("path", None)
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
        synapse_file_handle = synapse_file.get("_file_handle", None)
        if synapse_file_handle:
            file_handle = self.file_handle or FileHandle()
            self.file_handle = file_handle
            file_handle.id = synapse_file_handle.get("id", None)
            file_handle.etag = synapse_file_handle.get("etag", None)
            file_handle.created_by = synapse_file_handle.get("createdBy", None)
            file_handle.created_on = synapse_file_handle.get("createdOn", None)
            file_handle.modified_on = synapse_file_handle.get("modifiedOn", None)
            file_handle.concrete_type = synapse_file_handle.get("concreteType", None)
            self.content_type = synapse_file_handle.get("contentType", None)
            file_handle.content_type = synapse_file_handle.get("contentType", None)
            file_handle.content_md5 = synapse_file_handle.get("contentMd5", None)
            file_handle.file_name = synapse_file_handle.get("fileName", None)
            file_handle.storage_location_id = synapse_file_handle.get(
                "storageLocationId", None
            )
            file_handle.content_size = synapse_file_handle.get("contentSize", None)
            file_handle.status = synapse_file_handle.get("status", None)
            file_handle.bucket_name = synapse_file_handle.get("bucketName", None)
            file_handle.key = synapse_file_handle.get("key", None)
            file_handle.preview_id = synapse_file_handle.get("previewId", None)
            file_handle.is_preview = synapse_file_handle.get("isPreview", None)
            file_handle.external_url = synapse_file_handle.get("externalURL", None)

        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_file.get("annotations", None)
            )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Store: {self.path if self.path else self.id}"
    )
    async def store_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
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

        Arguments:
            parent: The parent folder or project to store the file in. May also be
                specified in the File object. If both are provided the parent passed
                into `store` will take precedence.
            synapse_client: If not passed in or None this will use the last client from
                the `.login()` method.

        Returns:
            The file object.


        Example: Using this function
            File with the ID `syn123` at path `path/to/file.txt`:

                file_instance = await File(id="syn123", path="path/to/file.txt").store_async()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = await File(path="path/to/file.txt", parent_id="syn456").store_async()

            File at the path `path/to/file.txt` and a parent folder with the ID `syn456`:

                file_instance = await File(path="path/to/file.txt").store_async(parent=Folder(id="syn456"))

            Rename a file (Does not update the file on disk or the name of the downloaded file):

                file_instance = await File(id="syn123", download_file=False).get_async()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                await file_instance.change_metadata_async(name="my_new_name_file.txt")

            Rename a file, and the name of the file as downloaded (Does not update the file on disk):

                file_instance = await File(id="syn123", download_file=False).get_async()
                print(file_instance.name)  ## prints, e.g., "my_file.txt"
                await file_instance.change_metadata_async(name="my_new_name_file.txt", download_as="my_new_name_file.txt")

        """
        if not (
            self.id is not None
            and (self.path is not None or self.data_file_handle_id is not None)
        ) and not (
            self.path is not None
            and (parent.id if parent else self.parent_id) is not None
        ):
            raise ValueError(
                "The file must have an (ID with a (path or `data_file_handle_id`)), or a "
                "(path with a (`parent_id` or parent with an id)) to store."
            )

        loop = asyncio.get_event_loop()
        synapse_file = Synapse_File(
            id=self.id,
            path=self.path,
            description=self.description,
            etag=self.etag,
            name=self.name or (guess_file_name(self.path) if self.path else None),
            parent=parent.id if parent else self.parent_id,
            contentType=self.content_type,
            dataFileHandleId=self.data_file_handle_id,
            synapseStore=self.synapse_store,
            modifiedOn=self.modified_on,
            versionLabel=self.version_label,
            versionNumber=self.version_number,
            versionComment=self.version_comment,
        )
        delete_none_keys(synapse_file)
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).store(
                    obj=synapse_file,
                    createOrUpdate=self.create_or_update,
                    forceVersion=self.force_version,
                    isRestricted=self.is_restricted,
                    set_annotations=False,
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_file=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self, synapse_client=synapse_client
        )
        if re_read_required:
            before_download_file = self.download_file
            self.download_file = False
            await self.get_async(
                synapse_client=synapse_client,
            )
            self.download_file = before_download_file

        self._set_last_persistent_instance()

        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Stored File {self.name}, id: {self.id}: {self.path}"
        )
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"File_Change_Metadata: {self.id}"
    )
    async def change_metadata_async(
        self,
        name: Optional[str] = None,
        download_as: Optional[str] = None,
        content_type: Optional[str] = None,
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
            Can be used to change the filename, the filename when the file is downloaded, or the file content-type without downloading:

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

            Assuming you have a file at the path "path/to/file.txt":

                file_instance = await File(path="path/to/file.txt").get_async()
        """
        if not self.id and not self.path:
            raise ValueError("The file must have an ID or path to get.")

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        entity = await loop.run_in_executor(
            None,
            lambda: run_and_attach_otel_context(
                lambda: Synapse.get_client(synapse_client=synapse_client).get(
                    entity=self.id or self.path,
                    version=self.version_number,
                    ifcollision=self.if_collision,
                    limitSearch=self.synapse_container_limit,
                    downloadFile=self.download_file,
                    downloadLocation=self.download_location,
                ),
                current_context,
            ),
        )

        self.fill_from_dict(synapse_file=entity, set_annotations=True)

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
