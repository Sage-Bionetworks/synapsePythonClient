"""Script to work with Synapse files."""

import asyncio
import dataclasses
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.api import get_from_entity_factory
from synapseclient.core import utils
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseFileNotFoundError
from synapseclient.core.utils import (
    delete_none_keys,
    guess_file_name,
    merge_dataclass_entities,
)
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins import AccessControllable, BaseJSONSchema
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity import store_entity
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)

if TYPE_CHECKING:
    from synapseclient.models import CsvTableDescriptor, FileHandle, Folder, Project


@dataclass()
class ValidationSummary:
    """Summary statistics for the JSON schema validation results for the children of
    an Entity container (Project or Folder).

    Attributes:
        container_id: The ID of the container Entity.
        total_number_of_children: The total number of children in the container.
        number_of_valid_children: The total number of children that are valid according
            to their bound JSON schema.
        number_of_invalid_children: The total number of children that are invalid
            according to their bound JSON schema.
        number_of_unknown_children: The total number of children that do not have
            validation results. This can occur when a child does not have a bound JSON
            schema or when a child has not been validated yet.
        generated_on: The date-time when the statistics were calculated.
    """

    container_id: Optional[str] = None
    """The ID of the container Entity."""

    total_number_of_children: Optional[int] = None
    """The total number of children in the container."""

    number_of_valid_children: Optional[int] = None
    """The total number of children that are valid according to their bound JSON schema."""

    number_of_invalid_children: Optional[int] = None
    """The total number of children that are invalid according to their bound JSON schema."""

    number_of_unknown_children: Optional[int] = None
    """The total number of children that do not have validation results. This can occur
    when a child does not have a bound JSON schema or when a child has not been
    validated yet.
    """
    generated_on: Optional[str] = None
    """The date-time when the statistics were calculated."""


class RecordSetSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "RecordSet":
        """
        Store the RecordSet in Synapse.

        This method uploads or updates a RecordSet in Synapse. It can handle both
        creating new RecordSets and updating existing ones based on the
        `create_or_update` flag. The method supports file uploads, metadata updates,
        and merging with existing entities when appropriate.

        Arguments:
            parent: The parent Folder or Project for this RecordSet. If provided,
                this will override the `parent_id` attribute.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The RecordSet object with updated metadata from Synapse after the
            store operation.

        Raises:
            ValueError: If the RecordSet does not have the required information
                for storing. Must have either: (ID with path or data_file_handle_id),
                or (path with parent_id), or (data_file_handle_id with parent_id).

        Example: Storing a new RecordSet
            Creating and storing a new RecordSet in Synapse:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(
                name="My RecordSet",
                description="A dataset for analysis",
                parent_id="syn123456",
                path="/path/to/data.csv"
            )
            stored_record_set = record_set.store()
            print(f"Stored RecordSet with ID: {stored_record_set.id}")
            ```

            Updating an existing RecordSet:
            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(id="syn789012").get()
            record_set.description = "Updated description"
            updated_record_set = record_set.store()

            ```
        """
        return self

    def get(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "RecordSet":
        """
        Get the RecordSet from Synapse.

        This method retrieves a RecordSet entity from Synapse. You may retrieve
        a RecordSet by either its ID or path. If you specify both, the ID will
        take precedence.

        If you specify the path and the RecordSet is stored in multiple locations
        in Synapse, only the first one found will be returned. The other matching
        RecordSets will be printed to the console.

        You may also specify a `version_number` to get a specific version of the
        RecordSet.

        Arguments:
            include_activity: If True, the activity will be included in the RecordSet
                if it exists.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The RecordSet object with data populated from Synapse.

        Raises:
            ValueError: If the RecordSet does not have an ID or path to retrieve.

        Example: Retrieving a RecordSet by ID
            Get an existing RecordSet from Synapse:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(id="syn123").get()
            print(f"RecordSet name: {record_set.name}")
            ```

            Downloading a RecordSet to a specific directory:
            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(
                id="syn123",
                path="/path/to/download/directory"
            ).get()
            ```

            Including activity information:
            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(id="syn123").get(include_activity=True)
            if record_set.activity:
                print(f"Activity: {record_set.activity.name}")
            ```
        """
        return self

    def delete(
        self,
        version_only: Optional[bool] = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the RecordSet from Synapse using its ID.

        This method removes a RecordSet entity from Synapse. You can choose to
        delete either a specific version or the entire RecordSet including all
        its versions.

        Arguments:
            version_only: If True, only the version specified in the `version_number`
                attribute of the RecordSet will be deleted. If False, the entire
                RecordSet including all versions will be deleted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If the RecordSet does not have an ID to delete.
            ValueError: If the RecordSet does not have a version number to delete a
                specific version, and `version_only` is True.

        Example: Deleting a RecordSet
            Delete an entire RecordSet and all its versions:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            RecordSet(id="syn123").delete()

            ```

            Delete only a specific version:
            ```python
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            syn = Synapse()
            syn.login()

            record_set = RecordSet(id="syn123", version_number=2)
            record_set.delete(version_only=True)
            ```
        """
        return None


@dataclass()
@async_to_sync
class RecordSet(RecordSetSynchronousProtocol, AccessControllable, BaseJSONSchema):
    """A RecordSet within Synapse.

    Attributes:
        id: The unique immutable ID for this file. A new ID will be generated for new
            Files. Once issued, this ID is guaranteed to never change or be re-issued.
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus
            signs, apostrophes, and parentheses. If not specified, the name will be
            derived from the file name.
        path: The path to the file on disk. Using shorthand `~` will be expanded to
            the user's home directory.

            This is used during a `get` operation to specify where to download the
            file to. It should be pointing to a directory.

            This is also used during a `store` operation to specify the file to
            upload. It should be pointing to a file.
        description: The description of this file. Must be 1000 characters or less.
        parent_id: The ID of the Entity that is the parent of this Entity. Setting
            this to a new value and storing it will move this File under the new
            parent.
        version_label: The version label for this entity. Updates to the entity will
            increment the version number.
        version_comment: The version comment for this entity.
        data_file_handle_id: ID of the file handle associated with this entity. You
            may define an existing data_file_handle_id to use the existing
            data_file_handle_id. The creator of the file must also be the owner of
            the data_file_handle_id to have permission to store the file.
        activity: The Activity model represents the main record of Provenance in
            Synapse.  It is analygous to the Activity defined in the
            [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance.
            Activity cannot be removed during a store operation by setting it to None.
            You must use: [synapseclient.models.Activity.delete_async][] or
            [synapseclient.models.Activity.disassociate_from_entity_async][].
        annotations: Additional metadata associated with the entity. The key is the
            name of your desired annotations. The value is an object containing a list
            of values (use empty list to represent no values for key) and the value
            type associated with all values in the list. To remove all annotations
            set this to an empty dict `{}`.
        upsert_keys: One or more column names that define this upsert key for this
            set. This key is used to determine if a new record should be treated as
            an update or an insert.
        csv_descriptor: The description of a CSV for upload or download.
        validation_summary: Summary statistics for the JSON schema validation results
            for the children of an Entity container (Project or Folder).
        file_name_override: An optional replacement for the name of the uploaded
            file. This is distinct from the entity name. If omitted the file will
            retain its original name.
        content_type: (New Upload Only) Used to manually specify Content-type header,
            for example 'application/png' or 'application/json; charset=UTF-8'. If not
            specified, the content type will be derived from the file extension.

            This can be specified only during the initial store of this file. In order
            to change this after the File has been created use
            [synapseclient.models.File.change_metadata][].
        content_size: (New Upload Only) The size of the file in bytes. This can be
            specified only during the initial creation of the File. This is also only
            applicable to files not uploaded to Synapse. ie: `synapse_store` is False.
        content_md5: (Store only) The MD5 of the file is known. If not supplied this
            will be computed in the client is possible. If supplied for a file entity
            already stored in Synapse it will be calculated again to check if a new
            upload needs to occur. This will not be filled in during a read for data.
            It is only used during a store operation. To retrieve the md5 of the file
            after read from synapse use the `.file_handle.content_md5` attribute.
        create_or_update: (Store only) Indicates whether the method should
            automatically perform an update if the file conflicts with an existing
            Synapse object.
        force_version: (Store only) Indicates whether the method should increment the
            version of the object if something within the entity has changed. For
            example updating the description or name. You may set this to False and
            an update to the entity will not increment the version.

            Updating the `version_label` attribute will also cause a version update
            regardless of this flag.

            An update to the MD5 of the file will force a version update regardless
            of this flag.
        is_restricted: (Store only) If set to true, an email will be sent to the
            Synapse access control team to start the process of adding terms-of-use
            or review board approval for this entity. You will be contacted with
            regards to the specific data being restricted and the requirements of
            access.

            This may be used only by an administrator of the specified file.
        merge_existing_annotations: (Store only) Works in conjunction with
            `create_or_update` in that this is only evaluated if `create_or_update`
            is True. If this entity exists in Synapse that has annotations that are
            not present in a store operation, these annotations will be added to the
            entity. If this is False any annotations that are not present within a
            store operation will be removed from this entity. This allows one to
            complete a destructive update of annotations on an entity.
        associate_activity_to_new_version: (Store only) Works in conjunction with
            `create_or_update` in that this is only evaluated if `create_or_update`
            is True. When true an activity already attached to the current version of
            this entity will be associated the new version during a store operation
            if the version was updated. This is useful if you are updating the entity
            and want to ensure that the activity is persisted onto the new version
            the entity.

            When this is False the activity will not be associated to the new version
            of the entity during a store operation.

            Regardless of this setting, if you have an Activity object on the entity
            it will be persisted onto the new version. This is only used when you
            don't have an Activity object on the entity.
        synapse_store: (Store only) Whether the File should be uploaded or if false:
            only the path should be stored when [synapseclient.models.File.store][]
            is called.
        download_file: (Get only) If True the file will be downloaded.
        if_collision: (Get only) Determines how to handle file collisions. Defaults
            to "keep.both". May be:

            - `overwrite.local`
            - `keep.local`
            - `keep.both`
        synapse_container_limit: (Get only) A Synanpse ID used to limit the search in
            Synapse if file is specified as a local file. That is, if the file is
            stored in multiple locations in Synapse only the ones in the specified
            folder/project will be returned.
        etag: (Read Only) Synapse employs an Optimistic Concurrency Control (OCC)
            scheme to handle concurrent updates. Since the E-Tag changes every time
            an entity is updated it is used to detect when a client's current
            representation of an entity is out-of-date.
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

    upsert_keys: Optional[List[str]] = field(default_factory=list)
    """One or more column names that define this upsert key for this set. This key is
    used to determine if a new record should be treated as an update or an insert.
    """

    csv_descriptor: Optional["CsvTableDescriptor"] = field(default=None)
    """The description of a CSV for upload or download."""

    validation_summary: Optional[ValidationSummary] = field(default=None, compare=False)
    """Summary statistics for the JSON schema validation results for the children of
    an Entity container (Project or Folder)"""

    file_name_override: Optional[str] = None
    """An optional replacement for the name of the uploaded file. This is distinct from
    the entity name. If omitted the file will retain its original name.
    """

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

    file_handle: Optional["FileHandle"] = field(default=None, compare=False)
    """(Read Only) The file handle associated with this entity."""

    _last_persistent_instance: Optional["RecordSet"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    @property
    def has_changed(self) -> bool:
        """
        Determines if the object has been changed and needs to be updated in Synapse."""
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

    def fill_from_dict(
        self,
        entity: Dict[str, Union[bool, str, int]],
        set_annotations: bool = True,
    ) -> "RecordSet":
        """
        Converts a response from the REST API into this dataclass.

        This method populates the RecordSet instance with data from a Synapse REST API
        response or a dictionary containing RecordSet information. It handles the
        conversion from Synapse API field names (camelCase) to Python attribute names
        (snake_case) and processes nested objects appropriately.

        Arguments:
            synapse_file: The response from the REST API or a dictionary containing
                RecordSet data. Can be either a Synapse_File object or a dictionary
                with string keys and various value types.
            set_annotations: Whether to set the annotations from the response.
                If True, annotations will be populated from the API response.

        Returns:
            The RecordSet object with updated attributes from the API response.
        """
        self.id = entity.get("id", None)
        self.name = entity.get("name", None)
        self.description = entity.get("description", None)
        self.etag = entity.get("etag", None)
        self.created_on = entity.get("createdOn", None)
        self.modified_on = entity.get("modifiedOn", None)
        self.created_by = entity.get("createdBy", None)
        self.modified_by = entity.get("modifiedBy", None)
        self.parent_id = entity.get("parentId", None)
        self.version_number = entity.get("versionNumber", None)
        self.version_label = entity.get("versionLabel", None)
        self.version_comment = entity.get("versionComment", None)
        self.is_latest_version = entity.get("isLatestVersion", False)
        self.data_file_handle_id = entity.get("dataFileHandleId", None)
        self.path = entity.get("path", self.path)
        self.file_name_override = entity.get("fileNameOverride", None)
        csv_descriptor = entity.get("csvDescriptor", None)
        if csv_descriptor:
            from synapseclient.models import CsvTableDescriptor

            self.csv_descriptor = CsvTableDescriptor().fill_from_dict(csv_descriptor)

        validation_summary = entity.get("validationSummary", None)

        if validation_summary:
            self.validation_summary = ValidationSummary(
                container_id=validation_summary.get("containerId", None),
                total_number_of_children=validation_summary.get(
                    "totalNumberOfChildren", None
                ),
                number_of_valid_children=validation_summary.get(
                    "numberOfValidChildren", None
                ),
                number_of_invalid_children=validation_summary.get(
                    "numberOfInvalidChildren", None
                ),
                number_of_unknown_children=validation_summary.get(
                    "numberOfUnknownChildren", None
                ),
                generated_on=validation_summary.get("generatedOn", None),
            )

        self.upsert_keys = entity.get("upsertKey", [])

        synapse_file_handle = entity.get("_file_handle", None)
        if synapse_file_handle:
            from synapseclient.models import FileHandle

            file_handle = self.file_handle or FileHandle()
            self.file_handle = file_handle.fill_from_dict(
                synapse_instance=synapse_file_handle
            )
            self._fill_from_file_handle()

        if set_annotations:
            self.annotations = Annotations.from_dict(entity.get("annotations", {}))
        return self

    def _cannot_store(self) -> bool:
        """
        Determines based on guard conditions if a store operation can proceed.
        """
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
        """
        Load the MD5 hash of a local file if it exists and hasn't been loaded yet.

        This method computes and sets the content_md5 attribute for local files
        that exist on disk. It only performs the calculation if the content_md5
        is not already set and the path points to an existing file.
        """
        if not self.content_md5 and self.path and os.path.isfile(self.path):
            self.content_md5 = utils.md5_for_file_hex(filename=self.path)

    async def _find_existing_entity(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> Union["RecordSet", None]:
        """
        Determines if the RecordSet already exists in Synapse.

        This method searches for an existing RecordSet in Synapse that matches the
        current instance. If found, it returns the existing RecordSet object, otherwise
        it returns None. This is used to determine if the RecordSet should be updated
        or created during a store operation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled, this will
                use the last created instance from the Synapse class constructor.

        Returns:
            The existing RecordSet object if it exists in Synapse, None otherwise.
        """

        async def get_entity(existing_id: str) -> "RecordSet":
            """Small wrapper to retrieve a file instance without raising an error if it
            does not exist.

            Arguments:
                existing_id: The ID of the file to retrieve.

            Returns:
                The file object if it exists, otherwise None.
            """
            try:
                entity_copy = RecordSet(
                    id=existing_id,
                    download_file=False,
                    version_number=self.version_number,
                    synapse_container_limit=self.synapse_container_limit,
                    parent_id=self.parent_id,
                )
                return await entity_copy.get_async(
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
                existing_entity_id := await get_id(
                    entity=self,
                    failure_strategy=None,
                    synapse_client=synapse_client,
                )
            )
            and (existing_file := await get_entity(existing_entity_id))
        ):
            return existing_file
        return None

    def _determine_fields_to_ignore_in_merge(self) -> List[str]:
        """
        Determine which fields should not be merged when merging two entities.

        This method returns a list of field names that should be ignored during
        entity merging operations. This allows for fine-tuned destructive updates
        of an entity based on the current configuration settings.

        The method has special handling for manifest uploads where specific fields
        are provided in the manifest and should take precedence over existing
        entity values.

        Returns:
            A list of field names that should not be merged from the existing entity.
        """
        fields_to_not_merge = []
        if not self.merge_existing_annotations:
            fields_to_not_merge.append("annotations")

        if not self.associate_activity_to_new_version:
            fields_to_not_merge.append("activity")

        return fields_to_not_merge

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        This method transforms the RecordSet object into a dictionary format that
        matches the structure expected by the Synapse REST API. It handles the
        conversion of Python snake_case attribute names to the camelCase format
        used by the API, and ensures that nested objects are properly serialized.

        Returns:
            A dictionary representation of this object formatted for API requests.
            None values are automatically removed from the dictionary.

        Example: Converting a RecordSet for API submission
            This method is used internally when storing or updating RecordSets:

            ```python
            from synapseclient.models import RecordSet

            record_set = RecordSet(
                name="My RecordSet",
                description="A test record set",
                parent_id="syn123456"
            )
            api_dict = record_set.to_synapse_request()
            # api_dict contains properly formatted data for the REST API
            ```
        """

        entity = {
            "concreteType": concrete_types.RECORD_SET_ENTITY,
            "name": self.name,
            "description": self.description,
            "id": self.id,
            "etag": self.etag,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "versionNumber": self.version_number,
            "versionLabel": self.version_label,
            "versionComment": self.version_comment,
            "isLatestVersion": self.is_latest_version,
            "dataFileHandleId": self.data_file_handle_id,
            "upsertKey": self.upsert_keys,
            "csvDescriptor": self.csv_descriptor.to_synapse_request()
            if self.csv_descriptor
            else None,
            "validationSummary": {
                "containerId": self.validation_summary.container_id,
                "totalNumberOfChildren": self.validation_summary.total_number_of_children,
                "numberOfValidChildren": self.validation_summary.number_of_valid_children,
                "numberOfInvalidChildren": self.validation_summary.number_of_invalid_children,
                "numberOfUnknownChildren": self.validation_summary.number_of_unknown_children,
                "generatedOn": self.validation_summary.generated_on,
            }
            if self.validation_summary
            else None,
            "fileNameOverride": self.file_name_override,
        }
        delete_none_keys(entity)

        return entity

    async def store_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "RecordSet":
        """
        Store the RecordSet in Synapse.

        This method uploads or updates a RecordSet in Synapse. It can handle both
        creating new RecordSets and updating existing ones based on the
        `create_or_update` flag. The method supports file uploads, metadata updates,
        and merging with existing entities when appropriate.

        Arguments:
            parent: The parent Folder or Project for this RecordSet. If provided,
                this will override the `parent_id` attribute.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The RecordSet object with updated metadata from Synapse after the
            store operation.

        Raises:
            ValueError: If the RecordSet does not have the required information
                for storing. Must have either: (ID with path or data_file_handle_id),
                or (path with parent_id), or (data_file_handle_id with parent_id).

        Example: Storing a new RecordSet
            Creating and storing a new RecordSet in Synapse:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = RecordSet(
                    name="My RecordSet",
                    description="A dataset for analysis",
                    parent_id="syn123456",
                    path="/path/to/data.csv"
                )
                stored_record_set = await record_set.store_async()
                print(f"Stored RecordSet with ID: {stored_record_set.id}")

            asyncio.run(main())
            ```

            Updating an existing RecordSet:
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = await RecordSet(id="syn789012").get_async()
                record_set.description = "Updated description"
                updated_record_set = await record_set.store_async()

            asyncio.run(main())
            ```
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

        if existing_file := await self._find_existing_entity(synapse_client=client):
            merge_dataclass_entities(
                source=existing_file,
                destination=self,
                fields_to_ignore=self._determine_fields_to_ignore_in_merge(),
            )

        if self.id:
            trace.get_current_span().set_attributes(
                {
                    "synapse.id": self.id,
                }
            )

        if self.path:
            self.path = os.path.expanduser(self.path)
            async with client._get_parallel_file_transfer_semaphore(
                asyncio_event_loop=asyncio.get_running_loop()
            ):
                from synapseclient.models.file import _upload_file

                await _upload_file(entity_to_upload=self, synapse_client=client)
        elif self.data_file_handle_id:
            self.path = client.cache.get(file_handle_id=self.data_file_handle_id)

        if self.has_changed:
            entity = await store_entity(
                resource=self, entity=self.to_synapse_request(), synapse_client=client
            )

            self.fill_from_dict(entity=entity, set_annotations=False)

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

    async def get_async(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "RecordSet":
        """
        Get the RecordSet from Synapse.

        This method retrieves a RecordSet entity from Synapse. You may retrieve
        a RecordSet by either its ID or path. If you specify both, the ID will
        take precedence.

        If you specify the path and the RecordSet is stored in multiple locations
        in Synapse, only the first one found will be returned. The other matching
        RecordSets will be printed to the console.

        You may also specify a `version_number` to get a specific version of the
        RecordSet.

        Arguments:
            include_activity: If True, the activity will be included in the RecordSet
                if it exists.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The RecordSet object with data populated from Synapse.

        Raises:
            ValueError: If the RecordSet does not have an ID or path to retrieve.

        Example: Retrieving a RecordSet by ID
            Get an existing RecordSet from Synapse:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = await RecordSet(id="syn123").get_async()
                print(f"RecordSet name: {record_set.name}")

            asyncio.run(main())
            ```

            Downloading a RecordSet to a specific directory:
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = await RecordSet(
                    id="syn123",
                    path="/path/to/download/directory"
                ).get_async()

            asyncio.run(main())
            ```

            Including activity information:
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = await RecordSet(id="syn123").get_async(include_activity=True)
                if record_set.activity:
                    print(f"Activity: {record_set.activity.name}")

            asyncio.run(main())
            ```
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
            synapse_client=syn,
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

    async def delete_async(
        self,
        version_only: Optional[bool] = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete the RecordSet from Synapse using its ID.

        This method removes a RecordSet entity from Synapse. You can choose to
        delete either a specific version or the entire RecordSet including all
        its versions.

        Arguments:
            version_only: If True, only the version specified in the `version_number`
                attribute of the RecordSet will be deleted. If False, the entire
                RecordSet including all versions will be deleted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)`, this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If the RecordSet does not have an ID to delete.
            ValueError: If the RecordSet does not have a version number to delete a
                specific version, and `version_only` is True.

        Example: Deleting a RecordSet
            Delete an entire RecordSet and all its versions:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                await RecordSet(id="syn123").delete_async()

            asyncio.run(main())
            ```

            Delete only a specific version:
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import RecordSet

            async def main():
                syn = Synapse()
                syn.login()

                record_set = RecordSet(id="syn123", version_number=2)
                await record_set.delete_async(version_only=True)

            asyncio.run(main())
            ```
        """
        if not self.id:
            raise ValueError("The file must have an ID to delete.")
        if version_only and not self.version_number:
            raise ValueError("The file must have a version number to delete a version.")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                obj=self.id,
                version=self.version_number if version_only else None,
            ),
        )
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Deleted file {self.id}"
        )
