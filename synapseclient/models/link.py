"""Link dataclass model for Synapse entities."""

import dataclasses
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.api import get_from_entity_factory
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import LINK_ENTITY
from synapseclient.core.utils import delete_none_keys, merge_dataclass_entities
from synapseclient.models import Activity, Annotations
from synapseclient.models.services.search import get_id
from synapseclient.models.services.storable_entity import store_entity
from synapseclient.models.services.storable_entity_components import (
    store_entity_components,
)

if TYPE_CHECKING:
    from synapseclient.models import (
        Dataset,
        DatasetCollection,
        EntityView,
        File,
        Folder,
        MaterializedView,
        Project,
        SubmissionView,
        Table,
        VirtualTable,
    )
    from synapseclient.operations.factory_operations import FileOptions


class LinkSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for Link operations."""

    def get(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        follow_link: bool = True,
        file_options: Optional["FileOptions"] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union[
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "MaterializedView",
        "Project",
        "SubmissionView",
        "Table",
        "VirtualTable",
        "Link",
    ]:
        """Get the link metadata from Synapse. You are able to find a link by
        either the id or the name and parent_id.

        Arguments:
            parent: The parent folder or project this link exists under.
            follow_link: If True then the entity this link points to will be fetched
                and returned instead of the Link entity itself.
            file_options: Options that modify file retrieval. Only used if `follow_link`
                is True and the link points to a File entity.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The link object.

        Raises:
            ValueError: If the link does not have an id or a
                (name and (`parent_id` or parent with an id)) set.

        Example: Using this function
            Retrieve a link and follow it to get the target entity:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Link

            syn = Synapse()
            syn.login()

            # Get the target entity that the link points to
            target_entity = Link(id="syn123").get()
            ```

            Retrieve only the link metadata without following the link:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Link

            syn = Synapse()
            syn.login()

            # Get just the link entity itself
            link_entity = Link(id="syn123").get(follow_link=False)
            ```

            When the link points to a File, you can specify file download options:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Link, FileOptions

            syn = Synapse()
            syn.login()

            # Follow link to file with custom download options
            file_entity = Link(id="syn123").get(
                file_options=FileOptions(
                    download_file=True,
                    download_location="/path/to/downloads/",
                    if_collision="overwrite.local"
                )
            )
            ```
        """
        return self

    def store(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Link":
        """Store the link in Synapse.

        Arguments:
            parent: The parent folder or project to store the link in. May also be
                specified in the Link object. If both are provided the parent passed
                into `store` will take precedence.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The link object.

        Raises:
            ValueError: If the link does not have a name and parent_id, or target_id.

        Example: Using this function
            Link with the name `my_link` referencing entity `syn123` and parent folder `syn456`:

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Link

            syn = Synapse()
            syn.login()

            link_instance = Link(
                name="my_link",
                parent_id="syn456",
                target_id="syn123"
            ).store()
            ```
        """
        return self


@dataclass()
@async_to_sync
class Link(LinkSynchronousProtocol):
    """A Link entity within Synapse that references another entity.

    Represents a [Synapse Link](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/Link.html).

    Attributes:
        name: The name of this entity. Must be 256 characters or less. Names may only
            contain: letters, numbers, spaces, underscores, hyphens, periods, plus signs,
            apostrophes, and parentheses
        description: The description of this entity. Must be 1000 characters or less.
        id: The unique immutable ID for this entity. A new ID will be generated for new
            Entities. Once issued, this ID is guaranteed to never change or be re-issued
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: (Read Only) The date this entity was created.
        modified_on: (Read Only) The date this entity was last modified.
        created_by: (Read Only) The ID of the user that created this entity.
        modified_by: (Read Only) The ID of the user that last modified this entity.
        parent_id: The ID of the Entity that is the parent of this Entity.
        concrete_type: Indicates which implementation of Entity this object represents.
            The value is the fully qualified class name, e.g. org.sagebionetworks.repo.model.FileEntity.
        target_id: The ID of the entity to which this link refers
        target_version_number: The version number of the entity to which this link refers
        links_to_class_name: The synapse Entity's class name that this link points to.
        activity: The Activity model represents the main record of Provenance in
            Synapse. It is analogous to the Activity defined in the
            W3C Specification on Provenance. Activity cannot be removed during a store
            operation by setting it to None. You must use Activity.delete_async or
            Activity.disassociate_from_entity_async.
        annotations: Additional metadata associated with the link. The key is the name
            of your desired annotations. The value is an object containing a list of
            values (use empty list to represent no values for key) and the value type
            associated with all values in the list. To remove all annotations set this
            to an empty dict {} or None and store the entity.
    """

    name: Optional[str] = None
    """The name of this entity. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    description: Optional[str] = None
    """The description of this entity. Must be 1000 characters or less."""

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new
    Entities. Once issued, this ID is guaranteed to never change or be re-issued"""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated
    it is used to detect when a client's current representation of an entity is
    out-of-date."""

    created_on: Optional[str] = None
    """(Read Only) The date this entity was created."""

    modified_on: Optional[str] = None
    """(Read Only) The date this entity was last modified."""

    created_by: Optional[str] = None
    """(Read Only) The ID of the user that created this entity."""

    modified_by: Optional[str] = None
    """(Read Only) The ID of the user that last modified this entity."""

    parent_id: Optional[str] = None
    """The ID of the Entity that is the parent of this Entity."""

    target_id: Optional[str] = None
    """The ID of the entity to which this link refers"""

    target_version_number: Optional[int] = None
    """The version number of the entity to which this link refers"""

    links_to_class_name: Optional[str] = None
    """The synapse Entity's class name that this link points to."""

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse. It is
    analogous to the Activity defined in the W3C Specification on Provenance. Activity
    cannot be removed during a store operation by setting it to None. You must use
    Activity.delete_async or Activity.disassociate_from_entity_async."""

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
    """Additional metadata associated with the link. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list. To remove all annotations set this to an empty dict {} or
    None and store the entity."""

    _last_persistent_instance: Optional["Link"] = field(
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
        self._last_persistent_instance = replace(self)
        self._last_persistent_instance.activity = (
            dataclasses.replace(self.activity) if self.activity else None
        )
        self._last_persistent_instance.annotations = (
            deepcopy(self.annotations) if self.annotations else {}
        )

    def fill_from_dict(
        self, synapse_entity: Dict[str, Any], set_annotations: bool = True
    ) -> "Link":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_entity: The response from the REST API.
            set_annotations: Whether to set the annotations from the response.

        Returns:
            The Link object.
        """
        self.name = synapse_entity.get("name", None)
        self.description = synapse_entity.get("description", None)
        self.id = synapse_entity.get("id", None)
        self.etag = synapse_entity.get("etag", None)
        self.created_on = synapse_entity.get("createdOn", None)
        self.modified_on = synapse_entity.get("modifiedOn", None)
        self.created_by = synapse_entity.get("createdBy", None)
        self.modified_by = synapse_entity.get("modifiedBy", None)
        self.parent_id = synapse_entity.get("parentId", None)

        # Handle nested Reference object
        links_to_data = synapse_entity.get("linksTo", None)
        if links_to_data:
            self.target_id = links_to_data.get("targetId", None)
            self.target_version_number = links_to_data.get("targetVersionNumber", None)
        else:
            self.target_id = None
            self.target_version_number = None

        self.links_to_class_name = synapse_entity.get("linksToClassName", None)

        if set_annotations:
            self.annotations = Annotations.from_dict(
                synapse_entity.get("annotations", {})
            )

        return self

    def to_synapse_request(self) -> Dict[str, Any]:
        """
        Converts this dataclass to a dictionary suitable for a Synapse REST API request.

        Returns:
            A dictionary representation of this object for API requests.
        """
        request_dict = {
            "name": self.name,
            "description": self.description,
            "id": self.id,
            "etag": self.etag,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "parentId": self.parent_id,
            "concreteType": LINK_ENTITY,
            "linksTo": {
                "targetId": self.target_id,
                "targetVersionNumber": self.target_version_number,
            }
            if self.target_id
            else None,
            "linksToClassName": self.links_to_class_name,
        }
        if request_dict["linksTo"]:
            delete_none_keys(request_dict["linksTo"])
        delete_none_keys(request_dict)
        return request_dict

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Link_Get: {self.id}"
    )
    async def get_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        follow_link: bool = True,
        file_options: Optional["FileOptions"] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union[
        "Dataset",
        "DatasetCollection",
        "EntityView",
        "File",
        "Folder",
        "MaterializedView",
        "Project",
        "SubmissionView",
        "Table",
        "VirtualTable",
        "Link",
    ]:
        """Get the link metadata from Synapse. You are able to find a link by
        either the id or the name and parent_id.

        Arguments:
            parent: The parent folder or project this link exists under.
            follow_link: If True then the entity this link points to will be fetched
                and returned instead of the Link entity itself.
            file_options: Options that modify file retrieval. Only used if `follow_link`
                is True and the link points to a File entity.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The link object.

        Raises:
            ValueError: If the link does not have an id or a
                (name and (`parent_id` or parent with an id)) set.

        Example: Using this function
            Retrieve a link and follow it to get the target entity:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Link

            async def get_link_target():
                syn = Synapse()
                syn.login()

                # Get the target entity that the link points to
                target_entity = await Link(id="syn123").get_async()
                return target_entity

            # Run the async function
            target_entity = asyncio.run(get_link_target())
            ```

            Retrieve only the link metadata without following the link:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Link

            async def get_link_metadata():
                syn = Synapse()
                syn.login()

                # Get just the link entity itself
                link_entity = await Link(id="syn123").get_async(follow_link=False)
                return link_entity

            # Run the async function
            link_entity = asyncio.run(get_link_metadata())
            ```

            When the link points to a File, you can specify file download options:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Link, FileOptions

            async def get_link_with_file_options():
                syn = Synapse()
                syn.login()

                # Follow link to file with custom download options
                file_entity = await Link(id="syn123").get_async(
                    file_options=FileOptions(
                        download_file=True,
                        download_location="/path/to/downloads/",
                        if_collision="overwrite.local"
                    )
                )
                return file_entity

            # Run the async function
            file_entity = asyncio.run(get_link_with_file_options())
            ```
        """
        parent_id = parent.id if parent else self.parent_id
        if not (self.id or (self.name and parent_id)):
            raise ValueError(
                "The link must have an id or a "
                "(name and (`parent_id` or parent with an id)) set."
            )
        self.parent_id = parent_id

        entity_id = await get_id(entity=self, synapse_client=synapse_client)

        await get_from_entity_factory(
            synapse_id_or_path=entity_id,
            entity_to_update=self,
            synapse_client=synapse_client,
        )
        self._set_last_persistent_instance()

        if follow_link:
            from synapseclient.operations.factory_operations import (
                get_async as factory_get_async,
            )

            return await factory_get_async(
                synapse_id=self.target_id,
                version_number=self.target_version_number,
                file_options=file_options,
                synapse_client=synapse_client,
            )
        else:
            return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Link_Store: {self.name}"
    )
    async def store_async(
        self,
        parent: Optional[Union["Folder", "Project"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Link":
        """Store the link in Synapse.

        Arguments:
            parent: The parent folder or project to store the link in. May also be
                specified in the Link object. If both are provided the parent passed
                into `store` will take precedence.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The link object.

        Raises:
            ValueError: If the link does not have a name and parent_id, or target_id.

        Example: Using this function
            Link with the name `my_link` referencing entity `syn123` and parent folder `syn456`:

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Link

            async def store_link():
                syn = Synapse()
                syn.login()

                link_instance = await Link(
                    name="my_link",
                    parent_id="syn456",
                    target_id="syn123"
                ).store_async()
                return link_instance

            # Run the async function
            link_instance = asyncio.run(store_link())
            ```
        """
        if parent:
            self.parent_id = parent.id

        if not self.name and not self.id:
            raise ValueError("The link must have a name.")
        if not self.parent_id and not self.id:
            raise ValueError("The link must have a parent_id.")
        if not self.target_id and not self.id:
            raise ValueError("The link must have a target_id.")

        if existing_entity := await self._find_existing_entity(
            synapse_client=synapse_client
        ):
            merge_dataclass_entities(
                source=existing_entity,
                destination=self,
            )

        if self.has_changed:
            entity = await store_entity(
                resource=self,
                entity=self.to_synapse_request(),
                synapse_client=synapse_client,
            )

            self.fill_from_dict(synapse_entity=entity, set_annotations=False)

        re_read_required = await store_entity_components(
            root_resource=self, synapse_client=synapse_client
        )
        if re_read_required:
            await self.get_async(
                synapse_client=synapse_client,
            )

        self._set_last_persistent_instance()
        return self

    async def _find_existing_entity(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> Union["Link", None]:
        """Determines if the entity already exists in Synapse. If it does it will return
        the entity object, otherwise it will return None. This is used to determine if the
        entity should be updated or created."""

        async def get_link(existing_id: str) -> "Link":
            """Small wrapper to retrieve a link instance without raising an error if it
            does not exist.

            Arguments:
                existing_id: The ID of the entity to retrieve.

            Returns:
                The entity object if it exists, otherwise None.
            """
            link_copy = Link(
                id=existing_id,
                parent_id=self.parent_id,
            )
            return await link_copy.get_async(
                synapse_client=synapse_client,
                follow_link=False,
            )

        if (
            not self._last_persistent_instance
            and (
                existing_entity_id := await get_id(
                    entity=self,
                    failure_strategy=None,
                    synapse_client=synapse_client,
                )
            )
            and (existing_entity := await get_link(existing_entity_id))
        ):
            return existing_entity
        return None
