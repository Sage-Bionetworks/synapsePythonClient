from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Union

from opentelemetry import trace

from synapseclient.api import (
    create_activity,
    delete_entity_generated_by,
    delete_entity_provenance,
    get_activity,
    get_entity_provenance,
    set_entity_provenance,
    update_activity,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import USED_ENTITY, USED_URL
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.utils import delete_none_keys, get_synid_and_version
from synapseclient.models.protocols.activity_protocol import ActivitySynchronousProtocol

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import Dataset, EntityView, File, Table

tracer = trace.get_tracer("synapseclient")


@dataclass
class UsedEntity:
    """
    Reference to a Synapse entity that was used or executed by an Activity.

    Attributes:
        target_id: The ID of the entity to which this reference refers.
        target_version_number: The version number of the entity to which this reference refers.
    """

    target_id: Optional[str] = None
    """The the id of the entity to which this reference refers."""

    target_version_number: Optional[int] = None
    """The version number of the entity to which this reference refers."""

    def format_for_manifest(self) -> str:
        """
        Format the content of this data class to be written to a manifest file.

        Returns:
            The formatted string.
        """
        return_value = f"{self.target_id}"
        if self.target_version_number is not None:
            return_value += f".{self.target_version_number}"
        return return_value

    def to_synapse_request(
        self, was_executed: bool = False
    ) -> Dict[str, Union[str, bool, Dict]]:
        """
        Converts the UsedEntity to a request expected by the Synapse REST API.

        Arguments:
            was_executed: Whether the entity was executed (vs. just used).

        Returns:
            A dictionary representation matching the Synapse REST API format.
        """
        return {
            "concreteType": USED_ENTITY,
            "reference": {
                "targetId": self.target_id,
                "targetVersionNumber": self.target_version_number,
            },
            "wasExecuted": was_executed,
        }


@dataclass
class UsedURL:
    """
    URL that was used or executed by an Activity such as a link to a
    GitHub commit or a link to a specific version of a software tool.

    Attributes:
        name: The name of the URL.
        url: The external URL of the file that was used such as a link to a
            GitHub commit or a link to a specific version of a software tool.
    """

    name: Optional[str] = None
    """The name of the URL."""

    url: Optional[str] = None
    """The external URL of the file that was used such as a link to a GitHub commit
    or a link to a specific version of a software tool."""

    def format_for_manifest(self) -> str:
        """
        Format the content of this data class to be written to a manifest file.

        Returns:
            The formatted string.
        """
        if self.name:
            return_value = self.name
        else:
            return_value = self.url

        return return_value

    def to_synapse_request(
        self, was_executed: bool = False
    ) -> Dict[str, Union[str, bool]]:
        """
        Converts the UsedURL to a request expected by the Synapse REST API.

        Arguments:
            was_executed: Whether the URL was executed (vs. just used).

        Returns:
            A dictionary representation matching the Synapse REST API format.
        """
        return {
            "concreteType": USED_URL,
            "name": self.name,
            "url": self.url,
            "wasExecuted": was_executed,
        }


class UsedAndExecutedSynapseActivities(NamedTuple):
    """
    Named tuple to hold the used and executed activities.

    Attributes:
        used: The entities or URLs used by this Activity.
        executed: The entities or URLs executed by this Activity.
    """

    used: List[Dict]
    executed: List[Dict]


@dataclass
@async_to_sync
class Activity(ActivitySynchronousProtocol):
    """
    An activity is a Synapse object that helps to keep track of what objects were used
    in an analysis step, as well as what objects were generated. Thus, all relationships
    between Synapse objects and an activity are governed by dependencies. That is, an
    activity needs to know what it `used`, and outputs need to know what activity
    they were `generatedBy`.

    Attributes:
        id: The unique immutable ID for this actvity.
        name: A name for this Activity.
        description: A description for this Activity.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's current representation of an entity is
            out-of-date.
        created_on: The date this object was created.
        modified_on: The date this object was last modified.
        created_by: The user that created this object.
        modified_by: The user that last modified this object.
        used: The entities or URLs used by this Activity.
        executed: The entities or URLs executed by this Activity.
    """

    id: Optional[str] = None
    """The unique immutable ID for this actvity."""

    name: Optional[str] = None
    """A name for this Activity."""

    description: Optional[str] = None
    """A description for this Activity."""

    etag: Optional[str] = None
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    created_on: Optional[str] = None
    """The date this object was created."""

    modified_on: Optional[str] = None
    """The date this object was last modified."""

    created_by: Optional[str] = None
    """The user that created this object."""

    modified_by: Optional[str] = None
    """The user that last modified this object."""

    used: List[Union[UsedEntity, UsedURL]] = field(default_factory=list)
    """The entities used by this Activity."""

    executed: List[Union[UsedEntity, UsedURL]] = field(default_factory=list)
    """The entities executed by this Activity."""

    def fill_from_dict(
        self, synapse_activity: Dict[str, Union[str, List[Dict[str, Union[str, bool]]]]]
    ) -> "Activity":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_activity: The response from the REST API.

        Returns:
            The Activity object.
        """
        if not synapse_activity:
            synapse_activity = {}
        self.id = synapse_activity.get("id", None)
        self.name = synapse_activity.get("name", None)
        self.description = synapse_activity.get("description", None)
        self.etag = synapse_activity.get("etag", None)
        self.created_on = synapse_activity.get("createdOn", None)
        self.modified_on = synapse_activity.get("modifiedOn", None)
        self.created_by = synapse_activity.get("createdBy", None)
        self.modified_by = synapse_activity.get("modifiedBy", None)
        self.executed = []
        self.used = []
        for used in synapse_activity.get("used", []):
            concrete_type = used.get("concreteType", None)
            if USED_URL == concrete_type:
                used_url = UsedURL(
                    name=used.get("name", None),
                    url=used.get("url", None),
                )

                if used.get("wasExecuted", False):
                    self.executed.append(used_url)
                else:
                    self.used.append(used_url)
            elif USED_ENTITY == concrete_type:
                reference = used.get("reference", {})
                used_entity = UsedEntity(
                    target_id=reference.get("targetId", None),
                    target_version_number=reference.get("targetVersionNumber", None),
                )

                if used.get("wasExecuted", False):
                    self.executed.append(used_entity)
                else:
                    self.used.append(used_entity)

        return self

    def _create_used_and_executed_synapse_activities(
        self,
    ) -> UsedAndExecutedSynapseActivities:
        """
        Helper function to create the used and executed activities for the
        Synapse Activity.

        Returns:
            A tuple of the used and executed activities.
        """
        synapse_activity_used = []
        synapse_activity_executed = []

        for used in self.used:
            if isinstance(used, UsedEntity):
                synapse_activity_used.append(
                    used.to_synapse_request(was_executed=False)
                )
            elif isinstance(used, UsedURL):
                synapse_activity_used.append(
                    used.to_synapse_request(was_executed=False)
                )

        for executed in self.executed:
            if isinstance(executed, UsedEntity):
                synapse_activity_executed.append(
                    executed.to_synapse_request(was_executed=True)
                )
            elif isinstance(executed, UsedURL):
                synapse_activity_executed.append(
                    executed.to_synapse_request(was_executed=True)
                )

        return UsedAndExecutedSynapseActivities(
            used=synapse_activity_used, executed=synapse_activity_executed
        )

    def to_synapse_request(self) -> Dict[str, Union[str, List[Dict]]]:
        """
        Converts the Activity to a request expected by the Synapse REST API.

        Returns:
            A dictionary representation matching the Synapse REST API format.
        """
        used_and_executed_activities = (
            self._create_used_and_executed_synapse_activities()
        )

        combined_used = (
            used_and_executed_activities.used + used_and_executed_activities.executed
        )

        request = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "etag": self.etag,
            "createdOn": self.created_on,
            "modifiedOn": self.modified_on,
            "createdBy": self.created_by,
            "modifiedBy": self.modified_by,
            "used": combined_used,
        }

        delete_none_keys(request)

        return request

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Activity_store: {self.name}"
    )
    async def store_async(
        self,
        parent: Optional[Union["Table", "File", "EntityView", "Dataset", str]] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "Activity":
        """
        Store the Activity in Synapse.

        Arguments:
            parent: The parent entity to associate this activity with. Can be an entity
                object or a string ID (e.g., "syn123").
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The activity object.

        Raises:
            ValueError: Raised if both of the following are true:

                - If the parent does not have an ID.
                - If the Activity does not have an ID and ETag.
        """
        from synapseclient import Synapse

        # TODO: Input validation: SYNPY-1400
        if parent:
            parent_id = parent if isinstance(parent, str) else parent.id
            saved_activity = await set_entity_provenance(
                entity_id=parent_id,
                activity=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
        else:
            if self.id:
                saved_activity = await update_activity(
                    self.to_synapse_request(), synapse_client=synapse_client
                )
            else:
                saved_activity = await create_activity(
                    self.to_synapse_request(), synapse_client=synapse_client
                )
        self.fill_from_dict(synapse_activity=saved_activity)

        if parent:
            parent_display_id = parent if isinstance(parent, str) else parent.id
            Synapse.get_client(synapse_client=synapse_client).logger.info(
                f"[{parent_display_id}]: Stored activity"
            )
        else:
            Synapse.get_client(synapse_client=synapse_client).logger.info(
                f"[{self.id}]: Stored activity"
            )

        return self

    @classmethod
    async def from_parent_async(
        cls,
        parent: Union["Table", "File", "EntityView", "Dataset", str],
        parent_version_number: Optional[int] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union["Activity", None]:
        """
        Get the Activity from Synapse based on the parent entity.

        Arguments:
            parent: The parent entity this activity is associated with. The parent may
                also have a version_number. Gets the most recent version if version is
                omitted.
            parent_version_number: The version number of the parent entity. When parent
                is a string with version (e.g., "syn123.4"), the version in the string
                takes precedence. When parent is an object, this parameter takes precedence
                over parent.version_number. Gets the most recent version if omitted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The activity object or None if it does not exist.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        if isinstance(parent, str):
            parent_id, version = get_synid_and_version(parent)
            if version is None:
                version = parent_version_number
        else:
            parent_id = parent.id
            version = (
                parent_version_number
                if parent_version_number is not None
                else parent.version_number
            )

        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(name=f"Activity_get: Parent_ID: {parent_id}"):
            try:
                synapse_activity = await get_entity_provenance(
                    entity_id=parent_id,
                    version_number=version,
                    synapse_client=synapse_client,
                )
            except SynapseHTTPError as ex:
                if ex.response.status_code == 404:
                    return None
                else:
                    raise ex
            if synapse_activity:
                return cls().fill_from_dict(synapse_activity=synapse_activity)
            else:
                return None

    @classmethod
    async def delete_async(
        cls,
        parent: Union["Table", "File", str],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Delete the Activity from Synapse. The Activity must be disassociated from
        all entities before it can be deleted. The first step of this delete call
        is to disassociate the Activity from the parent entity. If you have other
        entities that are associated with this Activity you must disassociate them
        by calling this method on them as well. You'll receive an error for all entities
        until the last one which will delete the Activity.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        parent_id = parent if isinstance(parent, str) else parent.id
        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(
            name=f"Activity_delete: Parent_ID: {parent_id}"
        ):
            await delete_entity_provenance(
                entity_id=parent_id, synapse_client=synapse_client
            )
            if not isinstance(parent, str):
                parent.activity = None

    @classmethod
    async def disassociate_from_entity_async(
        cls,
        parent: Union["Table", "File", str],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Disassociate the Activity from the parent entity. This is the first step in
        deleting the Activity. If you have other entities that are associated with this
        Activity you must disassociate them by calling this method on them as well.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        parent_id = parent if isinstance(parent, str) else parent.id
        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(
            name=f"Activity_disassociate: Parent_ID: {parent_id}"
        ):
            await delete_entity_generated_by(
                entity_id=parent_id, synapse_client=synapse_client
            )
            if not isinstance(parent, str):
                parent.activity = None

    @classmethod
    async def get_async(
        cls,
        activity_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        parent_version_number: Optional[int] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union["Activity", None]:
        """
        Get an Activity from Synapse by either activity ID or parent entity ID.

        Arguments:
            activity_id: The ID of the activity to retrieve. If provided, this takes
                precedence over parent_id.
            parent_id: The ID of the parent entity to get the activity for.
                Only used if activity_id is not provided (ignored when activity_id is provided).
            parent_version_number: The version number of the parent entity. Only used when
                parent_id is provided (ignored when activity_id is provided). Gets the
                most recent version if omitted.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The activity object or None if it does not exist.

        Raises:
            ValueError: If neither activity_id nor parent_id is provided.

        Example: Get activity by activity ID
            Retrieve an activity using its ID.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Activity

            syn = Synapse()
            syn.login()

            async def main():
                activity = await Activity.get_async(activity_id="12345")
                if activity:
                    print(f"Activity: {activity.name}")
                else:
                    print("Activity not found")

            asyncio.run(main())
            ```

        Example: Get activity by parent entity ID
            Retrieve an activity using the parent entity ID.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Activity

            syn = Synapse()
            syn.login()

            async def main():
                activity = await Activity.get_async(parent_id="syn123")
                if activity:
                    print(f"Activity: {activity.name}")
                else:
                    print("No activity found for entity")

            asyncio.run(main())
            ```

        Example: Get activity by parent entity ID with version
            Retrieve an activity for a specific version of a parent entity.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Activity

            syn = Synapse()
            syn.login()

            async def main():
                activity = await Activity.get_async(
                    parent_id="syn123",
                    parent_version_number=3
                )
                if activity:
                    print(f"Activity: {activity.name}")
                else:
                    print("No activity found for entity version")

            asyncio.run(main())
            ```
        """
        if not activity_id and not parent_id:
            raise ValueError("Either activity_id or parent_id must be provided")

        if activity_id:
            try:
                synapse_activity = await get_activity(
                    activity_id=activity_id,
                    synapse_client=synapse_client,
                )
                if synapse_activity:
                    return cls().fill_from_dict(synapse_activity=synapse_activity)
                else:
                    return None
            except SynapseHTTPError as ex:
                if ex.response.status_code == 404:
                    return None
                else:
                    raise ex
        else:
            try:
                synapse_activity = await get_entity_provenance(
                    entity_id=parent_id,
                    version_number=parent_version_number,
                    synapse_client=synapse_client,
                )
                if synapse_activity:
                    return cls().fill_from_dict(synapse_activity=synapse_activity)
                else:
                    return None
            except SynapseHTTPError as ex:
                if ex.response.status_code == 404:
                    return None
                else:
                    raise ex
