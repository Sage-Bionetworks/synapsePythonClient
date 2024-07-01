import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Union

from opentelemetry import context, trace

from synapseclient import Synapse
from synapseclient.activity import Activity as Synapse_Activity
from synapseclient.api import delete_entity_generated_by
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.constants.concrete_types import USED_ENTITY, USED_URL
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.utils import run_and_attach_otel_context
from synapseclient.models.protocols.activity_protocol import ActivitySynchronousProtocol

if TYPE_CHECKING:
    from synapseclient.models import File, Table

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


class UsedAndExecutedSynapseActivities(NamedTuple):
    """
    Named tuple to hold the used and executed activities.

    Attributes:
        used: The entities or URLs used by this Activity.
        executed: The entities or URLs executed by this Activity.
    """

    used: List[Dict]
    executed: List[Dict]


# TODO: When Views and Datasets are added we should add Activity to them.
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
        self, synapse_activity: Union[Synapse_Activity, Dict]
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
                    {
                        "reference": {
                            "targetId": used.target_id,
                            "targetVersionNumber": used.target_version_number,
                        }
                    }
                )
            elif isinstance(used, UsedURL):
                synapse_activity_used.append(
                    {
                        "name": used.name,
                        "url": used.url,
                    }
                )

        for executed in self.executed:
            if isinstance(executed, UsedEntity):
                synapse_activity_executed.append(
                    {
                        "reference": {
                            "targetId": executed.target_id,
                            "targetVersionNumber": executed.target_version_number,
                        },
                        "wasExecuted": True,
                    }
                )
            elif isinstance(executed, UsedURL):
                synapse_activity_executed.append(
                    {"name": executed.name, "url": executed.url, "wasExecuted": True}
                )
        return UsedAndExecutedSynapseActivities(
            used=synapse_activity_used, executed=synapse_activity_executed
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Activity_store: {self.name}"
    )
    async def store_async(
        self,
        parent: Optional[Union["Table", "File"]] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Activity":
        """
        Store the Activity in Synapse.

        Arguments:
            parent: The parent entity to associate this activity with.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The activity object.

        Raises:
            ValueError: Raised if both of the following are true:

                - If the parent does not have an ID.
                - If the Activity does not have an ID and ETag.
        """
        # TODO: Input validation: SYNPY-1400
        used_and_executed_activities = (
            self._create_used_and_executed_synapse_activities()
        )

        synapse_activity = Synapse_Activity(
            name=self.name,
            description=self.description,
            used=used_and_executed_activities.used,
            executed=used_and_executed_activities.executed,
        )

        loop = asyncio.get_event_loop()
        current_context = context.get_current()
        if self.id:
            # Despite init in `Synapse_Activity` not accepting an ID/ETAG the
            # `updateActivity` method expects that it exists on the dict
            # and `setProvenance` accepts it as well.
            synapse_activity["id"] = self.id
            synapse_activity["etag"] = self.etag
        if parent:
            saved_activity = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).setProvenance(
                        entity=parent.id,
                        activity=synapse_activity,
                    ),
                    current_context,
                ),
            )
        else:
            saved_activity = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).updateActivity(
                        activity=synapse_activity,
                    ),
                    current_context,
                ),
            )
        self.fill_from_dict(synapse_activity=saved_activity)
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Stored activity {self.id}"
        )

        return self

    @classmethod
    async def from_parent_async(
        cls,
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Union["Activity", None]:
        """
        Get the Activity from Synapse based on the parent entity.

        Arguments:
            parent: The parent entity this activity is associated with. The parent may
                also have a version_number. Gets the most recent version if version is
                omitted.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The activity object or None if it does not exist.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(name=f"Activity_get: Parent_ID: {parent.id}"):
            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            try:
                synapse_activity = await loop.run_in_executor(
                    None,
                    lambda: run_and_attach_otel_context(
                        lambda: Synapse.get_client(
                            synapse_client=synapse_client
                        ).getProvenance(
                            entity=parent.id,
                            version=parent.version_number,
                        ),
                        current_context,
                    ),
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
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
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
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(
            name=f"Activity_delete: Parent_ID: {parent.id}"
        ):
            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).deleteProvenance(
                        entity=parent.id,
                    ),
                    current_context,
                ),
            )
            parent.activity = None

    @classmethod
    async def disassociate_from_entity_async(
        cls,
        parent: Union["Table", "File"],
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Disassociate the Activity from the parent entity. This is the first step in
        deleting the Activity. If you have other entities that are associated with this
        Activity you must disassociate them by calling this method on them as well.

        Arguments:
            parent: The parent entity this activity is associated with.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Raises:
            ValueError: If the parent does not have an ID.
        """
        # TODO: Input validation: SYNPY-1400
        with tracer.start_as_current_span(
            name=f"Activity_disassociate: Parent_ID: {parent.id}"
        ):
            await delete_entity_generated_by(
                entity_id=parent.id, synapse_client=synapse_client
            )
            parent.activity = None
