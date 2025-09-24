from dataclasses import dataclass, field, replace
from typing import List, Optional

from opentelemetry import trace

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.utils import merge_dataclass_entities
from synapseclient.models.protocols.evaluation_protocol import (
    EvaluationSynchronousProtocol,
)


@dataclass
@async_to_sync
class Evaluation(EvaluationSynchronousProtocol):
    """
    An Evaluation is the core object of the Evaluation API, used to support collaborative data analysis challenges in Synapse.

    An `Evaluation` object represents an evaluation queue in Synapse:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/Evaluation.html>

    Attributes:
        id: The unique immutable ID for this Evaluation.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
              The eTag changes every time an Evaluation is updated; it is used to detect when a client's copy
              of an Evaluation is out-of-date.
        name: The name of this Evaluation.
        description: A text description of this Evaluation.
        owner_id: The ID of the Synapse user who created this Evaluation.
        created_on: The date on which Evaluation was created.
        content_source: The Synapse ID of the Entity to which this Evaluation belongs,
                        e.g. a reference to a Synapse project.
        submission_instructions_message: Message to display to users detailing acceptable formatting for Submissions to this Evaluation.
        submission_receipt_message: Message to display to users upon successful submission to this Evaluation.

    Example: Create a new evaluation in a project
    &nbsp;

    ```python
    from synapseclient.models import Evaluation
    from synapseclient import Synapse

    syn = Synapse()
    syn.login()

    evaluation = Evaluation(
        name="My Challenge Evaluation",
        description="Evaluation for my data challenge",
        content_source="syn123456",
        submission_instructions_message="Submit CSV files only",
        submission_receipt_message="Thank you for your submission!",
    )
    created = evaluation.store()
    ```


    Example: Update an existing evaluation retrieved from Synapse by ID
    &nbsp;

    ```python
    from synapseclient.models import Evaluation
    from synapseclient import Synapse

    syn = Synapse()
    syn.login()

    evaluation = Evaluation(id="9999999").get()
    evaluation.description = "Updated description for my evaluation"
    updated = evaluation.store()
    ```
    """

    id: Optional[str] = None
    """The unique immutable ID for this Evaluation."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
    The eTag changes every time an Evaluation is updated; it is used to detect when a client's copy
    of an Evaluation is out-of-date."""

    name: Optional[str] = None
    """The name of this Evaluation."""

    description: Optional[str] = None
    """A text description of this Evaluation."""

    owner_id: Optional[str] = None
    """The ID of the Synapse user who created this Evaluation."""

    created_on: Optional[str] = None
    """The date on which Evaluation was created."""

    content_source: Optional[str] = None
    """The Synapse ID of the Entity to which this Evaluation belongs,
    e.g. a reference to a Synapse project."""

    submission_instructions_message: Optional[str] = None
    """Message to display to users detailing acceptable formatting for Submissions to this Evaluation."""

    submission_receipt_message: Optional[str] = None
    """Message to display to users upon successful submission to this Evaluation."""

    _last_persistent_instance: Optional["Evaluation"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def fill_from_dict(self, evaluation: dict) -> "Evaluation":
        """
        Converts the data coming from the Synapse Evaluation API into this datamodel.

        Arguments:
            evaluation: The data coming from the Synapse Evaluation API

        Returns:
            The Evaluation object instance.
        """
        self.id = evaluation.get("id", None)
        self.etag = evaluation.get("etag", None)
        self.name = evaluation.get("name", None)
        self.description = evaluation.get("description", None)
        self.owner_id = evaluation.get("ownerId", None)
        self.created_on = evaluation.get("createdOn", None)
        self.content_source = evaluation.get("contentSource", None)
        self.submission_instructions_message = evaluation.get(
            "submissionInstructionsMessage", None
        )
        self.submission_receipt_message = evaluation.get(
            "submissionReceiptMessage", None
        )

        return self

    @property
    def has_changed(self) -> bool:
        """Determines if the object has been newly created OR changed since last retrieval, and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        self._last_persistent_instance = replace(self)

    def to_synapse_request(self, request_type: str):
        """Creates a request body expected of the Synapse REST API for the Evaluation model."""

        # These attributes are required in our PUT requests for creating or updating an evaluation
        if not self.name:
            raise ValueError(
                "Your evaluation object is missing the 'name' attribute. A name is required to create/update an evaluation"
            )
        if not self.description:
            raise ValueError(
                "Your evaluation object is missing the 'description' attribute. A description is required to create/update an evaluation"
            )
        if not self.content_source:
            raise ValueError(
                "Your evaluation object is missing the 'content_source' attribute. A content_source is required to create/update an evaluation"
            )
        if not self.submission_instructions_message:
            raise ValueError(
                "Your evaluation object is missing the 'submission_instructions_message' attribute. A submission_instructions_message is required to create/update an evaluation"
            )
        if not self.submission_receipt_message:
            raise ValueError(
                "Your evaluation object is missing the 'submission_receipt_message' attribute. A submission_receipt_message is required to create/update an evaluation"
            )

        # Build a request body for storing a brand new evaluation
        request_body = {
            "name": self.name,
            "description": self.description,
            "contentSource": self.content_source,
            "submissionInstructionsMessage": self.submission_instructions_message,
            "submissionReceiptMessage": self.submission_receipt_message,
        }

        # For 'update' request types, add id and etag
        if request_type.lower() == "update":
            if not self.id:
                raise ValueError(
                    "Your evaluation object is missing the 'id' attribute. An id is required to update an evaluation"
                )
            if not self.etag:
                raise ValueError(
                    "Your evaluation object is missing the 'etag' attribute. An etag is required to update an evaluation"
                )

            request_body["id"] = self.id
            request_body["etag"] = self.etag

        return request_body

    async def store_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Evaluation":
        """
        Create a new Evaluation or update an existing one in Synapse.

        If the Evaluation object has an ID and etag, it will be updated.
        Otherwise, a new Evaluation will be created.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            The created or updated Evaluation object.

        Raises:
            ValueError: If required fields are missing.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Creating a new evaluation
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def create_evaluation():
            evaluation = await Evaluation(
                name="My Challenge Evaluation",
                description="Evaluation for my data challenge",
                content_source="syn123456",
                submission_instructions_message="Submit CSV files only",
                submission_receipt_message="Thank you for your submission!"
            ).store_async()

            return evaluation

        created_evaluation = asyncio.run(create_evaluation())
        ```

        Example: Updating an existing evaluation
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def update_evaluation():
            evaluation = await Evaluation(id="9999999").get_async()
            evaluation.description = "Updated description for my evaluation"
            evaluation.submission_instructions_message = "New submission instructions"
            updated_evaluation = await evaluation.store_async()
            return updated_evaluation

        updated_evaluation = asyncio.run(update_evaluation())
        ```
        """

        from synapseclient.api.evaluation_services import store_evaluation_async

        # Get the client for logging
        client = Synapse.get_client(synapse_client=synapse_client)
        logger = client.logger

        # Set up OpenTelemetry tracing
        trace.get_current_span().set_attributes(
            {
                "synapse.name": self.name or "",
                "synapse.id": self.id or "",
            }
        )

        # CASE 1: No previous interaction with Synapse, so attempt to make a new evaluation
        if not self._last_persistent_instance:
            request_body = self.to_synapse_request(request_type="create")
            result = await store_evaluation_async(
                request_body=request_body,
                synapse_client=synapse_client,
            )

        # CASE 2: Previous interaction with Synapse, so attempt to update an existing evaluation
        elif self._last_persistent_instance:
            if self.has_changed:
                merge_dataclass_entities(
                    source=self._last_persistent_instance,
                    destination=self,
                    fields_to_preserve_from_source=[
                        "id",
                        "etag",
                        "content_source",
                        "owner_id",
                        "created_on",
                    ],
                )
                request_body = self.to_synapse_request(request_type="update")
                result = await store_evaluation_async(
                    request_body=request_body,
                    synapse_client=synapse_client,
                )
            else:
                logger.warning(
                    f"Evaluation {self.name} (ID: {self.id}) has not changed since last 'store' or 'get' event, so it will not be updated in Synapse. Please get the evaluation again if you want to refresh its state."
                )
                return self

        self.fill_from_dict(result)

        # Save the current state to track future changes
        self._set_last_persistent_instance()

        logger.debug(f"Saved Evaluation {self.name}, id: {self.id}")

        return self

    async def get_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Evaluation":
        """
        Get this Evaluation from Synapse by its ID or name.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            The retrieved Evaluation object.

        Raises:
            ValueError: If neither id nor name is set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_evaluations():
            # Get an evaluation by ID
            evaluation_by_id = await Evaluation(id="9999999").get_async()

            # Get an evaluation by name
            evaluation_by_name = await Evaluation(name="My Challenge Evaluation").get_async()

            return evaluation_by_id, evaluation_by_name

        evaluation_by_id, evaluation_by_name = asyncio.run(get_evaluations())
        ```
        """
        from synapseclient.api.evaluation_services import get_evaluation_async

        if not self.id and not self.name:
            raise ValueError("Either id or name must be set to get an evaluation")

        retrieved_evaluation = await get_evaluation_async(
            evaluation_id=self.id,
            name=self.name,
            synapse_client=synapse_client,
        )

        self.fill_from_dict(retrieved_evaluation)

        # Save the current state to track future changes
        self._set_last_persistent_instance()

        return self

    async def delete_async(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Delete this Evaluation from Synapse. ID must be set in order to delete the Evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def delete_evaluations():
            # Delete an evaluation by ID
            await Evaluation(id="9614112").delete_async()

            # Get and then delete an evaluation
            # First get the evaluation by name, so the ID attribute is set in your
            # Evaluation object, then delete it.
            evaluation = await Evaluation(name="My Challenge Evaluation").get_async()
            await evaluation.delete_async()

        asyncio.run(delete_evaluations())
        ```
        """
        from synapseclient.api.evaluation_services import delete_evaluation_async

        if not self.id:
            raise ValueError("id must be set to delete an evaluation")

        await delete_evaluation_async(
            evaluation_id=self.id,
            synapse_client=synapse_client,
        )

        # Clear the persistent instance since this object has been deleted
        self._last_persistent_instance = None

    async def get_acl_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> dict:
        """
        Get the access control list (ACL) governing this evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            AccessControlList: The ACL for this Evaluation.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_evaluation_acl():
            # Get the evaluation first
            evaluation = await Evaluation(id="9999999").get_async()

            # Get the ACL for the evaluation
            acl = await evaluation.get_acl_async()
            return acl

        acl = asyncio.run(get_evaluation_acl())
        ```
        """
        from synapseclient.api.evaluation_services import get_evaluation_acl_async

        if not self.id:
            raise ValueError("id must be set to get evaluation ACL")

        return await get_evaluation_acl_async(
            evaluation_id=self.id,
            synapse_client=synapse_client,
        )

    async def update_acl_async(
        self,
        acl: dict,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Update the access control list (ACL) for this evaluation. The available access types are:

        - 'CREATE'
        - 'SUBMIT',
        - 'READ_PRIVATE_SUBMISSION',
        - 'DELETE_SUBMISSION',
        - 'UPDATE_SUBMISSION',
        - 'CHANGE_PERMISSIONS',
        - 'READ',
        - 'DELETE',
        - 'UPDATE'

        Arguments:
            acl: An AccessControlList object or dictionary containing the ACL data to update.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            AccessControlList: The updated ACL.

        Raises:
            ValueError: If the ACL object is invalid or missing required fields.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def update_evaluation_acl():
            # Get the evaluation first
            evaluation = await Evaluation(id="9999999").get_async()

            # Get the current ACL
            acl = await evaluation.get_acl_async()

            # Modify the ACL - this is just an example, actual structure may vary
            acl["resourceAccess"].append({
                "principalId": "12345",
                "accessType": ["READ", "SUBMIT"]
            })

            # Update the ACL
            updated_acl = await evaluation.update_acl_async(acl)
            return updated_acl

        updated_acl = asyncio.run(update_evaluation_acl())
        ```
        """
        from synapseclient.api.evaluation_services import update_evaluation_acl_async

        return await update_evaluation_acl_async(
            acl=acl,
            synapse_client=synapse_client,
        )

    async def get_permissions_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the user permissions for this evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            dict: The permissions for the specified user.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_evaluation_permissions():
            # Get the evaluation first
            evaluation = await Evaluation(id="9999999").get_async()

            # Get the permissions for the current user
            my_permissions = await evaluation.get_permissions_async()
            return my_permissions

        my_permissions = asyncio.run(get_evaluation_permissions())
        ```
        """
        from synapseclient.api.evaluation_services import (
            get_evaluation_permissions_async,
        )

        if not self.id:
            raise ValueError("id must be set to get evaluation permissions")

        return await get_evaluation_permissions_async(
            evaluation_id=self.id,
            synapse_client=synapse_client,
        )

    @staticmethod
    async def get_all_evaluations_async(
        access_type: Optional[str] = None,
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["Evaluation"]:
        """
        Get a list of all Evaluations, within a given range.

        Arguments:
            access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of all evaluations.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_evaluations():
            # Get all evaluations the user has at least READ access to
            all_evaluations = await Evaluation.get_all_evaluations_async()

            # Get only active evaluations with a limit
            active_evaluations = await Evaluation.get_all_evaluations_async(
                active_only=True,
                limit=20
            )

            # Get specific evaluations by ID
            specific_evaluations = await Evaluation.get_all_evaluations_async(
                evaluation_ids=["9999991", "9999992"]
            )

            return all_evaluations, active_evaluations, specific_evaluations

        all_evaluations, active_evaluations, specific_evaluations = asyncio.run(get_evaluations())
        ```
        """
        from synapseclient.api.evaluation_services import get_all_evaluations_async

        result_dict = await get_all_evaluations_async(
            access_type=access_type,
            active_only=active_only,
            evaluation_ids=evaluation_ids,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )
        items = (
            result_dict.get("results") if isinstance(result_dict, dict) else result_dict
        )
        evaluations: List[Evaluation] = []
        for item in items:
            evaluations.append(Evaluation().fill_from_dict(item))

        return evaluations

    @staticmethod
    async def get_available_evaluations_async(
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["Evaluation"]:
        """
        Get a list of Evaluations to which the user has SUBMIT permission, within a given range.

        Arguments:
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of available evaluations.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_available_evaluations():
            # Get all evaluations where the current user has SUBMIT permission
            available_evaluations = await Evaluation.get_available_evaluations_async()

            # Get only active evaluations where the current user has SUBMIT permission
            active_available_evaluations = await Evaluation.get_available_evaluations_async(
                active_only=True
            )

            # Get the first 5 evaluations where the current user has SUBMIT permission
            limited_evaluations = await Evaluation.get_available_evaluations_async(
                limit=5
            )

            return available_evaluations, active_available_evaluations, limited_evaluations

        available_evaluations, active_available_evaluations, limited_evaluations = asyncio.run(get_available_evaluations())
        ```
        """
        from synapseclient.api.evaluation_services import (
            get_available_evaluations_async,
        )

        result_dict = await get_available_evaluations_async(
            active_only=active_only,
            evaluation_ids=evaluation_ids,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )
        items = (
            result_dict.get("results") if isinstance(result_dict, dict) else result_dict
        )
        evaluations: List[Evaluation] = []
        for item in items:
            evaluations.append(Evaluation().fill_from_dict(item))

        return evaluations

    @staticmethod
    async def get_evaluations_by_project_async(
        project_id: str,
        access_type: Optional[str] = None,
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["Evaluation"]:
        """
        Get Evaluations tied to a project.

        Arguments:
            project_id: The ID of the project (e.g., "syn123456").
            access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of Evaluations tied to the project.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Using this function
        &nbsp;

        ```python
        from synapseclient.models import Evaluation
        from synapseclient import Synapse
        import asyncio

        syn = Synapse()
        syn.login()

        async def get_project_evaluations():
            # Get all evaluations for a project
            project_evaluations = await Evaluation.get_evaluations_by_project_async(
                project_id="syn123456"
            )

            # Get only active evaluations for a project
            active_project_evaluations = await Evaluation.get_evaluations_by_project_async(
                project_id="syn123456",
                active_only=True
            )

            # Get a limited set of evaluations for a project
            limited_project_evaluations = await Evaluation.get_evaluations_by_project_async(
                project_id="syn123456",
                limit=5
            )

            return project_evaluations, active_project_evaluations, limited_project_evaluations

        project_evaluations, active_project_evaluations, limited_project_evaluations = asyncio.run(get_project_evaluations())
        ```
        """
        from synapseclient.api.evaluation_services import (
            get_evaluations_by_project_async,
        )

        result_dict = await get_evaluations_by_project_async(
            project_id=project_id,
            access_type=access_type,
            active_only=active_only,
            evaluation_ids=evaluation_ids,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )
        items = (
            result_dict.get("results") if isinstance(result_dict, dict) else result_dict
        )
        evaluations: List[Evaluation] = []
        for item in items:
            evaluations.append(Evaluation().fill_from_dict(item))

        return evaluations
