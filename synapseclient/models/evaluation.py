from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Protocol

from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse


class EvaluationSynchronousProtocol(Protocol):
    """
    This is the protocol for methods that are asynchronous
    but also have a synchronous counterpart that may also be called.
    """


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

    Example:
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

    def to_synapse_request(self, request_type: str):
        """Creates a request body expected of the Synapse REST API for the Evaluation model."""

        # These attributes are required in our PUT requests for creating or updating an evaluation
        if not self.name:
            raise ValueError("Your evaluation object is missing the 'name' attribute. A name is required to create/update an evaluation")
        if not self.description:
            raise ValueError("Your evaluation object is missing the 'description' attribute. A description is required to create/update an evaluation")
        if not self.content_source:
            raise ValueError("Your evaluation object is missing the 'content_source' attribute. A content_source is required to create/update an evaluation")
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
        if request_type.lower() == 'update':
            if not self.id:
                raise ValueError("Your evaluation object is missing the 'id' attribute. An id is required to update an evaluation")
            if not self.etag:
                raise ValueError("Your evaluation object is missing the 'etag' attribute. An etag is required to update an evaluation")

            request_body["id"] = self.id
            request_body["etag"] = self.etag

        return request_body

    async def store_async(
        self, *, synapse_client: Optional["Synapse"] = None
    ) -> "Evaluation":
        """
        Create a new Evaluation in Synapse. Required fields are `name`, `description`, `content_source`, `submission_instructions_message` and `submission_receipt_message`.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            The created Evaluation object.

        Raises:
            ValueError: If required fields are missing.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        from synapseclient.api.evaluation_services import create_evaluation_async

        request_body = self.to_synapse_request(request_type='create')

        created_evaluation = await create_evaluation_async(
            request_body=request_body,
            synapse_client=synapse_client,
        )

        self.fill_from_dict(created_evaluation)

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

        return self

    async def update_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "Evaluation":
        """
        Update the latest state of this Evaluation in Synapse.

        Arguments:
            name: The human-readable name of the Evaluation.
            description: A short description of the Evaluation's purpose.
            content_source: The ID of the Project or Entity this Evaluation belongs to (e.g., "syn123").
            submission_instructions_message: Instructions presented to submitters when creating a submission.
            submission_receipt_message: A confirmation message shown after a successful submission.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            The updated Evaluation object.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs (including PRECONDITION_FAILED 412).
        """
        from synapseclient.api.evaluation_services import update_evaluation_async

        request_body = self.to_synapse_request('update')

        updated_evaluation = await update_evaluation_async(
            request_body=request_body,
            synapse_client=synapse_client,
        )

        self.fill_from_dict(updated_evaluation)

        return self

    async def delete_async(self, *, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Delete this Evaluation from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        from synapseclient.api.evaluation_services import delete_evaluation_async

        if not self.id:
            raise ValueError("id must be set to delete an evaluation")

        await delete_evaluation_async(
            evaluation_id=self.id,
            synapse_client=synapse_client,
        )

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
        Update the access control list (ACL) for this evaluation.

        Arguments:
            acl: An AccessControlList object or dictionary containing the ACL data to update.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            AccessControlList: The updated ACL.

        Raises:
            ValueError: If the ACL object is invalid or missing required fields.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        from synapseclient.api.evaluation_services import update_evaluation_acl_async

        return await update_evaluation_acl_async(
            acl=acl,
            synapse_client=synapse_client,
        )

    async def get_permissions_async(
        self,
        principal_id: Optional[str] = None,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the user permissions for this evaluation.

        Arguments:
            principal_id: The principal ID to get permissions for. Defaults to the current user.
            synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                            instance from the Synapse class constructor.

        Returns:
            dict: The permissions for the specified user.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        from synapseclient.api.evaluation_services import (
            get_evaluation_permissions_async,
        )

        if not self.id:
            raise ValueError("id must be set to get evaluation permissions")

        return await get_evaluation_permissions_async(
            evaluation_id=self.id,
            principal_id=principal_id,
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
